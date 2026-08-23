import os from 'os';
import { pool } from './db';
import { unlockDependentJobs } from './services/dagService';

export class DistributedWorker {
  private workerId!: string;
  private isRunning = false;
  private activeJobs = 0;
  private concurrency = parseInt(process.env.WORKER_CONCURRENCY || '5', 10);
  private heartbeatTimer?: NodeJS.Timeout;

  async start() {
    const res = await pool.query(
      `INSERT INTO workers (hostname, status, active_jobs_count, last_seen_at)
       VALUES ($1, 'ACTIVE', 0, NOW()) RETURNING id`,
      [`${os.hostname()}-${process.pid}`]
    );
    this.workerId = res.rows[0].id;
    this.isRunning = true;

    this.startHeartbeat();
    this.registerSignals();
    this.pollLoop();
    console.log(`[Worker] Started node ${this.workerId} with concurrency ${this.concurrency}`);
  }

  private startHeartbeat() {
    this.heartbeatTimer = setInterval(async () => {
      try {
        await pool.query(
          `UPDATE workers 
           SET last_seen_at = NOW(), active_jobs_count = $1, status = 'ACTIVE' 
           WHERE id = $2`,
          [this.activeJobs, this.workerId]
        );
      } catch (err) {
        console.error('[Worker Heartbeat Error]:', err);
      }
    }, 10000);
  }

  private async pollLoop() {
    while (this.isRunning) {
      if (this.activeJobs < this.concurrency) {
        const claimed = await this.claimJob();
        if (!claimed) {
          await new Promise((r) => setTimeout(r, 1000));
        }
      } else {
        await new Promise((r) => setTimeout(r, 200));
      }
    }
  }

  private async claimJob(): Promise<boolean> {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const claimSql = `
        SELECT j.id, j.name, j.payload, j.attempt, j.max_retries, j.queue_id,
               rp.strategy, rp.base_delay_seconds, rp.max_delay_seconds
        FROM jobs j
        JOIN queues q ON j.queue_id = q.id
        LEFT JOIN retry_policies rp ON q.retry_policy_id = rp.id
        WHERE j.status = 'QUEUED' 
          AND j.scheduled_at <= NOW()
          AND q.is_paused = FALSE
        ORDER BY j.priority DESC, j.scheduled_at ASC
        FOR UPDATE OF j SKIP LOCKED
        LIMIT 1
      `;
      const res = await client.query(claimSql);

      if (res.rowCount === 0) {
        await client.query('ROLLBACK');
        return false;
      }

      const job = res.rows[0];

      await client.query(
        `UPDATE jobs 
         SET status = 'RUNNING', claimed_by_worker_id = $1, claimed_at = NOW(), 
             started_at = NOW(), attempt = attempt + 1 
         WHERE id = $2`,
        [this.workerId, job.id]
      );

      const execRes = await client.query(
        `INSERT INTO job_executions (job_id, worker_id, attempt, status, started_at)
         VALUES ($1, $2, $3, 'RUNNING', NOW()) RETURNING id`,
        [job.id, this.workerId, job.attempt + 1]
      );

      await client.query('COMMIT');

      this.activeJobs++;
      this.execute(job, execRes.rows[0].id).finally(() => this.activeJobs--);
      return true;
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('[Claim Error]:', err);
      return false;
    } finally {
      client.release();
    }
  }

  private async execute(job: any, execId: string) {
    const start = Date.now();
    try {
      if (job.payload?.forceFail) {
        throw new Error('Induced failure via payload.forceFail');
      }

      await new Promise((r) => setTimeout(r, 500));
      const duration = Date.now() - start;

      await pool.query(
        `UPDATE jobs SET status = 'COMPLETED', completed_at = NOW() WHERE id = $1`,
        [job.id]
      );
      await pool.query(
        `UPDATE job_executions 
         SET status = 'COMPLETED', execution_time_ms = $1, finished_at = NOW() 
         WHERE id = $2`,
        [duration, execId]
      );

      await unlockDependentJobs(job.id);
    } catch (err: any) {
      const duration = Date.now() - start;
      await this.handleFailure(job, execId, err, duration);
    }
  }

  private async handleFailure(job: any, execId: string, err: Error, duration: number) {
    const attempt = job.attempt + 1;
    const isExhausted = attempt >= job.max_retries;

    await pool.query(
      `UPDATE job_executions 
       SET status = 'FAILED', error_message = $1, stack_trace = $2, 
           execution_time_ms = $3, finished_at = NOW() 
       WHERE id = $4`,
      [err.message, err.stack, duration, execId]
    );

    if (isExhausted) {
      await pool.query(`UPDATE jobs SET status = 'DEAD_LETTER' WHERE id = $1`, [job.id]);
      await pool.query(
        `INSERT INTO dead_letter_queue (
          job_id, queue_id, exhausted_attempts, last_error_message, last_stack_trace, payload
        ) VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (job_id) DO NOTHING`,
        [job.id, job.queue_id, attempt, err.message, err.stack, job.payload]
      );
    } else {
      const strategy = job.strategy || 'EXPONENTIAL';
      const base = job.base_delay_seconds || 5;
      const max = job.max_delay_seconds || 3600;
      let delay = base;

      if (strategy === 'EXPONENTIAL') {
        delay = Math.min(max, base * Math.pow(2, attempt - 1));
      } else if (strategy === 'LINEAR') {
        delay = Math.min(max, base * attempt);
      } else { // FIXED
        delay = base;
      }

      const jitter = Math.floor(Math.random() * (delay * 0.2));
      const nextRun = new Date(Date.now() + (delay + jitter) * 1000);

      await pool.query(
        `UPDATE jobs 
         SET status = 'QUEUED', scheduled_at = $1, claimed_by_worker_id = NULL 
         WHERE id = $2`,
        [nextRun, job.id]
      );
    }
  }

  private registerSignals() {
    const shutdown = async () => {
      console.log(`[Worker] SIGTERM/SIGINT received. Draining ${this.activeJobs} jobs...`);
      this.isRunning = false;
      if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);

      const timeout = Date.now() + 25000;
      while (this.activeJobs > 0 && Date.now() < timeout) {
        await new Promise((r) => setTimeout(r, 500));
      }

      await pool.query(
        `UPDATE workers SET status = 'STOPPED', active_jobs_count = 0 WHERE id = $1`,
        [this.workerId]
      );
      process.exit(0);
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  }
}

if (require.main === module) {
  new DistributedWorker().start();
}