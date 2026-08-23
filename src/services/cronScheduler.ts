import parser from 'cron-parser';
import { pool, redis } from '../db';

export async function processCronSchedules() {
  const lockKey = 'lock:cron:scheduler';
  const acquired = await redis.set(lockKey, '1', 'PX', 9000, 'NX');
  if (!acquired) return;

  try {
    const res = await pool.query(
      `SELECT id, cron_expression, queue_id, name, payload, priority 
       FROM jobs 
       WHERE cron_expression IS NOT NULL AND status IN ('SCHEDULED', 'COMPLETED')`
    );

    for (const job of res.rows) {
      try {
        const interval = parser.parseExpression(job.cron_expression);
        const nextDate = interval.next().toDate();

        await pool.query(
          `INSERT INTO jobs (queue_id, name, payload, priority, status, scheduled_at, cron_expression)
           VALUES ($1, $2, $3, $4, 'SCHEDULED', $5, NULL)
           ON CONFLICT DO NOTHING`,
          [job.queue_id, `${job.name}-recurring`, job.payload, job.priority, nextDate]
        );
      } catch (err) {
        console.error(`Invalid cron expression on job ${job.id}:`, err);
      }
    }
  } finally {
    await redis.del(lockKey);
  }
}