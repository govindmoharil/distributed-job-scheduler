import { pool, redis } from '../src/db';

describe('Distributed Concurrency & Claim Isolation', () => {
  let queueId: string;
  let projectId: string;

  beforeAll(async () => {
    const org = await pool.query(
      `INSERT INTO organizations (name) VALUES ('Test Suite Org') RETURNING id`
    );
    const project = await pool.query(
      `INSERT INTO projects (org_id, name, api_key) VALUES ($1, 'Test Suite Project', 'test-key-suite') RETURNING id`,
      [org.rows[0].id]
    );
    projectId = project.rows[0].id;

    // Set is_paused = true so live worker containers do not claim test jobs
    const queue = await pool.query(
      `INSERT INTO queues (project_id, name, priority, is_paused) 
       VALUES ($1, 'race-test-queue', 10, true) 
       RETURNING id`,
      [projectId]
    );
    queueId = queue.rows[0].id;
  });

  afterAll(async () => {
    if (projectId) {
      await pool.query(`DELETE FROM projects WHERE id = $1`, [projectId]);
    }
    await pool.end();
    await redis.quit();
  });

  it('guarantees strictly zero duplicate claims across 50 concurrent workers', async () => {
    // Seed 20 unique queued jobs
    for (let i = 0; i < 20; i++) {
      await pool.query(
        `INSERT INTO jobs (queue_id, name, status, priority) VALUES ($1, $2, 'QUEUED', 1)`,
        [queueId, `concurrent-job-${i}`]
      );
    }

    const atomicClaim = async () => {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const res = await client.query(
          `SELECT id FROM jobs 
           WHERE queue_id = $1 AND status = 'QUEUED'
           ORDER BY priority DESC, scheduled_at ASC
           FOR UPDATE SKIP LOCKED
           LIMIT 1`,
          [queueId]
        );

        if (res.rowCount && res.rowCount > 0) {
          const id = res.rows[0].id;
          await client.query(`UPDATE jobs SET status = 'CLAIMED' WHERE id = $1`, [id]);
          await client.query('COMMIT');
          return id;
        }
        await client.query('ROLLBACK');
        return null;
      } catch (err) {
        await client.query('ROLLBACK');
        return null;
      } finally {
        client.release();
      }
    };

    // Dispatch 50 simultaneous parallel claim attempts
    const claimRequests = Array.from({ length: 50 }, () => atomicClaim());
    const results = await Promise.all(claimRequests);
    const claimedIds = results.filter((id): id is string => Boolean(id));

    // Must claim all 20 jobs with zero duplicates
    const uniqueClaimed = new Set(claimedIds);
    expect(claimedIds.length).toBe(20);
    expect(uniqueClaimed.size).toBe(20);
  });
});