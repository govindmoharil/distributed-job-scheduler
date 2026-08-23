import { Router, Response } from 'express';
import { pool } from '../db';
import { AuthenticatedRequest } from '../middleware/auth';

const router = Router();

router.get('/', async (req: AuthenticatedRequest, res: Response) => {
  const result = await pool.query(`
    SELECT dlq.*, q.name as queue_name, j.name as job_name
    FROM dead_letter_queue dlq
    JOIN queues q ON dlq.queue_id = q.id
    JOIN jobs j ON dlq.job_id = j.id
    WHERE q.project_id = $1
    ORDER BY dlq.created_at DESC
  `, [req.project!.id]);

  res.json({ dlq: result.rows });
});

router.post('/:jobId/replay', async (req: AuthenticatedRequest, res: Response) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const dlqRes = await client.query(
      `DELETE FROM dead_letter_queue WHERE job_id = $1 RETURNING *`,
      [req.params.jobId]
    );

    if (dlqRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'DLQ Entry not found' });
    }

    await client.query(
      `UPDATE jobs 
       SET status = 'QUEUED', attempt = 0, scheduled_at = NOW(), claimed_by_worker_id = NULL
       WHERE id = $1`,
      [req.params.jobId]
    );

    await client.query('COMMIT');
    res.json({ success: true, message: 'Job re-queued successfully' });
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
});

router.post('/replay-all', async (req: AuthenticatedRequest, res: Response) => {
  const result = await pool.query(`
    WITH deleted AS (
      DELETE FROM dead_letter_queue dlq
      USING queues q
      WHERE dlq.queue_id = q.id AND q.project_id = $1
      RETURNING dlq.job_id
    )
    UPDATE jobs j
    SET status = 'QUEUED', attempt = 0, scheduled_at = NOW(), claimed_by_worker_id = NULL
    FROM deleted
    WHERE j.id = deleted.job_id
    RETURNING j.id
  `, [req.project!.id]);

  res.json({ replayed_count: result.rowCount });
});

export default router;