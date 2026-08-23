import { Router, Response } from 'express';
import { pool } from '../db';
import { AuthenticatedRequest } from '../middleware/auth';

const router = Router();

router.get('/', async (_req: AuthenticatedRequest, res: Response) => {
  const result = await pool.query(`
    SELECT id, hostname, status, active_jobs_count, last_seen_at, registered_at,
           CASE WHEN last_seen_at > NOW() - INTERVAL '25 seconds' THEN true ELSE false END as is_healthy
    FROM workers
    ORDER BY registered_at DESC
  `);
  res.json({ workers: result.rows });
});

export default router;