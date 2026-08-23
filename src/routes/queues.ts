import { Router, Response } from 'express';
import { z } from 'zod';
import { pool } from '../db';
import { AuthenticatedRequest } from '../middleware/auth';

const router = Router();

router.get('/', async (req: AuthenticatedRequest, res: Response) => {
  const result = await pool.query(`
    SELECT q.*,
      COALESCE(SUM(CASE WHEN j.status = 'QUEUED' THEN 1 ELSE 0 END), 0)::int AS queued_count,
      COALESCE(SUM(CASE WHEN j.status = 'RUNNING' THEN 1 ELSE 0 END), 0)::int AS active_count,
      COALESCE(SUM(CASE WHEN j.status = 'COMPLETED' THEN 1 ELSE 0 END), 0)::int AS completed_count,
      COALESCE(SUM(CASE WHEN j.status = 'DEAD_LETTER' THEN 1 ELSE 0 END), 0)::int AS dead_letter_count
    FROM queues q
    LEFT JOIN jobs j ON q.id = j.queue_id
    WHERE q.project_id = $1
    GROUP BY q.id
    ORDER BY q.priority DESC
  `, [req.project!.id]);

  res.json({ queues: result.rows });
});

router.post('/', async (req: AuthenticatedRequest, res: Response) => {
  const schema = z.object({
    name: z.string().min(1),
    priority: z.number().int().min(1).max(100).default(1),
    concurrency_limit: z.number().int().min(1).default(10),
  });

  const body = schema.parse(req.body);
  const result = await pool.query(
    `INSERT INTO queues (project_id, name, priority, concurrency_limit)
     VALUES ($1, $2, $3, $4) RETURNING *`,
    [req.project!.id, body.name, body.priority, body.concurrency_limit]
  );

  res.status(201).json({ queue: result.rows[0] });
});

router.patch('/:id/toggle-pause', async (req: AuthenticatedRequest, res: Response) => {
  const result = await pool.query(
    `UPDATE queues 
     SET is_paused = NOT is_paused 
     WHERE id = $1 AND project_id = $2 
     RETURNING *`,
    [req.params.id, req.project!.id]
  );
  if (result.rowCount === 0) return res.status(404).json({ error: 'Queue not found' });
  res.json({ queue: result.rows[0] });
});

export default router;