import { Router, Response, NextFunction } from 'express';
import { z } from 'zod';
import { pool } from '../db';
import { AuthenticatedRequest } from '../middleware/auth';

const router = Router();

const IngestJobSchema = z.object({
  queueName: z.string().min(1, 'Queue name is required'),
  name: z.string().min(1, 'Job name is required'),
  payload: z.record(z.any()).default({}),
  priority: z.number().int().min(1).max(100).default(1),
  scheduledAt: z.string().datetime().optional(),
  cronExpression: z.string().optional(),
  parentJobId: z.string().uuid('parentJobId must be a valid UUID').optional(),
  idempotencyKey: z.string().optional(),
});

router.post('/', async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const body = IngestJobSchema.parse(req.body);

    // Resolve queue under the authenticated project
    const qRes = await pool.query(
      `SELECT id FROM queues WHERE project_id = $1 AND name = $2`,
      [req.project!.id, body.queueName]
    );

    if (qRes.rowCount === 0) {
      return res.status(404).json({ error: `Queue '${body.queueName}' not found` });
    }

    const queueId = qRes.rows[0].id;
    let initialStatus: 'QUEUED' | 'SCHEDULED' = 'QUEUED';
    const scheduledTime = body.scheduledAt ? new Date(body.scheduledAt) : new Date();

    // Determine initial status based on scheduling or workflow dependencies
    if (body.scheduledAt || body.cronExpression) {
      initialStatus = 'SCHEDULED';
    } else if (body.parentJobId) {
      const parentRes = await pool.query(
        `SELECT status FROM jobs WHERE id = $1`,
        [body.parentJobId]
      );

      if (parentRes.rowCount === 0) {
        return res.status(404).json({ error: `Parent job '${body.parentJobId}' not found` });
      }

      // If parent has already finished, queue immediately; otherwise hold in SCHEDULED
      initialStatus = parentRes.rows[0].status === 'COMPLETED' ? 'QUEUED' : 'SCHEDULED';
    }

    const insertRes = await pool.query(
      `INSERT INTO jobs (
        queue_id, idempotency_key, name, payload, priority, status, 
        scheduled_at, cron_expression, parent_job_id
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      ON CONFLICT (queue_id, idempotency_key) DO UPDATE 
        SET updated_at = NOW()
      RETURNING *`,
      [
        queueId,
        body.idempotencyKey || null,
        body.name,
        body.payload,
        body.priority,
        initialStatus,
        scheduledTime,
        body.cronExpression || null,
        body.parentJobId || null,
      ]
    );

    return res.status(201).json({ job: insertRes.rows[0] });
  } catch (err: any) {
    if (err instanceof z.ZodError) {
      return res.status(400).json({ error: 'Validation Failed', details: err.errors });
    }
    next(err);
  }
});

router.post('/batch', async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  const BatchSchema = z.object({
    queueName: z.string().min(1, 'Queue name is required'),
    jobs: z.array(
      z.object({
        name: z.string().min(1, 'Job name is required'),
        payload: z.record(z.any()).default({}),
        priority: z.number().int().min(1).max(100).default(1),
      })
    ).min(1, 'At least one job is required in batch'),
  });

  const client = await pool.connect();
  try {
    const body = BatchSchema.parse(req.body);
    await client.query('BEGIN');

    const qRes = await client.query(
      `SELECT id FROM queues WHERE project_id = $1 AND name = $2`,
      [req.project!.id, body.queueName]
    );

    if (qRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: `Queue '${body.queueName}' not found` });
    }

    const queueId = qRes.rows[0].id;
    const insertedJobs = [];

    for (const j of body.jobs) {
      const res = await client.query(
        `INSERT INTO jobs (queue_id, name, payload, priority, status)
         VALUES ($1, $2, $3, $4, 'QUEUED') 
         RETURNING id, name, status, priority, created_at`,
        [queueId, j.name, j.payload, j.priority]
      );
      insertedJobs.push(res.rows[0]);
    }

    await client.query('COMMIT');
    return res.status(201).json({ count: insertedJobs.length, jobs: insertedJobs });
  } catch (err: any) {
    await client.query('ROLLBACK');
    if (err instanceof z.ZodError) {
      return res.status(400).json({ error: 'Validation Failed', details: err.errors });
    }
    next(err);
  } finally {
    client.release();
  }
});

router.get('/', async (req: AuthenticatedRequest, res: Response, next: NextFunction) => {
  try {
    const status = req.query.status as string;
    const queueId = req.query.queue_id as string;
    const page = Math.max(1, parseInt(req.query.page as string || '1', 10));
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit as string || '20', 10)));
    const offset = (page - 1) * limit;

    let query = `
      SELECT j.*, q.name as queue_name 
      FROM jobs j
      JOIN queues q ON j.queue_id = q.id
      WHERE q.project_id = $1
    `;
    const params: any[] = [req.project!.id];

    if (status) {
      params.push(status);
      query += ` AND j.status = $${params.length}`;
    }
    if (queueId) {
      params.push(queueId);
      query += ` AND j.queue_id = $${params.length}`;
    }

    query += ` ORDER BY j.created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);

    const result = await pool.query(query, params);
    return res.json({ page, limit, count: result.rowCount, jobs: result.rows });
  } catch (err) {
    next(err);
  }
});

export default router;