import { Request, Response, NextFunction } from 'express';
import { pool } from '../db';

export interface AuthenticatedRequest extends Request {
  project?: { id: string; org_id: string; name: string };
}

export async function authenticateApiKey(req: AuthenticatedRequest, res: Response, next: NextFunction) {
  const apiKey = (req.headers['x-api-key'] as string) || (req.query.api_key as string);

  if (!apiKey) {
    return res.status(401).json({ error: 'Unauthorized: Missing X-API-Key header or query param' });
  }

  try {
    const result = await pool.query(
      `SELECT id, org_id, name FROM projects WHERE api_key = $1`,
      [apiKey]
    );

    if (result.rowCount === 0) {
      return res.status(403).json({ error: 'Forbidden: Invalid API Key' });
    }

    req.project = result.rows[0];
    next();
  } catch (error) {
    next(error);
  }
}