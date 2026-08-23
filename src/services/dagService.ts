import { pool } from '../db';

export async function unlockDependentJobs(completedJobId: string): Promise<string[]> {
  const res = await pool.query(
    `UPDATE jobs 
     SET status = 'QUEUED', scheduled_at = NOW() 
     WHERE parent_job_id = $1 AND status = 'SCHEDULED'
     RETURNING id`,
    [completedJobId]
  );
  return res.rows.map(r => r.id);
}
