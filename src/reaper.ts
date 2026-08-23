import { pool } from './db';

async function reapZombies() {
  try {
    const deadWorkers = await pool.query(
      `UPDATE workers 
       SET status = 'DEAD' 
       WHERE status = 'ACTIVE' AND last_seen_at < NOW() - INTERVAL '30 seconds'
       RETURNING id`
    );

    if (deadWorkers.rowCount && deadWorkers.rowCount > 0) {
      const ids = deadWorkers.rows.map(w => w.id);
      const reclaimed = await pool.query(
        `UPDATE jobs 
         SET status = 'QUEUED', claimed_by_worker_id = NULL 
         WHERE claimed_by_worker_id = ANY($1::uuid[]) AND status IN ('CLAIMED', 'RUNNING')
         RETURNING id`,
        [ids]
      );
      console.warn(`[Reaper] Reclaimed ${reclaimed.rowCount} jobs from dead workers: ${ids.join(', ')}`);
    }
  } catch (err) {
    console.error('[Reaper Error]:', err);
  }
}

console.log('[Reaper] Active monitoring zombie worker reclamation daemon...');
setInterval(reapZombies, 15000);