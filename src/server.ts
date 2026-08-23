import express from 'express';
import cors from 'cors';
import path from 'path';
import queueRoutes from './routes/queues';
import jobRoutes from './routes/jobs';
import workerRoutes from './routes/workers';
import dlqRoutes from './routes/dlq';
import { authenticateApiKey } from './middleware/auth';
import { processCronSchedules } from './services/cronScheduler';

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Public Health Check
app.get('/health', (_req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));

// Authenticated API V1 Routes
app.use('/api/v1/queues', authenticateApiKey, queueRoutes);
app.use('/api/v1/jobs', authenticateApiKey, jobRoutes);
app.use('/api/v1/workers', authenticateApiKey, workerRoutes);
app.use('/api/v1/dlq', authenticateApiKey, dlqRoutes);

// Cron background dispatch tick
setInterval(() => {
  processCronSchedules().catch((err) => console.error('Cron tick failure:', err));
}, 10000);

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`[API Engine] Distributed Job Scheduler API live on http://localhost:${PORT}`);
});