# Design Decisions & Engineering Trade-Offs

### 1. PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` vs Dedicated Message Brokers
- **Decision**: PostgreSQL row-level locks were chosen over RabbitMQ/Kafka.
- **Trade-off**: Relational locks offer ACID transactions, zero dual-write synchronization errors, dynamic multi-tenant queries, native pause/resume capabilities, and ad-hoc job explorer filtering without running extra cluster infrastructure.

### 2. At-Least-Once Delivery Semantics
- **Decision**: Jobs are marked `COMPLETED` only after successful worker execution.
- **Crash Recovery**: If a worker node crashes mid-execution, its heartbeat stops emitting. The Reaper background daemon marks the worker `DEAD` after 30 seconds and transitions orphaned jobs back to `QUEUED`.
- **Requirement**: Execution tasks should remain idempotent.

### 3. Distributed Coordination & Cron Ingestion
- **Decision**: Redis distributed locks (`NX`, `PX`) ensure cron evaluations execute once per cluster tick without duplicate scheduled jobs.