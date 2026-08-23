CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enums
CREATE TYPE job_status AS ENUM ('QUEUED', 'SCHEDULED', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'DEAD_LETTER');
CREATE TYPE retry_strategy AS ENUM ('FIXED', 'LINEAR', 'EXPONENTIAL');
CREATE TYPE worker_status AS ENUM ('ACTIVE', 'IDLE', 'DRAINING', 'DEAD', 'STOPPED');

-- Organizations & Projects (Auth Scoping)
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL DEFAULT 'MEMBER',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    api_key VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Retry Policies
CREATE TABLE retry_policies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    strategy retry_strategy NOT NULL DEFAULT 'EXPONENTIAL',
    max_retries INT NOT NULL DEFAULT 3 CHECK (max_retries >= 0),
    base_delay_seconds INT NOT NULL DEFAULT 5 CHECK (base_delay_seconds > 0),
    max_delay_seconds INT NOT NULL DEFAULT 3600 CHECK (max_delay_seconds >= base_delay_seconds),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Queues
CREATE TABLE queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    priority INT NOT NULL DEFAULT 1 CHECK (priority BETWEEN 1 AND 100),
    concurrency_limit INT NOT NULL DEFAULT 10 CHECK (concurrency_limit > 0),
    is_paused BOOLEAN NOT NULL DEFAULT FALSE,
    retry_policy_id UUID REFERENCES retry_policies(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, name)
);

-- Workers
CREATE TABLE workers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname VARCHAR(255) NOT NULL,
    status worker_status NOT NULL DEFAULT 'ACTIVE',
    active_jobs_count INT NOT NULL DEFAULT 0 CHECK (active_jobs_count >= 0),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Jobs
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority INT NOT NULL DEFAULT 1 CHECK (priority BETWEEN 1 AND 100),
    status job_status NOT NULL DEFAULT 'QUEUED',
    attempt INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_by_worker_id UUID REFERENCES workers(id) ON DELETE SET NULL,
    claimed_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cron_expression VARCHAR(100),
    parent_job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (queue_id, idempotency_key)
);

-- Job Executions
CREATE TABLE job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id UUID REFERENCES workers(id) ON DELETE SET NULL,
    attempt INT NOT NULL,
    status job_status NOT NULL,
    error_message TEXT,
    stack_trace TEXT,
    execution_time_ms INT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

-- Job Logs
CREATE TABLE job_logs (
    id BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    log_level VARCHAR(20) NOT NULL DEFAULT 'INFO',
    message TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dead Letter Queue (DLQ)
CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    queue_id UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    exhausted_attempts INT NOT NULL,
    last_error_message TEXT,
    last_stack_trace TEXT,
    payload JSONB NOT NULL,
    replayed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_jobs_polling ON jobs (queue_id, priority DESC, scheduled_at ASC) WHERE status = 'QUEUED';
CREATE INDEX idx_jobs_scheduled ON jobs (scheduled_at) WHERE status = 'SCHEDULED';
CREATE INDEX idx_jobs_worker_active ON jobs (claimed_by_worker_id) WHERE status IN ('CLAIMED', 'RUNNING');
CREATE INDEX idx_workers_heartbeat ON workers (last_seen_at) WHERE status = 'ACTIVE';
CREATE INDEX idx_job_executions_job_id ON job_executions (job_id, attempt DESC);

-- Seed Initial Auth & Queue Data
INSERT INTO organizations (id, name) 
VALUES ('00000000-0000-0000-0000-000000000001', 'Default Organization')
ON CONFLICT DO NOTHING;

INSERT INTO projects (id, org_id, name, api_key) 
VALUES ('00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'Production Scheduler', 'test-api-key-12345')
ON CONFLICT DO NOTHING;

INSERT INTO retry_policies (id, project_id, name, strategy, max_retries, base_delay_seconds, max_delay_seconds)
VALUES ('00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000002', 'Default Exponential Policy', 'EXPONENTIAL', 3, 5, 300)
ON CONFLICT DO NOTHING;

INSERT INTO queues (id, project_id, name, priority, concurrency_limit, retry_policy_id)
VALUES 
  ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000002', 'critical-queue', 10, 20, '00000000-0000-0000-0000-000000000003'),
  ('00000000-0000-0000-0000-000000000005', '00000000-0000-0000-0000-000000000002', 'default-queue', 1, 10, '00000000-0000-0000-0000-000000000003')
ON CONFLICT DO NOTHING;