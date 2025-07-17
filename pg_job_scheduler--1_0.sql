CREATE TABLE job_schedules (
    job_id SERIAL PRIMARY KEY,
    command TEXT NOT NULL,
    job_type TEXT NOT NULL CHECK (job_type IN ('cron', 'once')),
    schedule TEXT NOT NULL,
    next_run TIMESTAMPTZ,
    last_run TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT true
);

COMMENT ON TABLE job_schedules IS 'Scheduled jobs for pg_job_scheduler';

CREATE OR REPLACE FUNCTION pg_job_scheduler_add_job(
    command TEXT,
    job_type TEXT,
    schedule TEXT
) RETURNS INTEGER AS $$
DECLARE
    next_time TIMESTAMPTZ;
BEGIN
    IF job_type = 'once' THEN
        next_time := NOW() + schedule::INTERVAL;
    ELSIF job_type = 'cron' THEN
        SELECT * INTO next_time 
        FROM pg_job_scheduler_compute_next_run(schedule, NOW()) 
        AS next_run;
    ELSE
        RAISE EXCEPTION 'Invalid job type: %', job_type;
    END IF;

    INSERT INTO job_schedules (command, job_type, schedule, next_run, active)
    VALUES (command, job_type, schedule, next_time, true)
    RETURNING job_id INTO next_time;

    RETURN next_time;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_job_scheduler_remove_job(job_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE job_schedules SET active = false 
    WHERE job_schedules.job_id = remove_job.job_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_job_scheduler_compute_next_run(
    cron_expr TEXT,
    from_time TIMESTAMPTZ
) RETURNS TIMESTAMPTZ AS 'MODULE_PATHNAME', 'pg_job_scheduler_compute_next_run'
LANGUAGE C STRICT;