EXTENSION = pg_job_scheduler
DATA = pg_job_scheduler--1.0.sql
MODULE_big = pg_job_scheduler
OBJS = pg_job_scheduler.o
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)