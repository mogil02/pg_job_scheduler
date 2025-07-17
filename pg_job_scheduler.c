#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "tcop/tcopprot.h"
#include "executor/spi.h"
#include "commands/dbcommands.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/datetime.h"
#include "storage/procsignal.h"
#include "pgstat.h"  

PG_MODULE_MAGIC;

typedef struct {
    bool sigterm;
} WorkerState;

static void handle_sigterm(SIGNAL_ARGS);
static void job_scheduler_main(Datum main_arg);
static TimestampTz compute_next_cron_time(const char *cron_expr, TimestampTz from_time);

PG_FUNCTION_INFO_V1(pg_job_scheduler_compute_next_run);
Datum
pg_job_scheduler_compute_next_run(PG_FUNCTION_ARGS) {
    text *cron_expr_text = PG_GETARG_TEXT_PP(0);
    TimestampTz from_time = PG_GETARG_TIMESTAMPTZ(1);
    char *cron_expr = text_to_cstring(cron_expr_text);
    TimestampTz next_time = compute_next_cron_time(cron_expr, from_time);
    PG_RETURN_TIMESTAMPTZ(next_time);
}

static TimestampTz
compute_next_cron_time(const char *cron_expr, TimestampTz from_time) {
    int target_min = -1;
    int target_hour = -1;
    char min_str[10], hour_str[10], day_str[10], month_str[10], dow_str[10];
    TimestampTz current;
    int64 epoch_diff = 946684800;
    int i;
    time_t unix_time;
    struct pg_tm *tm;

    if (sscanf(cron_expr, "%9s %9s %9s %9s %9s", 
               min_str, hour_str, day_str, month_str, dow_str) != 5)
        elog(ERROR, "Invalid cron expression format");

    if (strcmp(day_str, "*") != 0 || 
        strcmp(month_str, "*") != 0 || 
        strcmp(dow_str, "*") != 0)
        elog(ERROR, "Only minutes and hours are supported in cron expression");

    if (strcmp(min_str, "*") == 0) target_min = -1;
    else target_min = atoi(min_str);
    
    if (strcmp(hour_str, "*") == 0) target_hour = -1;
    else target_hour = atoi(hour_str);

    if ((target_min != -1 && (target_min < 0 || target_min > 59)) ||
        (target_hour != -1 && (target_hour < 0 || target_hour > 23)))
        elog(ERROR, "Invalid cron value");

    current = from_time;
    for (i = 0; i < 525600; i++) {
        current += 60000000; // Добавляем 1 минуту (60 секунд * 1000000 мкс)
        unix_time = (time_t)((current / 1000000) + epoch_diff);
        tm = pg_gmtime(&unix_time);
        
        if ((target_min == -1 || tm->tm_min == target_min) &&
            (target_hour == -1 || tm->tm_hour == target_hour))
            return current;
    }
    elog(ERROR, "Next cron time not found");
    return 0;
}

static void
job_scheduler_main(Datum main_arg) {
    WorkerState *state = (WorkerState *) palloc(sizeof(WorkerState));
    bool isnull;
    const char *query;
    int rc;
    uint32 i;
    int job_id;
    char *command;
    char *job_type;
    char *schedule;
    int ret;
    TimestampTz next_time;
    const char *nulls = NULL;  

    state->sigterm = false;
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();

    while (!state->sigterm) {
        if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
            elog(LOG, "SPI_connect failed: %d", rc);
            goto sleep;
        }

        query = "SELECT job_id, command, job_type, schedule "
                "FROM job_schedules "
                "WHERE active AND next_run <= NOW() FOR UPDATE";
        rc = SPI_execute(query, true, 0);
        if (rc != SPI_OK_SELECT) {
            elog(LOG, "SPI_execute failed: %d", rc);
            SPI_finish();
            goto sleep;
        }

        for (i = 0; i < SPI_processed; i++) {
            job_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], 
                                                SPI_tuptable->tupdesc, 
                                                1, &isnull));
            command = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], 
                                                       SPI_tuptable->tupdesc, 
                                                       2, &isnull));
            job_type = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], 
                                                        SPI_tuptable->tupdesc, 
                                                        3, &isnull));
            schedule = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], 
                                                        SPI_tuptable->tupdesc, 
                                                        4, &isnull));

            elog(LOG, "Executing job %d: %s", job_id, command);
            ret = system(command);
            if (ret != 0)
                elog(WARNING, "Job %d failed with code %d", job_id, ret);

            if (strcmp(job_type, "once") == 0) {
                SPI_execute_with_args("UPDATE job_schedules SET active = false, "
                                      "last_run = NOW() WHERE job_id = $1",
                                      1, 
                                      (Oid[]){INT4OID}, 
                                      (Datum[]){Int32GetDatum(job_id)},
                                      &nulls, 
                                      false, 
                                      0);
            } else {
                next_time = compute_next_cron_time(schedule, GetCurrentTimestamp());
                SPI_execute_with_args("UPDATE job_schedules SET next_run = $1, "
                                      "last_run = NOW() WHERE job_id = $2",
                                      2, 
                                      (Oid[]){TIMESTAMPTZOID, INT4OID},
                                      (Datum[]){TimestampTzGetDatum(next_time), Int32GetDatum(job_id)}, 
                                      &nulls,  
                                      false, 
                                      0);
            }
        }
        SPI_finish();

sleep:
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         60000,  // 60 секунд
                         WAIT_EVENT_EXTENSION);  
        ResetLatch(MyLatch);
    }
    pfree(state);
    proc_exit(0);
}

static void
handle_sigterm(SIGNAL_ARGS) {
    int save_errno = errno;
    WorkerState *state = (WorkerState *) MyBgworkerEntry->bgw_extra;
    if (state) {
        state->sigterm = true;
        if (MyProc)
            SetLatch(&MyProc->procLatch);
    }
    errno = save_errno;
}

void
_PG_init(void) {
    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_job_scheduler");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    worker.bgw_main = job_scheduler_main;  
    worker.bgw_main_arg = (Datum) 0;
    RegisterBackgroundWorker(&worker);
}
