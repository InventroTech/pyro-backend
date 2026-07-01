import logging
from pyro_jobs.jobs.dispatch_data_sync import run_dispatch_sync
from pyro_jobs.jobs.purge_old_log_tables import run_purge_old_log_tables
from pyro_jobs.jobs.snoozed_to_not_connected_midnight import run_snoozed_to_not_connected_midnight

logger = logging.getLogger(__name__)


JOB_HANDLERS = {
    "dispatch_data_sync":                    run_dispatch_sync,
    "purge_old_log_tables":                  run_purge_old_log_tables,
    "snoozed_to_not_connected_midnight":     run_snoozed_to_not_connected_midnight,
}
