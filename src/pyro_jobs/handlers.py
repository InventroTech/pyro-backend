import logging
from pyro_jobs.jobs.check_render_metrics import run_check_render_metrics
from pyro_jobs.jobs.check_supabase_metrics import run_check_supabase_metrics
from pyro_jobs.jobs.close_stale_self_trial_support_tickets import run_close_stale_self_trial_support_tickets
from pyro_jobs.jobs.discover_entity_types import run_discover_entity_types
from pyro_jobs.jobs.dispatch_data_sync import run_dispatch_sync
from pyro_jobs.jobs.process_dumped_tickets import run_process_dumped_tickets
from pyro_jobs.jobs.purge_old_log_tables import run_purge_old_log_tables
from pyro_jobs.jobs.snoozed_to_not_connected_midnight import run_snoozed_to_not_connected_midnight

logger = logging.getLogger(__name__)


JOB_HANDLERS = {
    "check_render_metrics":                   run_check_render_metrics,
    "check_supabase_metrics":                 run_check_supabase_metrics,
    "close_stale_self_trial_support_tickets": run_close_stale_self_trial_support_tickets,
    "discover_entity_types":                  run_discover_entity_types,
    "dispatch_data_sync":                     run_dispatch_sync,
    "process_dumped_tickets":                 run_process_dumped_tickets,
    "purge_old_log_tables":                   run_purge_old_log_tables,
    "snoozed_to_not_connected_midnight":      run_snoozed_to_not_connected_midnight,
}
