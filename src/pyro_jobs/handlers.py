import logging
from pyro_jobs.jobs.dispatch_data_sync import run_dispatch_sync

logger = logging.getLogger(__name__)


JOB_HANDLERS = {
    "dispatch_data_sync":  run_dispatch_sync,
}
