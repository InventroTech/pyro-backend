from django.core.management.base import BaseCommand

from crm_records.entity_type_discovery import (
    DEFAULT_BATCH_SIZE,
    discover_entity_types_from_records,
    reset_discovery_bookmark,
)


class Command(BaseCommand):
    help = "Backfill tenant entity type field schemas from existing records."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help="Number of records to process per batch.",
        )
        parser.add_argument(
            "--max-runtime-seconds",
            type=int,
            default=None,
            help="Optional runtime limit. Omit to process until no records remain.",
        )
        parser.add_argument(
            "--reset-bookmark",
            action="store_true",
            help="Start discovery from the beginning of the records table.",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        max_runtime_seconds = options["max_runtime_seconds"]

        if options["reset_bookmark"]:
            reset_discovery_bookmark()
            self.stdout.write(self.style.WARNING("Reset entity type discovery bookmark."))

        result = discover_entity_types_from_records(
            batch_size=batch_size,
            max_runtime_seconds=max_runtime_seconds or 86400,
        )

        self.stdout.write(
            self.style.SUCCESS(
                "Entity type backfill complete: "
                f"processed={result.processed}, "
                f"entity_types_touched={result.entity_types_touched}, "
                f"schemas_updated={result.schemas_updated}, "
                f"last_processed_record_id={result.last_processed_record_id}, "
                f"last_processed_updated_at={result.last_processed_updated_at}, "
                f"has_more={result.has_more}"
            )
        )
