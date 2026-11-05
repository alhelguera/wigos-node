from django.apps import AppConfig


class OutboxConfig(AppConfig):
    """The old publishing, switched off: station saves no longer queue events.
    Being replaced as described in docs/publishing-refactoring.md."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "outbox"

    def ready(self):
        from . import checks, publishers  # noqa: F401 (registers the check and cache reset)
