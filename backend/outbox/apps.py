from django.apps import AppConfig


class OutboxConfig(AppConfig):
    """Glue between the stations and the plugin based publishing in the
    repo-root publishing/ package (docs/publishing-plan.md). Removing this app
    from INSTALLED_APPS switches publishing off."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "outbox"

    def ready(self):
        from . import checks, publishers  # noqa: F401 (registers the check and the cache reset)
