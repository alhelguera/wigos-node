"""The Celery app that runs publishing jobs, found by `celery -A publishing`.

Jobs run inside the Django project, for its settings and models: Celery sets
Django up in the worker when DJANGO_SETTINGS_MODULE is set."""

import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("publishing")
app.conf.update(
    # Valkey, through its Redis client
    broker_url=os.environ.get("CELERY_BROKER_URL", "redis://valkey:6379/0"),
    broker_connection_retry_on_startup=True,
    # A station's publication status lives on the station
    task_ignore_result=True,
)
