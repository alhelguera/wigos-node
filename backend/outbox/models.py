from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import models

from publishing.base import Event, Operation


class PublicationEvent(models.Model):
    """A committed station change waiting to be (or already) published. The
    row is written in the same transaction as the change and holds a snapshot,
    so it outlives the station."""

    class Status(models.TextChoices):
        PENDING = "pending"
        PROCESSING = "processing"  # claimed by a worker
        PROCESSED = "processed"  # every publisher ran (our broker accepted the WNM)
        FAILED = "failed"  # gave up after the last attempt
        PUBLISHED = "published"  # the WNM came back from upstream (MQTT listener)

    class Source(models.TextChoices):
        API = "api", "API"  # a regular user, through the API
        ADMIN = "admin", "Administrator"  # a superuser, through the API or the Django admin
        SYSTEM = "system", "System"  # the shell, scripts, migrations

    operation = models.CharField(max_length=6, choices=[(o.value, o.name.title()) for o in Operation])
    # plain integer, not a FK: the row outlives a deleted station, and
    # Postgres never reuses the id
    station_id = models.BigIntegerField(db_index=True)
    wigos_identifier = models.CharField(max_length=255, db_index=True)
    wmdrv2 = models.JSONField()  # the record as stored when the change was made
    previous_wmdrv2 = models.JSONField(null=True, blank=True)

    # attribution only; never set for superusers (source tells them apart)
    changed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    source = models.CharField(max_length=6, choices=Source.choices, default=Source.SYSTEM)
    # issuers of every old and new WIGOS identifier: who may see the event
    issuers = ArrayField(models.CharField(max_length=255), default=list, blank=True)
    # station:<pk> and wsi:<id>; events sharing a key are published in order
    ordering_keys = ArrayField(models.CharField(max_length=300), default=list, blank=True)

    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING)
    locked_until = models.DateTimeField(null=True, blank=True)  # lease of the worker on it
    completed = ArrayField(models.CharField(max_length=255), default=list, blank=True)  # publisher names
    deliveries = models.JSONField(default=list, blank=True)  # receipts of the completed publishers
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(null=True, blank=True)  # None: as soon as possible
    last_error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["id"]
        indexes = [
            GinIndex(fields=["issuers"], name="outbox_event_issuers_gin"),
            GinIndex(fields=["ordering_keys"], name="outbox_event_ordering_gin"),
            models.Index(fields=["status", "next_attempt_at"], name="outbox_event_due"),
        ]

    def __str__(self):
        return f"{self.operation} {self.wigos_identifier} ({self.status})"

    def to_event(self):
        """The plain snapshot the publishing plugins work with."""
        return Event(
            id=self.id,
            operation=Operation(self.operation),
            station_id=self.station_id,
            wigos_identifier=self.wigos_identifier,
            wmdrv2=self.wmdrv2,
            previous_wmdrv2=self.previous_wmdrv2,
            changed_by=self.changed_by_id,
            source=self.source,
            occurred_at=self.created_at,
        )
