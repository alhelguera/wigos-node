"""The publication state of each station kept in Valkey: everything apart from
the status, which lives on the station. Losing it costs the file links and the
last error until the next publication, and the next WNM goes out as canonical."""

import json
from functools import cache

import redis

from .base import Delivery, Location, Message
from .celery import app

# Longer than any real job. If a process dies, its lock frees itself after this.
LOCK_TIMEOUT = 300  # seconds


@cache
def client():
    return redis.Redis.from_url(app.conf.broker_url)


def station_lock(station_id):
    """Held while a job runs for the station, so that no second job runs
    alongside it. acquire() doesn't wait: a job finding it taken stops, and the
    running job picks up any newer save when it finishes."""
    return client().lock(f"publication-lock:{station_id}", timeout=LOCK_TIMEOUT, blocking=False)


def is_locked(station_id):
    return station_lock(station_id).locked()


def load(station_id):
    """{"deliveries": [...], "leftovers": [...], "error": str, "finished_at": str | None}"""
    saved = client().get(f"publication:{station_id}")
    return json.loads(saved) if saved else {"deliveries": [], "leftovers": [], "error": "", "finished_at": None}


def save(station_id, record):
    client().set(f"publication:{station_id}", json.dumps(record))


def dump_delivery(delivery):
    """A receipt as saved. The body isn't kept."""
    location = delivery.location
    return {
        "publisher": delivery.publisher,
        "step": delivery.step,
        "name": delivery.message.name,
        "media_type": delivery.message.media_type,
        "topic": delivery.message.topic,
        "url": location.url if location else None,
        "internal_url": location.internal_url if location else None,
    }


def load_delivery(data):
    return Delivery(
        publisher=data["publisher"],
        message=Message(body=None, media_type=data["media_type"], name=data["name"], topic=data["topic"]),
        location=Location(data["url"], data["internal_url"]) if data["url"] else None,
        step=data["step"],
    )
