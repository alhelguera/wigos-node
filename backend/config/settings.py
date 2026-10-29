import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "insecure-dev-key-change-me")

DEBUG = os.environ.get("DJANGO_DEBUG", "true").lower() == "true"

ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1,backend").split(",")

INSTALLED_APPS = [
    "api.apps.ApiConfig",
    "outbox.apps.OutboxConfig",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "django.contrib.postgres",
    "guardian"
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.contrib.gis.db.backends.postgis",
        "NAME": os.environ.get("POSTGRES_DB", "app_db"),
        "USER": os.environ.get("POSTGRES_USER", "app_user"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "app_password"),
        "HOST": os.environ.get("POSTGRES_HOST", "db"),
        "PORT": os.environ.get("POSTGRES_PORT", "5432"),
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

AUTH_USER_MODEL = 'api.WIGOSNodeUser'

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

CORS_ALLOWED_ORIGINS = os.environ.get(
    "CORS_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
).split(",")

# Allow the SPA to send the session cookie on cross-origin API calls (used when
# the frontend talks to :8000 directly instead of through the Vite dev proxy).
CORS_ALLOW_CREDENTIALS = True

# Origins Django trusts for unsafe (POST/PUT) requests. The Vite dev proxy keeps
# the browser on :5173 while forwarding to Django, so that origin must be listed
# or every login/write request is rejected with 403.
CSRF_TRUSTED_ORIGINS = os.environ.get(
    "CSRF_TRUSTED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
).split(",")

# WIS2 publishing (repo-root publishing/): this node's WIS2 centre id, and the
# public web storage the announced record files are served from.
WNM_CENTRE_ID = os.environ.get("WNM_CENTRE_ID", "io-wis2dev-test-wigosnode")
WNM_STORAGE_URL = os.environ.get("WNM_STORAGE_URL", "https://wigosnode.test.wis2dev.io/webstorage")

# Publishers the outbox worker runs, in this order, for every station change.
# Classes are dotted paths into any installed package (publishing.registry).
# Both still log instead of storing/sending until the MinIO and MQTT
# transports exist (docs/publishing-plan.md, commits 12-13).
PUBLISHERS = [
    {
        "NAME": "wis2-storage",
        "GENERATOR": {
            "CLASS": "publishing.generators.wmdr2.RecordFileGenerator",
            "OPTIONS": {"centre_id": WNM_CENTRE_ID},
        },
        "TRANSPORT": {
            "CLASS": "publishing.transports.log.LogTransport",
            # No link until the MinIO transport exists (commit 12): nothing is
            # stored, so a link could not be retrieved for the WNM.
            # "OPTIONS": {"url": WNM_STORAGE_URL},
        },
    },
    {
        "NAME": "wis2-wnm",
        "GENERATOR": {
            "CLASS": "publishing.generators.wnm.WNMGenerator",
            # skip_missing until commit 12: the log storage gives no link, so
            # there is nothing to announce yet. Remove it with the S3Transport.
            "OPTIONS": {"centre_id": WNM_CENTRE_ID, "skip_missing": True},
        },
        "TRANSPORT": {
            "CLASS": "publishing.transports.log.LogTransport",
            # Placeholder: the WMDR2 topic is not decided yet
            "OPTIONS": {"topic": os.environ.get("WIS2_TOPIC", f"origin/a/wis2/{WNM_CENTRE_ID}/TBD-wmdr2")},
        },
        "ENABLED": os.environ.get("WIS2_PUBLISH_WNM", "true") == "true",
    },
]

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.AllowAny",
    ],
}

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',  # this is default
    'guardian.backends.ObjectPermissionBackend',
)
