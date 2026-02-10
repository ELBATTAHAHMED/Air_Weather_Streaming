import os


SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "superset_m2_bigdata_air_quality_weather_platform_secret_key",
)

SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DB_URI",
    "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
)

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "HORIZONTAL_FILTER_BAR": False,
    "FILTERBAR_CLOSED_BY_DEFAULT": False,
}

ENABLE_TEMPLATE_PROCESSING = True
