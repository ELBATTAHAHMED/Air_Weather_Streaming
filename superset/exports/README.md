# Superset Export Bundle

Target bundle path:

- `superset/exports/air_quality_weather_bundle.zip`

## Regenerate from a running Superset instance

From project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\superset\exports\export_bundle.ps1
```

The script exports dashboards (including linked charts/datasets) from the `superset` container and copies the zip into this folder.

## Import into a clean Superset

1. Open Superset UI.
2. Go to `Settings -> Import dashboards`.
3. Select `superset/exports/air_quality_weather_bundle.zip`.
4. Enable overwrite if updating an existing environment.
