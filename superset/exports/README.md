# Superset Export Bundle

Target bundle path:

- `superset/exports/air_quality_weather_bundle.zip`

Bundle source YAML (versioned assets):

- `superset/exports/bundle_src/air_quality_weather_bundle/`

## Regenerate from a running Superset instance

From project root:

```powershell
powershell -ExecutionPolicy Bypass -File .\superset\exports\export_bundle.ps1
```

The script exports dashboards (including linked charts/datasets) from the `superset` container and copies the zip into this folder.

If you need a deterministic fallback artifact without container export, rebuild from YAML source with:

```powershell
py -c "import pathlib, zipfile; src=pathlib.Path('superset/exports/bundle_src/air_quality_weather_bundle'); out=pathlib.Path('superset/exports/air_quality_weather_bundle.zip'); z=zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED); [z.write(p, p.as_posix().replace('superset/exports/bundle_src/','')) for p in src.rglob('*') if p.is_file()]; z.close()"
```

## Import into a clean Superset

1. Open Superset UI.
2. Go to `Settings -> Import dashboards`.
3. Select `superset/exports/air_quality_weather_bundle.zip`.
4. Enable overwrite if updating an existing environment.
