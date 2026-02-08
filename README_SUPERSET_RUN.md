# Superset Run (Trino + Hive Lakehouse)

Guide de reproduction pour M2 Big Data.

## 1) Pre-requis

Les services suivants doivent deja etre operationnels:

- HDFS + Hive Metastore (compose Hadoop)
- Trino (catalog `hive` fonctionnel)
- Tables Gold presentes dans `hive.lakehouse`

Validation rapide Trino:

```powershell
docker exec trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM hive.lakehouse"
```

## 2) Demarrage Superset (Docker)

Depuis la racine du projet:

```powershell
cd C:\Users\LENOVO\Desktop\project-air-quality

# 1) Demarrer metadata DB Superset
docker compose -f .\docker\docker-compose.superset.yml up -d superset-db

# 2) Initialiser Superset (migration + admin + roles/permissions)
docker compose -f .\docker\docker-compose.superset.yml run --rm superset-init

# 3) Demarrer le serveur web Superset
docker compose -f .\docker\docker-compose.superset.yml up -d superset
```

Acces UI:

- URL: `http://localhost:8082`
- User: `admin`
- Password: `admin123`

## 3) Connexion Trino dans Superset

Dans Superset:

1. `Settings` -> `Database Connections` -> `+ Database`
2. Choisir `Trino`
3. URI SQLAlchemy:

```text
trino://trino@trino:8080/hive/lakehouse
```

4. Cliquer `Test Connection` puis `Connect`.

## 4) Datasets et SQL decisionnel

Utiliser SQL Lab pour creer des datasets virtuels, puis construire les charts.

### A) Evolution temperature moyenne par ville

```sql
SELECT
  date_trunc('hour', window_start) AS ts_hour,
  city,
  avg(avg_temperature) AS avg_temp_c
FROM hive.lakehouse.gold_weather_metrics
GROUP BY 1, 2
ORDER BY 1, 2
```

Type de chart recommande:

- `Time-series Line Chart`
- Time column: `ts_hour`
- Metric: `avg_temp_c`
- Series: `city`

### B) Evolution NO2 / PM10 / PM2.5 par ville

```sql
SELECT
  date_trunc('hour', window_start) AS ts_hour,
  city,
  upper(parameter) AS pollutant,
  avg(rolling_avg) AS pollutant_avg
FROM hive.lakehouse.gold_air_quality_metrics
WHERE lower(parameter) IN ('no2', 'pm10', 'pm25', 'pm2.5')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

Type de chart recommande:

- `Time-series Line Chart`
- Time column: `ts_hour`
- Metric: `pollutant_avg`
- Series: `pollutant` (filtre `city` global dashboard)

### C) Table des pics de pollution

```sql
SELECT
  window_start,
  window_end,
  city,
  upper(parameter) AS pollutant,
  rolling_avg,
  prev_rolling_avg,
  delta,
  status,
  is_peak
FROM hive.lakehouse.gold_air_quality_peaks
WHERE is_peak = true
ORDER BY window_start DESC
LIMIT 500
```

Type de chart recommande:

- `Table`
- Tri descendant sur `window_start`

## 5) Structure de dashboard final (M2)

Nom suggere:

- `M2 - Air & Weather Decision Dashboard`

Sections recommandees:

1. **KPI (bandeau haut)**
   - Nb villes actives (24h)
   - Temperature moyenne globale (24h)
   - Nb pics de pollution (24h)
2. **Meteo (milieu gauche)**
   - Evolution temporelle temperature par ville
3. **Qualite de l'air (milieu droite)**
   - Evolution NO2/PM10/PM2.5 par ville
4. **Alertes/Pics (bas)**
   - Table des pics de pollution

Filtres dashboard:

- Ville
- Polluant
- Fenetre temporelle

## 6) Checklist de validation

```powershell
# Superset
docker ps --filter "name=superset"

# Trino depuis CLI
docker exec trino trino --server http://localhost:8080 --execute "SHOW CATALOGS"
docker exec trino trino --server http://localhost:8080 --execute "SHOW SCHEMAS FROM hive"
docker exec trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM hive.lakehouse"
```

Dans Superset:

- connexion Trino = OK
- datasets SQL executent sans erreur
- 3 visualisations creees et ajoutees au dashboard

## 6bis) Export reproductible du dashboard

Apres creation/validation du dashboard dans Superset, exporter le bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\superset\exports\export_bundle.ps1
```

Fichier attendu:

- `superset/exports/air_quality_weather_bundle.zip`

## 7) Bonnes pratiques de nommage et documentation (rapport M2)

### Nommage

- Dashboard: `M2_<Theme>_Decision_Dashboard`
- Dataset virtuel: `vds_<domain>_<grain>_<purpose>`
  - ex: `vds_air_hourly_pollutants`
- Charts: `ch_<domain>_<metric>_<viztype>`
  - ex: `ch_weather_avg_temp_line`

### Documentation a inclure dans le memo

- Objectif metier de chaque visualisation (1-2 phrases)
- SQL utilise (annexe)
- Source table Gold et grain temporel
- Limites connues (retard streaming, valeurs manquantes)
- Choix d'architecture (Superset -> Trino -> Hive/HDFS)

### Justification academique

- Separation claire ingestion / processing / serving
- Couche Gold exploitee pour la decision
- Federation SQL via Trino sans acces direct HDFS par BI
