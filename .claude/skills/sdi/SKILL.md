---
name: sdi
description: Answer spatial / location / site-feasibility questions about a place by querying a live, sovereign, cloud-native Spatial Data Infrastructure — a federation of Apache Iceberg + STAC catalogs on EU object storage, queried directly with DuckDB. Use this WHENEVER the user asks something geographic about a specific location — e.g. "is this a good site for a data center / factory / building?", "Microsoft wants to build near Helsinki, what does the data say?", "what are the constraints / problems at this location?", "what does the data tell me about building here?". Trigger on spatial feasibility, siting, land-use, environmental/infrastructure-at-a-location questions.
---

# SDI — ask a sovereign, federated spatial data infrastructure

A next-generation SDI is something an agent can just **use**: each publisher exposes its
own holdings as a **sovereign, serverless Apache Iceberg catalog** on EU object storage,
indexed by **STAC (stac-geoparquet)**, with **cloud-native** data (GeoParquet + raquet).
You discover datasets by querying the catalogs, query the data directly with **DuckDB**,
and synthesize an answer — no GIS server, no portal, no data copies.

**You do the work live** — attach the catalogs, search them, write the SQL, read the
results, and build the artifact. Don't run a canned script; the point is that discovery
is real and every number traces to a query. *(Part of the **Portolan** project — open,
agent-ready SDI.)*

## Step 1 — read what's out there
Open **[`catalogs.md`](catalogs.md)** (the registry): the SDIs available and what each
publisher holds. Today: three Finnish/European publishers, three Iceberg endpoints.
Tell the user what you're about to federate.

## Step 2 — parse the question
→ a **location** (lon/lat — geocode the place name if needed) + **intent** (default:
data-center siting feasibility). The canonical demo site is Espoo / Hepokorpi
(`24.6883, 60.2371`) — a real, contested Microsoft data-center location.

## Step 3 — connect (DuckDB + Iceberg), out loud
```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial; INSTALL raquet FROM community; LOAD raquet;
SET geometry_always_xy = true;
ATTACH 'nls'  (TYPE iceberg, ENDPOINT '…/catalog/national-land-survey',          AUTHORIZATION_TYPE 'none');
ATTACH 'syke' (TYPE iceberg, ENDPOINT '…/catalog/finnish-environment-institute', AUTHORIZATION_TYPE 'none');
ATTACH 'cop'  (TYPE iceberg, ENDPOINT '…/catalog/copernicus',                    AUTHORIZATION_TYPE 'none');
SHOW ALL TABLES;
```
> Use the DuckDB **CLI**: `duckdb -unsigned -json -c "<sql>"` (the raquet community
> extension needs the CLI; `-unsigned` to load it). Endpoints are in `catalogs.md`.

## Step 4 — discover, progressively
Search each publisher's STAC index for what bears on the question — narrate what you find:
```sql
SELECT id, collection, json_extract_string(properties,'$.materialized') AS materialized,
       json_extract_string(assets,'$.data.href') AS data
FROM nls.catalog.datasets
WHERE properties ILIKE '%data-center%'
ORDER BY materialized DESC;
```
> "NLS publishes 129 datasets; these bear on siting — power, water, protected areas,
> elevation — and these are already cloud-native. Copernicus has Sentinel-2 NDVI. SYKE
> has Natura 2000 and flood maps, catalogued and convertible on demand."

## Step 5 — query (and join) the cloud-native data
`materialized` datasets are queryable now. Example queries (substitute the site lon/lat):

**Vector — nearest transmission line (m), GeoParquet via WKB, metric CRS:**
```sql
SELECT round(min(ST_Distance(
  ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),
  ST_Transform(ST_Point(24.6883,60.2371),'EPSG:4326','EPSG:3067')))) AS metres
FROM nls.v2.power_lines;
```
Repeat for `nls.v2.water` (cooling/flood context) and `nls.v2.protected` (nearest protected area).

**Raster — elevation/slope from the 2 m DEM (raquet), sampled on a small grid:**
```sql
WITH grid AS (SELECT 24.6883+(i-3)*0.0009 lon, 60.2371+(j-3)*0.00045 lat
              FROM range(0,7) a(i), range(0,7) b(j))
SELECT min(elev) AS min_m, stddev(elev) AS slope_sigma FROM (
  SELECT ST_RasterValue(r.block, r.band_1, ST_Point(g.lon,g.lat), r.metadata) elev
  FROM grid g, read_raquet('…/data/raster/dem_2m.parquet') r
  WHERE ST_Contains(ST_GeomFromQuadbin(r.block), ST_Point(g.lon,g.lat)));
```

**Raster — NDVI from Copernicus Sentinel-2 (band_2=NIR, band_1=Red):**
```sql
SELECT avg((b2-b1)/(b2+b1)) AS ndvi FROM (
  SELECT ST_RasterValue(r.block,r.band_2,ST_Point(g.lon,g.lat),r.metadata) b2,
         ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) b1
  FROM grid g, read_raquet('…/data/raster/ndvi.parquet') r
  WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)));
```
Geometry is just a column — you can **join across publishers** in one statement when a
question needs it (e.g. site × NLS protected × a converted SYKE layer).

## Step 6 — synthesize an HTML artifact
Write a **self-contained HTML file** (open it for the user): the finding in plain
language, a metrics panel where **every figure shows its publisher + dataset + the query
that produced it**, and a MapLibre map with the queried features (pull nearby geometries
with `ST_AsGeoJSON(ST_GeomFromWKB(geom_wkb))`). For a recent basemap/imagery you may use
a Sentinel-2 tile via titiler. Keep provenance visible — it's the whole point.

## Honest framing (do not regress — see the project's claim rules)
- **Grid:** "favorable proximity, subject to capacity and permitting" — never "cheap/excellent grid".
- **Flood:** flat/inland from the DEM → "no obvious topographic flood concern from the DEM; SYKE flood-hazard maps would sharpen this" — never "low flood risk".
- **Water:** distance to a lake is cooling/context, not a permission.
- **Sovereignty:** "European / sovereign infrastructure" — not "no data leaves the country".
- Always: **initial spatial screening, not a permit decision.** Every number keeps its source + query.

## Reference / web demo
`sdi_report.py` is a **reference implementation** that runs this whole flow
deterministically and renders the HTML — it's what precomputes the static web demo
(`webapp/`). In a **live** session, prefer doing the steps above yourself so the
discovery is real on camera.
