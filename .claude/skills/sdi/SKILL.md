---
name: sdi
description: Answer spatial / location / site-feasibility questions about a place by querying a live, sovereign, cloud-native Spatial Data Infrastructure. Use this WHENEVER the user asks something geographic about a specific location — e.g. "is this a good site for a data center / factory / building?", "Microsoft wants to build near Helsinki, what does the data say?", "what are the constraints / problems at this location?", "what does the data tell me about building here?". Connects to an Iceberg/STAC catalog, discovers datasets, queries them with DuckDB, and writes an HTML report with a map. Trigger on spatial feasibility, siting, land-use, environmental/infrastructure-at-a-location questions.
---

# SDI — ask a sovereign spatial data infrastructure

A next-generation SDI is something an agent can just *use*: a sovereign,
serverless **Iceberg catalog** on EU object storage whose index is **STAC
(stac-geoparquet)** and whose data is **cloud-native** (GeoParquet + raquet).
You discover datasets by querying the catalog, query the data with DuckDB, and
return a report — no GIS server, no portal, no data copies, nothing touching a
US service.

The catalog already exists (a published artifact). You **use** it; you don't build it.

## The catalog
- **Endpoint:** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog` (UpCloud, sovereign 🇫🇮, anonymous read)
- **Attach (DuckDB):**
  ```sql
  INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
  INSTALL spatial; LOAD spatial; INSTALL raquet FROM community; LOAD raquet;
  SET geometry_always_xy = true;
  ATTACH 'sdi' AS sdi (TYPE iceberg, ENDPOINT '<endpoint>', AUTHORIZATION_TYPE 'none');
  ```
- **STAC index:** `sdi.catalog.datasets` — stac-geoparquet (id, collection, geometry, bbox, datetime, properties, assets). 130 datasets catalogued; `properties.materialized=true` ones have published cloud-native data (`assets.data.href`); the rest are convert-on-demand.
- **Materialized data:** `sdi.v2.{power_lines,protected,water}` (GeoParquet, WKB `geom_wkb` + bbox) and the rasters `…/data/raster/{dem_2m,ndvi}.parquet` (raquet).

## How to answer (the live flow)
1. **Parse** the question → a location (lon/lat — geocode the place name if needed) + intent (default: data-center siting feasibility).
2. **Discover — out loud.** Search the catalog's STAC index and say what you found:
   ```sql
   SELECT id, collection, json_extract_string(properties,'$.materialized') AS materialized
   FROM sdi.catalog.datasets WHERE properties ILIKE '%data-center%' ORDER BY materialized DESC;
   ```
   "The catalog has 130 datasets; these bear on siting a data center, and these are already cloud-native."
3. **Analyse + report** — run the engine (it attaches the catalog, queries each
   materialized dataset, and renders the HTML):
   ```bash
   python3 .claude/skills/sdi/sdi_report.py --lon <lon> --lat <lat> --name "<place>" --question "<their words>"
   open demo/output/sdi_report.html
   ```

## What the analysis covers (all from the catalog, via DuckDB)
- **Grid** — distance to nearest transmission line (`sdi.v2.power_lines`)
- **Terrain** — flatness from the 2 m DEM raquet (elevation σ)
- **Flood / water** — min elevation + distance to lakes (`sdi.v2.water`)
- **Environment** — distance to protected areas (`sdi.v2.protected`) + vegetation/forest from NDVI raquet

## Narrate while it runs
- "It connects to a sovereign Iceberg catalog on European object storage — no server."
- "It searches the STAC index, finds the datasets that bear on a data center, sees which are cloud-native, and queries them directly with DuckDB — GeoParquet for vectors, raquet for raster."
- On the map: "Here's the answer — and none of it touched a US service: data on a Finnish cloud, queried by an open engine."

## Honesty
- This is an initial screen from open data at modest resolution — not a permit decision (the report says so).
- The demo site (Espoo Hepokorpi) is a **real, contested** Microsoft data-center location; the data tends to confirm the real objections (forest/nature), which is the point — the agent surfaces the tension.
