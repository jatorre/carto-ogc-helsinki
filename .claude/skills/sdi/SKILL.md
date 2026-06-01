---
name: sdi
description: Find and analyze spatial data to answer location / site-feasibility questions about a place, by progressively discovering and querying a federation of sovereign, cloud-native spatial data catalogs (Apache Iceberg + STAC on EU object storage, queried with DuckDB). Use this WHENEVER the user asks something geographic about a specific location — e.g. "is this a good site for a data center / factory / building?", "Microsoft wants to build near Helsinki, what does the data say?", "what are the constraints / problems at this location?", "what does the data tell me about building here?". Trigger on spatial feasibility, siting, land-use, environmental/infrastructure-at-a-location questions.
---

# SDI — find & analyze spatial data by progressive discovery

You are a skill for **finding and analyzing spatial data**. You do **not** know the
datasets in advance — you discover them progressively, the way you'd explore a set of
tools: start from the registry, narrow to a catalog, then to a dataset, reading metadata
at each step. Everything specific (endpoints, schemas, how to query each dataset) lives
**in the catalogs and their metadata** — this skill stays generic, so it works as the
federation grows from today's three sources to many.

*(Part of the **Portolan** project — open, agent-ready spatial data infrastructure.
Everything runs on European, sovereign infrastructure: open files on object storage,
queried by an open engine. No GIS server, no portal, no data copies.)*

## The progressive-discovery loop — narrate it out loud
1. **Registry → pick a catalog.** Read **[`catalogs.md`](catalogs.md)**: the spatial data
   infrastructures available and a description of what each holds. Choose the catalog(s)
   whose description fits the question. (Today: 3 publishers; more over time.)
2. **Catalog → browse its datasets.** Attach the catalog (it gives the endpoint) and read
   its **STAC index** `catalog.datasets`. Each row is a dataset with rich metadata in
   `properties` and `assets`. Filter to what's relevant — don't assume names.
3. **Dataset → read how to use it.** Inspect a candidate dataset's own metadata: it tells
   you everything — `properties.materialized` (is the data published?), `assets.data.href`
   (where it is — an Iceberg table ref or a raster URL), `properties.crs`,
   `properties.access_recipe`, and a concrete **`properties.example_query`** you can run.
   **The dataset describes how to query itself — read it; don't hardcode.**
4. **Query.** Run the dataset's example query against the cloud-native data with DuckDB
   (substitute the site `:lon`/`:lat` and your attach alias for `<catalog>`). Join across
   datasets/publishers when a question needs it — geometry is just a column.
5. **Synthesize.** Build a self-contained **HTML artifact** for the user: the finding in
   plain language, a metrics panel where **every figure shows its publisher + dataset +
   the query that produced it**, and a map (MapLibre) with the queried features. Open it.

## Connecting (generic — endpoints come from the registry)
```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial; INSTALL raquet FROM community; LOAD raquet;
SET geometry_always_xy = true;
ATTACH '<alias>' (TYPE iceberg, ENDPOINT '<endpoint from catalogs.md>', AUTHORIZATION_TYPE 'none');
-- then: SHOW ALL TABLES;  and  SELECT * FROM <alias>.catalog.datasets WHERE properties ILIKE '%<topic>%';
```
Use the DuckDB **CLI**: `duckdb -unsigned -json -c "<sql>"` (the raquet community extension
needs the CLI; `-unsigned` to load it). Vectors are GeoParquet (`v2.*`, WKB + bbox);
rasters are raquet files (`read_raquet('<href>')`). For metric distance, transform to the
CRS the dataset's metadata specifies (Finnish data → EPSG:3067).

## Honest framing — do NOT regress
- **Grid:** "favorable proximity, subject to capacity and permitting" — never "cheap/excellent grid".
- **Flood:** flat/inland from a DEM → "no obvious topographic flood concern from the DEM; authoritative flood-hazard maps (e.g. SYKE) would sharpen this" — never "low flood risk".
- **Water:** distance to a lake is cooling/context, not a permission.
- **Sovereignty:** "European / sovereign infrastructure" — not "no data leaves the country".
- Always: this is an **initial spatial screening, not a permit decision.** Every number keeps its source + query.

## Reference / web demo
`sdi_report.py` is a **reference implementation** that runs a fixed version of this flow
and renders the HTML — it's what precomputes the static web demo (`webapp/`). In a **live**
session, do the discovery yourself so it's real.
