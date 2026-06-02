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
federation grows from today's nine sources to many.

*(Part of the **Portolan** project — open, agent-ready spatial data infrastructure.
Everything runs on European, sovereign infrastructure: open files on object storage,
queried by an open engine. No GIS server, no portal, no data copies.)*

## The progressive-discovery loop — narrate it out loud
1. **Registry → pick a catalog.** Fetch the **reference catalog of catalogs** — a STAC
   `Catalog` on sovereign object storage at
   `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/stac.json`. Each
   `child` link is an independent publisher: its `href` is the Iceberg endpoint to attach,
   with a `description` of what it holds and a `bbox` (WGS84) of its extent. Choose the
   catalog(s) whose description fits the question — and, for a located question, **pre-filter
   by `bbox`**: skip catalogs whose extent doesn't cover your point (a Helsinki site keeps the
   Finnish catalogs + global Overture, drops nothing relevant). `bbox: null` = non-geospatial. (Adding a publisher = a new `child` link there; the skill doesn't change.
   [`catalogs.md`](catalogs.md) is a human-readable mirror of the same registry.) Today:
   9 publishers, 18 datasets; more over time.
2. **Catalog → browse its datasets.** Attach the catalog (it gives the endpoint) and read
   its **STAC index** `catalog.datasets`. Each row is a dataset with rich metadata in
   `properties` and `assets`. Filter to what's relevant — don't assume names.
3. **Dataset → understand it.** Inspect a candidate dataset's metadata: `properties.materialized`
   (is the data published?), `assets.data.href` (where it is — an Iceberg table ref `v2.<name>`
   / `tab.<name>`, a raquet raster URL, or a remote GeoParquet URL), `properties.crs`, the
   GeoParquet `geo` metadata (geometry column + encoding), and the **OSI `properties.semantics`**
   block (what it describes / answers / its unit).
4. **Query — write it yourself.** From the schema + geo-metadata + semantics, **compose your own
   SQL** (e.g. for a vector Iceberg table: `ST_Distance` on `ST_GeomFromWKB(geom_wkb)`, transformed to
   a metric CRS like EPSG:3067 for Finland). Only **genuinely tricky access patterns carry a
   `properties.query_hint`** — **raquet rasters** (`read_raquet` + `ST_RasterValue` +
   `ST_GeomFromQuadbin` block sampling) and the **remote Overture GeoParquet** (S3 secret + hive
   partitioning + bbox prune), which you won't guess; use it there. Join across
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
needs the CLI; `-unsigned` to load it). Vectors are Iceberg tables (`v2.*`, WKB `geom_wkb` +
bbox); non-spatial data is `tab.*`; rasters are raquet files (`read_raquet('<href>')`); a few
sources are remote GeoParquet read in place. For metric distance, transform to the
CRS the dataset's metadata specifies (Finnish data → EPSG:3067).

## Composing answers across catalogs (this is the powerful part)
Most real questions need **2–3 publishers joined by location or arithmetic**, not one lookup.
Discover the relevant datasets, compose a query for each (use a `query_hint` only where one is shipped), then combine. Patterns seen:

- **"Viability of this site?"** → the full screen: grid/water/protected (NLS) + flood/Natura/groundwater (SYKE) + land cover (Copernicus) + zoning (HSY) + climate (Location Finland).
- **"What radius covers as many people as the data centre consumes?"** → DC load ÷ per-capita power → people-equivalent, then grow a radius over the **population grid** (Statistics Finland) until the cumulative population matches. *(per-capita ≈ Fingrid national load ÷ Finland population ≈ 1.55 kW/person; a ~100 MW DC ≈ 64,000 people ≈ everyone within ~4 km.)*
- **"How much will it pay for electricity — and in Berlin?"** → DC load × hours × **Eurostat** industrial price, per country (FI vs DE). *(100 MW ≈ 876 GWh/yr → ≈ €74M in Finland vs €208M in Germany.)*
- **"Is there housing for 3,000 workers?"** → workers → dwellings/floor-area need, vs **HSY** residential building-rights reserve (`laskvar_ak`) and/or **Paavo** dwellings nearby.
- **"Enough supermarkets?"** → **Overture** `places` filtered to `category IN ('supermarket','grocery_store')` within a radius, vs **population** (pop grid / Paavo) → shops per 1,000 people.

State your assumptions (DC size, per-capita figures) out loud, and keep every number traceable to the dataset + query that produced it.

## Honest framing — do NOT regress
- **Grid:** "favorable proximity, subject to capacity and permitting" — never "cheap/excellent grid".
- **Flood:** prefer SYKE's flood-hazard zones (`tulvavaarakartta`) when materialized — report distance to / inside a mapped zone (with return period). Only if no flood layer is available, infer cautiously from the DEM ("no obvious topographic flood concern from the DEM; authoritative flood-hazard maps would sharpen this"). Never "low flood risk" from elevation alone.
- **Water:** distance to a lake is cooling/context, not a permission.
- **Sovereignty:** "European / sovereign infrastructure" — not "no data leaves the country".
- Always: this is an **initial spatial screening, not a permit decision.** Every number keeps its source + query.

## Web demo
`webapp/` is a **precomputed snapshot** of one run of this flow (a CARTO-branded site that
replays the Espoo scenario from committed `data/*.json`). In a live session, do the
discovery yourself — that's the point.
