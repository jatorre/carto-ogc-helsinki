# Portolan — positioning

The one job of this page: say what Portolan *is* in a way that's consistent across the deck,
the demo, the repos, and the talk. Pick the altitude that fits the moment.

## The word

**Convergence.** Portolan is the convergence layer — where the geospatial standards world
(STAC, OGC API - Records) meets the open analytics world (Apache Iceberg, GeoParquet,
DuckDB / Snowflake).

## The sentence (lead with this)

> **Portolan carries the OGC lineage — STAC and OGC API - Records for what data *means* and
> how it's *discovered* — onto the open analytics foundation the rest of the world already runs
> on — Apache Iceberg, GeoParquet, DuckDB, Snowflake — so geospatial keeps its standards yet
> stops being a silo: _described by OGC, queried by everyone._**

## The elevator version (≈20 seconds)

> The geospatial community spent decades building hard-won standards for meaning and
> interoperability — and then ran them on its own parallel, special-purpose stack. Portolan
> keeps that OGC heritage — STAC, OGC API - Records, GeoParquet — but sets it on the same open
> analytics infrastructure everyone else uses: Apache Iceberg tables on object storage, queried
> by DuckDB, Snowflake, BigQuery, Spark. We're not inventing a geospatial silo or abandoning the
> standards — we're **connecting OGC's past to the analytics present**, so spatial data is
> finally *just data*. That convergence is how the GIS silo breaks.

## Bumper lines (pick per audience)

- **"Described by OGC, queried by everyone."**
- "Spatial standards meet the analytics stack — and the silo falls."
- "We didn't leave geospatial. We connected it to everything else."
- "OGC's meaning, the analytics world's engines, one open file."

## The wrap-up (what we bring)

It's **bidirectional**: we bring **decades of OGC interoperability and semantics forward** into
the cloud-native analytics era, *and* we bring **the analytics era's scale, ubiquity, and
tooling into geospatial**. The silo doesn't break by throwing OGC away — it breaks because
spatial data now lives in the *same files and the same engines* as the rest of the data world,
while still carrying the meaning OGC standardized.

## Why this is accurate, not spin (citations for the floor)

- **STAC is itself an OGC Community Standard** (adopted 2025) — so using the STAC / OGC API -
  Records model is staying *inside* OGC, not diverging from it.
  STAC core → [OGC 25-004](https://docs.ogc.org/cs/25-004/25-004.html) ·
  STAC API → [OGC 25-005](https://docs.ogc.org/cs/25-005/25-005.html) ·
  [OGC announcement](https://www.ogc.org/announcement/ogc-announces-publication-of-the-spatiotemporal-asset-catalog-community-standards/)
- **STAC API is a strict superset of OGC API - Features - Part 1: Core** — a conformant STAC API
  is a valid OGC API - Features implementation.
  [radiantearth/stac-api-spec › ogcapi-features](https://github.com/radiantearth/stac-api-spec/tree/v1.0.0/ogcapi-features)
- **STAC explicitly aligns with OGC API - Records**; a STAC Item *is* a GeoJSON Feature, and the
  two communities co-developed for alignment.
  [STAC & OGC (Chris Holmes / Radiant Earth)](https://medium.com/radiant-earth-insights/spatiotemporal-asset-catalogs-and-the-open-geospatial-consortium-659538dce5c7) ·
  [OGC STAC standard page](https://www.ogc.org/standards/stac/)
- **GeoParquet** is on the OGC standards track (GeoParquet SWG).

> Honest nuance to keep in your back pocket: "superset of OGC API - Features" refers to the STAC
> *API*; STAC *core* is the GeoJSON catalog/item model. Both are OGC. Don't conflate the two if a
> pedant probes.

## What we deliberately do NOT adopt

- **CQL2 / a standardized query language.** Query is an *analytics* concern, not a spatial one —
  we stay with the engine's native SQL (DuckDB / Snowflake). Standardizing query language is OGC
  reaching past its lane.
- **Running an OGC API server.** We reuse the OGC *building-block data models* (record schema +
  JSON-LD context) and serve them as **static cloud-native files**. Interoperable *and* serverless.
