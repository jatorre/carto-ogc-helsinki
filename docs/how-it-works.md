# How the live demo works — an agent querying a sovereign SDI

> **Part of [Portolan](#portolan), the cloud-native, agent-ready Spatial Data
> Infrastructure project we're working on.** This demo is one concrete slice of
> that idea: a Claude **skill** that lets an agent *use* an SDI the way a developer
> uses an API — discover what's there, query it, join it, and answer in plain
> language, with every number traceable to a publisher and a query.

This is not a web app with a backend. It's an **agent + a skill + open files on
object storage.** There is no GIS server, no portal, no data copies. You record it
by asking Claude one question and watching it work.

---

## 0. The setup — a question and a skill

The user has the **`sdi` skill** loaded in Claude (think of it as
*"ask the geospatial data infrastructure"*). They ask something ordinary:

> *"Microsoft is planning data centres near Helsinki. Take one contested site and
> tell me what the data actually says — infrastructure and environment. Show me the
> datasets, the queries, the map, and the uncertainties."*

No dataset names. No portal. No CRS. Just intent. The skill's description tells
Claude this is its job (siting / feasibility / "what does the data say about
building here"), so the agent picks it up and starts.

The whole point is **de-intermediation**: the user doesn't need to be a GIS expert,
and the agent doesn't need the answer baked in. It discovers everything live.

---

## 1. What the skill actually is

A skill is a folder with a `SKILL.md` (instructions + the catalog endpoint) and a
small engine script. It carries **no data** — only the knowledge of *how to reach a
sovereign SDI and reason about it*:

```
.claude/skills/sdi/
├── SKILL.md          # when to trigger · the catalog endpoint · the live flow
└── sdi_report.py     # the engine: attach → discover → query → render
```

The catalog it points at is a **published artifact** living on European object
storage (UpCloud, in Finland). The agent *uses* it; it never builds or copies it.

---

## 2. Progressive discovery — the agent finds the catalogs and searches them

The agent doesn't assume what exists. It **attaches the catalogs and asks them**,
narrating each step out loud (this is the part that reads well on camera).

It connects to a **federation of three publishers' catalogs**, each an
**Apache Iceberg** catalog on object storage — attached anonymously, no credentials:

```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial; INSTALL raquet FROM community; LOAD raquet;
SET geometry_always_xy = true;

ATTACH 'nls'  (TYPE iceberg, ENDPOINT '…/national-land-survey',         AUTHORIZATION_TYPE 'none'); -- 🇫🇮
ATTACH 'syke' (TYPE iceberg, ENDPOINT '…/finnish-environment-institute', AUTHORIZATION_TYPE 'none'); -- 🇫🇮
ATTACH 'cop'  (TYPE iceberg, ENDPOINT '…/copernicus',                    AUTHORIZATION_TYPE 'none'); -- 🇪🇺
SHOW ALL TABLES;
```

Each catalog exposes a machine-readable **STAC index** as a table
(`catalog.datasets`, stored as **stac-geoparquet** — STAC items as columns:
`id, collection, geometry, bbox, datetime, properties, assets`). The agent searches
that index to find what's *relevant* to the question — it doesn't need to know the
dataset names in advance:

```sql
SELECT id, collection,
       json_extract_string(properties,'$.materialized') AS materialized
FROM nls.catalog.datasets
WHERE properties ILIKE '%data-center%'        -- "what bears on siting a data centre?"
ORDER BY materialized DESC;
```

> *"There are 130-odd datasets catalogued across three publishers. These bear on a
> data centre — power lines, elevation, water, protected areas, land cover. And
> these are already cloud-native, so I can query them directly right now; the rest
> are catalogued and convertible on demand."*

This is the **progressive** part: the agent reveals the SDI to itself and to the
audience, layer by layer — *which catalogs, which datasets, which are queryable* —
before it commits to an analysis.

---

## 3. Query and join the cloud-native data with DuckDB

The relevant datasets are **cloud-native files** referenced from the STAC index:

- **Vectors** as **GeoParquet** — `nls.v2.{power_lines, water, protected}` (WKB
  geometry + a flat bbox for pruning).
- **Rasters** as **raquet** (cloud-native raster-in-Parquet) — the 2 m laser DEM and
  Sentinel-2 NDVI, read in place with range requests.

DuckDB reads them **directly from object storage** — no server, no download, no tile
service. Every metric is one query. Distance to the nearest transmission line, in a
projected CRS:

```sql
SELECT round(min(ST_Distance(
    ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),
    ST_Transform(ST_Point(:lon,:lat),  'EPSG:4326','EPSG:3067')))) AS metres
FROM nls.v2.power_lines;
```

Terrain and land cover come from the rasters by sampling a small grid around the site
out of the **raquet** blocks (`ST_RasterValue` over the DEM and NDVI). And because
it's all just tables, the agent can **join across publishers** — e.g. the site against
NLS protected areas *and* a SYKE layer — in a single SQL statement. Geometry is just a
column; spatial joins are ordinary joins.

What the agent assembles for the site:

| Question | Dataset | Publisher | How |
|---|---|---|---|
| Grid tie-in? | `power_lines` | NLS 🇫🇮 | nearest transmission line (m) |
| Buildable terrain / flood? | `dem_2m` (raquet) | NLS 🇫🇮 | elevation + slope from the 2 m DEM |
| Cooling / water? | `water` | NLS 🇫🇮 | distance to nearest lake (m) |
| Green-land impact? | `ndvi` (raquet) | Copernicus 🇪🇺 | Sentinel-2 NDVI over the site |
| Protected nature? | `protected` | NLS 🇫🇮 | distance to nearest protected area (m) |

---

## 4. Synthesize → an HTML artifact with a map

With the numbers in hand, the agent writes a **self-contained HTML artifact**: the
finding in plain language, a metrics panel where **every figure carries its publisher,
dataset and the query that produced it**, and a MapLibre map with the queried features
drawn on it.

For the Espoo / Hepokorpi site the data says: favorable infrastructure and terrain —
but the site is forest, and building it clears green land at a residential edge. The
agent surfaces that tension rather than hiding it. The framing is deliberately honest:

- **Grid:** ~64 m to a transmission line is *favorable proximity, subject to capacity
  and permitting* — proximity is not capacity.
- **Terrain/flood:** flat and inland → *no obvious topographic flood concern from the
  DEM*; SYKE's flood-hazard maps (catalogued, convert-on-demand) would sharpen it.
- **Water:** ~931 m to a lake is cooling/context, not a permission.
- **Land cover:** NDVI ~0.66 → forest. The local objection is visible in the data.
- It says, on the artifact, that this is an **initial spatial screening, not a permit
  decision.**

> **Provenance is the deliverable.** In the AI era a number without a source is noise.
> Here every number has a publisher and a re-runnable query — that's what makes an
> agent's answer trustworthy, and it's why standards (STAC, Iceberg, OGC) matter *more*
> with agents, not less.

---

## Why this architecture

Four properties fall out of "open files an agent can query," and they're the four
requirements for the next SDI:

- **AI-ready** — the catalog is a *machine-readable contract*. The agent discovers and
  queries it; no human portal in the loop.
- **Cheap & fast** — object storage + cloud-native files + compute pushed to the
  engine. No GIS server per dataset, no tile cache to operate.
- **Interoperable** — geometry and geography are just data types in Parquet and
  Iceberg. The same data is reachable from DuckDB, BigQuery, Snowflake, Databricks,
  notebooks — and agents. OGC APIs still provide discovery and governance.
- **Sovereign** — storage **UpCloud** (Finland), compute **DuckDB** (open engine), AI
  **Mistral** or self-hosted, platform **CARTO**. It runs on **European, sovereign
  infrastructure** end to end.

---

## Portolan

**Portolan** is the cloud-native, agent-ready SDI project this demo is part of: the
idea that a Spatial Data Infrastructure should be open files on object storage,
indexed by STAC, queryable by any engine, governed by open standards — and therefore
usable directly by agents. This Helsinki demo is a working proof of one publisher
federation (NLS · SYKE · Copernicus) and one agent skill on top of it.

The next SDI is not a portal for experts. It's trusted infrastructure for agents that
serve everyone — the *Google Maps moment* for SDIs.

---

## Recording notes (for the screencast)

1. Show the question typed into Claude with the `sdi` skill available.
2. Let it **narrate discovery** — attaching the three catalogs, `SHOW ALL TABLES`,
   searching the STAC index, calling out which datasets are cloud-native.
3. Let it **run the queries** — each metric is a visible SQL statement against
   GeoParquet/raquet on object storage.
4. **Open the HTML artifact** — read the finding, point at the map, and stress that
   every figure links to a publisher + a query.
5. Land the message: *no portal, no server, no data left an open standard — and it all
   ran on European infrastructure.*

*Implementation lives in [`.claude/skills/sdi/`](../.claude/skills/sdi/); the precomputed
web version is in [`webapp/`](../webapp/).*
