# Catalog registry — the SDIs an agent can reach

> This is the **reference document** the `sdi` skill reads first: *what spatial data
> infrastructures exist, and what you'll find in each.* Today it lists the three
> sources in the **Portolan** Helsinki demo — three **independent Iceberg endpoints**,
> one per publisher, exactly as you'd get if each agency converted its own holdings to
> cloud-native and exposed them. Add a source = add an endpoint + an entry here; the
> skill needs no code change. That's the direction: a growing federation of sovereign,
> self-describing catalogs an agent discovers and queries the same way.

## How to read this
Each source is an **Apache Iceberg** catalog on object storage, attached anonymously
with DuckDB. Each publishes its own **STAC index** (`stac-geoparquet`, the table
`catalog.datasets`) listing **only its own datasets** — you query it to discover what's
there. Data is **cloud-native**: GeoParquet for vectors, raquet (raster-in-Parquet) for
rasters, read in place — no server, no download.

- **Hosting:** UpCloud object storage, **European / sovereign infrastructure** (Finland) · anonymous read
- **STAC index columns:** `id, collection, geometry, bbox, datetime, properties, assets, stac_version, type`
- **`properties.materialized = true`** → cloud-native data is published now; `assets.data.href` tells you where:
  - a **vector table** ref like `v2.power_lines` → query `<alias>.v2.power_lines` in that catalog (also a native-geometry `v3.*` for Snowflake/CARTO),
  - or a **raster URL** (`…/data/raster/*.parquet`) → `read_raquet('<url>')`.
  - Otherwise the dataset is **catalogued, convert-on-demand** (assets empty).

## Attach the federation
```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial; INSTALL raquet FROM community; LOAD raquet;
SET geometry_always_xy = true;

ATTACH 'nls'  (TYPE iceberg, ENDPOINT 'https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/national-land-survey',          AUTHORIZATION_TYPE 'none'); -- 🇫🇮
ATTACH 'syke' (TYPE iceberg, ENDPOINT 'https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/finnish-environment-institute', AUTHORIZATION_TYPE 'none'); -- 🇫🇮
ATTACH 'cop'  (TYPE iceberg, ENDPOINT 'https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/copernicus',                    AUTHORIZATION_TYPE 'none'); -- 🇪🇺
SHOW ALL TABLES;
```

---

## 🇫🇮 National Land Survey of Finland (Maanmittauslaitos) — `nls`
Topographic + elevation base data — the physical-infrastructure backbone. **129 datasets** (`nls-topographic`, `nls-elevation`).

| Dataset (`id`) | Status | Table / file | What it is |
|---|---|---|---|
| `sahkolinja` | ✅ materialized | `nls.v2.power_lines` (+ `v3`) | Electricity transmission lines |
| `jarvi` | ✅ materialized | `nls.v2.water` (+ `v3`) | Lakes & water bodies |
| `luonnonsuojelualue` | ✅ materialized | `nls.v2.protected` (+ `v3`) | Protected nature areas |
| `korkeusmalli_2m` | ✅ materialized | `…/data/raster/dem_2m.parquet` (raquet) | 2 m laser DEM (elevation/slope) |
| ~125 more | 🗂 convert-on-demand | — | buildings, roads, wetlands, shoreline, contours, etc. |

- **CRS:** geometries are WGS84 (EPSG:4326); for metric distances transform to **EPSG:3067** (ETRS-TM35FIN).
- **Vector schema:** `v2` = `geom_wkb` (WKB) + flat bbox (`fp_xmin…`), DuckDB-portable; `v3` = native `geom` (geoarrow).

## 🇪🇺 Copernicus (European Union) — `cop`
Earth observation — what's actually on the ground, recently. **1 dataset** (`copernicus-sentinel2`).

| Dataset (`id`) | Status | File | What it is |
|---|---|---|---|
| `ndvi` | ✅ materialized | `…/data/raster/ndvi.parquet` (raquet, 2 bands: Red, NIR) | Sentinel-2 NDVI — vegetation / land cover |

- **NDVI** = `(NIR − Red) / (NIR + Red)` → high ≈ forest/field, low ≈ built/bare.

## 🇫🇮 Finnish Environment Institute (SYKE) — `syke`
Environmental / regulatory layers — the authoritative environmental picture. **3 datasets** (`syke`).

| Dataset (`id`) | Status | What it is |
|---|---|---|
| `natura2000` | 🗂 convert-on-demand | Natura 2000 protected areas |
| `tulvavaarakartta` | 🗂 convert-on-demand | Flood-hazard maps |
| `corine-land-cover` | 🗂 convert-on-demand | CORINE land cover 2018 |

- **Not yet materialized** in this demo — use them to *sharpen* a finding (authoritative
  flood risk beats inferring it from the DEM); narrate that they exist and are one
  conversion away.

---

## Note: a combined catalog also exists
The endpoint `…/catalog` (no publisher suffix) is a **combined** catalog that federates
all three publishers in one place (133 datasets; vectors as `sdi.v2.*`). The precomputed
web demo uses it. For the **live agent demo, prefer the three publisher endpoints** above
— attaching them separately is the whole point: it shows a real federation of
independent, sovereign sources.

## Adding a source (the direction)
A new SDI joins by: standing up its own Iceberg endpoint (or a collection), publishing a
STAC index so its datasets are discoverable, and adding an entry here saying *what you'll
find*. No skill code changes — the agent reads this file, attaches the catalog, searches
the index, and queries. That's the Portolan idea: SDIs as open, self-describing,
agent-usable infrastructure.
