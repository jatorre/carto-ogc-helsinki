# Catalog registry — the spatial data infrastructures you can reach

> The **directory** the `sdi` skill reads first: *which catalogs exist and what each holds*,
> so you can pick one. You don't get dataset-level detail here — once you choose a catalog,
> **attach it and read its own STAC index** (`catalog.datasets`); each dataset carries its
> own metadata (OSI `semantics`, CRS, schema) from which **you compose your own query**.
> That's the progressive discovery: registry → catalog → dataset → query. Adding a source = a new endpoint + an entry here;
> the skill doesn't change. Today there are **nine** (the Portolan Helsinki demo) spanning
> every tier — **regional → national → EU → global** — and two are **non-geospatial**, to show
> the SDI federates open data, not just maps. Some need a free API key. There will be many more.

## How catalogs work (applies to all of them)
Each is an **Apache Iceberg** catalog on object storage (UpCloud, European / sovereign
infrastructure 🇫🇮), attached anonymously with DuckDB. Each publishes its own **STAC index**
table `catalog.datasets` (`stac-geoparquet`) listing **only its own datasets**. Per dataset,
`properties.materialized = true` means cloud-native data is published; `assets.data.href`
points to it (an Iceberg table like `v2.power_lines`, or a raster URL). Read the dataset's
`properties.semantics` (OSI), `crs` and schema and **write your own query**. Only tricky
cloud-native formats ship a `properties.query_hint` — notably **raquet rasters** (`read_raquet`
+ `ST_RasterValue` + `ST_GeomFromQuadbin`), which you wouldn't guess. Otherwise the dataset is
catalogued, **convert-on-demand**.

```sql
ATTACH '<alias>' (TYPE iceberg, ENDPOINT '<endpoint>', AUTHORIZATION_TYPE 'none');
SELECT id, collection, json_extract_string(properties,'$.materialized') AS materialized,
       json_extract_string(properties,'$.semantics') AS semantics,   -- what it means / answers / unit
       json_extract_string(assets,'$.data.href')     AS data
FROM <alias>.catalog.datasets
WHERE properties ILIKE '%<your topic>%';   -- match on generic keywords/semantics
```

## The catalogs

### 🇫🇮 National Land Survey of Finland (Maanmittauslaitos)
Authoritative topographic + elevation base data — the physical-infrastructure backbone:
power lines, water bodies, protected areas, buildings, roads, land-cover features, and the
2 m laser elevation model. **~129 datasets** (several materialized; the rest convert-on-demand).
- **Attach as** `nls` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/national-land-survey`

### 🇪🇺 Copernicus (European Union)
Earth observation + land monitoring — what's actually on the ground. **2 datasets, both
materialized**: Sentinel-2 `ndvi` (vegetation index) and `urbanatlas` (Copernicus Urban
Atlas 2018 land-use polygons for the Helsinki FUA — authoritative class per parcel:
Forests / urban fabric / industrial / …). NDVI gives the index; Urban Atlas gives the class.
- **Attach as** `cop` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/copernicus`

### 🇫🇮 Finnish Environment Institute (SYKE)
Authoritative environmental / regulatory layers. **4 datasets — 3 materialized**:
flood-hazard zones (`tulvavaarakartta`, inundation extent by return period), Natura 2000
(`natura2000`, SAC+SPA), and classified groundwater areas (`pohjavesialue`, VHS2022).
CORINE land cover is still convert-on-demand. (Clipped to the Helsinki-region AOI.)
- **Attach as** `syke` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/finnish-environment-institute`

### 🏙️ Helsinki Region — HSY (regional)
Regional planning. **1 dataset, materialized**: `seuturamava_kortteli` (SeutuRAMAVA) —
per detailed-plan-block **land-use category + building-rights reserve** (built vs unused
floor area by use class AK/AP/K/T/Y), aggregated from the municipal plans across Espoo /
Vantaa / Kauniainen. Captures **what the plan permits at each block** — land-use category and unused building-rights reserve. Vector.
- **Attach as** `hsy` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/helsinki-region-hsy`

### ⚡ Fingrid (national TSO) — NON-geospatial
The Finnish transmission system operator. **1 dataset, materialized**: `electricity_consumption`
— national grid load, 15-min MW time-series (30-day snapshot). **No geometry** — it's a plain
table, here to show the SDI federates open *data*, not just maps. National electricity load /
demand (Finland runs ~8–10 GW); the agent composes a simple aggregate over it (avg / peak / min MW).
- **Attach as** `fingrid` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/fingrid`

### 📊 Statistics Finland (Tilastokeskus)
Official statistics. **2 datasets, materialized**: `paavo_vaesto` (Paavo postal-area
demographics 2025 — population, income, employment, age) and `vaestoruutu_1km` (1 km
population grid 2025 — inhabitants + age groups per cell). Quantifies the **residential
context** at the site (postal-area population/income; people in the 1 km cell).
- **Attach as** `statfi` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/statistics-finland`

### 🧭 Location Finland (national Location Innovation Hub platform)
Finland's national location-data gateway (OGC API; needs a free API key at source — already
mirrored here, anonymous). **2 datasets, materialized**: `ykr_urban_structure` (YKR
settlement/urban-structure zones — urban→peripheral gradient) and `temperature` (mean annual
air temperature, °C — a **climate coverage** converted GeoTIFF→**raquet** and queried like the
DEM/NDVI rasters). The source API also offers 3D buildings
(CityJSON) and roads/rail — candidates for later.
- **Attach as** `lf` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/location-finland`

### 🇪🇺 Eurostat (EU statistical office) — NON-geospatial
**1 dataset, materialized**: `electricity_prices` — industrial electricity price (€/kWh, incl.
taxes), **Finland vs EU-27 and other countries**. No geometry — industrial electricity cost by
country (Finland ≈ €0.085/kWh vs EU-27 ≈ €0.19); the agent composes a simple comparison.
- **Attach as** `eurostat` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/eurostat`

### 🌍 Overture Maps (global)
**1 dataset, materialized**: `places` — points of interest from Overture's **planet-scale,
cloud-native GeoParquet** catalog (we federated a slice into the bucket; the source is queried
with DuckDB *exactly* like everything else here — the same pattern, at global scale). POI
density / activity near the site.
- **Attach as** `overture` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/overture-maps`

> A **combined** catalog also exists at `…/catalog` (all publishers, 143 datasets,
> vectors as `sdi.v2.*`) — the precomputed web demo uses it. For the **live agent demo,
> prefer the three publisher endpoints above** — attaching them separately is the point:
> a real federation of independent, sovereign sources.

## Adding a source (the direction)
Stand up an Iceberg endpoint (or a collection), publish a STAC index so the datasets are
discoverable (OSI semantics + schema; a query_hint only where the access pattern is tricky), and add an entry here. No skill code
changes — the agent reads this file, attaches the catalog, searches the index, and queries.
That's the Portolan idea: SDIs as open, self-describing, agent-usable infrastructure.
