# Catalog registry — the spatial data infrastructures you can reach

> The **directory** the `sdi` skill reads first: *which catalogs exist and what each holds*,
> so you can pick one. You don't get dataset-level detail here — once you choose a catalog,
> **attach it and read its own STAC index** (`catalog.datasets`); each dataset carries its
> own metadata, including a runnable `example_query`. That's the progressive discovery:
> registry → catalog → dataset → query. Adding a source = a new endpoint + an entry here;
> the skill doesn't change. Today there are **five** (the Portolan Helsinki demo) spanning
> EU, national, regional and municipal tiers — and one is **non-geospatial**, to show the
> SDI federates open data, not just maps. There will be many more.

## How catalogs work (applies to all of them)
Each is an **Apache Iceberg** catalog on object storage (UpCloud, European / sovereign
infrastructure 🇫🇮), attached anonymously with DuckDB. Each publishes its own **STAC index**
table `catalog.datasets` (`stac-geoparquet`) listing **only its own datasets**. Per dataset,
`properties.materialized = true` means cloud-native data is published; `assets.data.href`
points to it (an Iceberg table like `v2.power_lines`, or a raster URL for `read_raquet`),
and `properties.example_query` is a concrete query you can run (substitute the site
`:lon`/`:lat` and your attach alias for `<catalog>`). Otherwise the dataset is catalogued,
**convert-on-demand**.

```sql
ATTACH '<alias>' (TYPE iceberg, ENDPOINT '<endpoint>', AUTHORIZATION_TYPE 'none');
SELECT id, collection, json_extract_string(properties,'$.materialized') AS materialized,
       json_extract_string(properties,'$.example_query') AS how_to_query,
       json_extract_string(assets,'$.data.href') AS data
FROM <alias>.catalog.datasets
WHERE properties ILIKE '%<your topic>%';   -- e.g. '%data-center%'
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
Vantaa / Kauniainen. Answers **"what does the plan permit at this exact block?"** (e.g. the
Espoo site is industrial-zoned with ~70,000 m² of unused building rights). Vector.
- **Attach as** `hsy` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/helsinki-region-hsy`

### ⚡ Fingrid (national TSO) — NON-geospatial
The Finnish transmission system operator. **1 dataset, materialized**: `electricity_consumption`
— national grid load, 15-min MW time-series (30-day snapshot). **No geometry** — it's a plain
table, here to show the SDI federates open *data*, not just maps. Grid-load / capacity context
for siting a large consumer (a hyperscale data centre is ~100–300 MW; Finland runs ~8–10 GW).
`example_query` is a non-spatial aggregate (avg / peak / min MW).
- **Attach as** `fingrid` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/fingrid`

> A **combined** catalog also exists at `…/catalog` (all publishers, 137 datasets,
> vectors as `sdi.v2.*`) — the precomputed web demo uses it. For the **live agent demo,
> prefer the three publisher endpoints above** — attaching them separately is the point:
> a real federation of independent, sovereign sources.

## Adding a source (the direction)
Stand up an Iceberg endpoint (or a collection), publish a STAC index so the datasets are
discoverable and each carries its own `example_query`, and add an entry here. No skill code
changes — the agent reads this file, attaches the catalog, searches the index, and queries.
That's the Portolan idea: SDIs as open, self-describing, agent-usable infrastructure.
