# Catalog registry — the spatial data infrastructures you can reach

> The **directory** the `sdi` skill reads first: *which catalogs exist and what each holds*,
> so you can pick one. You don't get dataset-level detail here — once you choose a catalog,
> **attach it and read its own STAC index** (`catalog.datasets`); each dataset carries its
> own metadata, including a runnable `example_query`. That's the progressive discovery:
> registry → catalog → dataset → query. Adding a source = a new endpoint + an entry here;
> the skill doesn't change. Today there are three (the Portolan Helsinki demo); there will
> be many.

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
Earth observation — what's actually on the ground, recently (Sentinel-2). Vegetation /
land-cover context (NDVI). **1 dataset, materialized.**
- **Attach as** `cop` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/copernicus`

### 🇫🇮 Finnish Environment Institute (SYKE)
Authoritative environmental / regulatory layers. **4 datasets — 3 materialized**:
flood-hazard zones (`tulvavaarakartta`, inundation extent by return period), Natura 2000
(`natura2000`, SAC+SPA), and classified groundwater areas (`pohjavesialue`, VHS2022).
CORINE land cover is still convert-on-demand. (Clipped to the Helsinki-region AOI.)
- **Attach as** `syke` · **endpoint** `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/finnish-environment-institute`

> A **combined** catalog also exists at `…/catalog` (all three publishers, 134 datasets,
> vectors as `sdi.v2.*`) — the precomputed web demo uses it. For the **live agent demo,
> prefer the three publisher endpoints above** — attaching them separately is the point:
> a real federation of independent, sovereign sources.

## Adding a source (the direction)
Stand up an Iceberg endpoint (or a collection), publish a STAC index so the datasets are
discoverable and each carries its own `example_query`, and add an entry here. No skill code
changes — the agent reads this file, attaches the catalog, searches the index, and queries.
That's the Portolan idea: SDIs as open, self-describing, agent-usable infrastructure.
