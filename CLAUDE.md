# CLAUDE.md — working in this repo

CARTO keynote + live demo for **OGC Connect Helsinki**: "The Next Spatial Data
Infrastructure — Geospatial Sovereignty in the Age of AI" (speaker: Javier de la Torre).
Everything is **static** and served from the repo root.

## Run / preview
```bash
python3 -m http.server 8765
# slides → /index.html (S = notes, F = fullscreen) · demo → /webapp/
```
Published via **GitHub Pages** at `https://jatorre.github.io/carto-ogc-helsinki/`.

## The slides (`index.html`)
- A reveal.js deck where **each slide is a full-bleed 16:9 PNG** (`slides/img/01.png`…`10.png`),
  not HTML — they mirror the speaker's Google Slides exactly. The **full speaker script is
  in each slide's `<aside class="notes">`** (presenter view).
- Slides 5 & 6 (the demo) have a teal **"Open the live demo"** button linking to `webapp/`.
- **To refresh slides:** drop new full-screen screenshots into `slides_screenshots/`
  (gitignored, kept local) and re-run a PIL auto-crop that trims the white letterbox to
  exactly 16:9 and writes `slides/img/NN.png` ordered by capture timestamp. Don't hand-edit
  the PNGs.

## The demo (`webapp/`)
- Static site, **precomputed snapshot** of one skill run: `index.html` (federation index of
  18 datasets across 9 publishers) + `app.html` (scripted Q&A with a MapLibre map). Both read
  committed `webapp/data/{catalog,scenario}.json` — no backend, no build step.
- The catalog lives on a **public, anonymous** UpCloud bucket
  (`https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog`, `AUTHORIZATION_TYPE 'none'`),
  so the demo needs no credentials. To rebuild/republish the catalog itself, use
  `demo/build_iceberg_catalog.py` (testbed venv + `mc`; see catalog-architecture memory).

## The `sdi` skill (`.claude/skills/sdi/`)
The live demo. `SKILL.md` is a **generic progressive-discovery loop** (registry → catalog →
dataset → query) and `catalogs.md` is the **catalog registry**. The agent attaches the
Iceberg/STAC catalogs, discovers datasets, reads each dataset's own metadata (OSI semantics +
schema) and **composes its own query**, runs it with the DuckDB CLI, and builds an HTML
site-assessment artifact — all live. A `query_hint` ships only for genuinely tricky access
(raquet rasters, the remote Overture GeoParquet); everything else the agent works out itself.
Specifics (endpoints, schemas) live in the catalog metadata, not the skill, so adding a
publisher is just a new endpoint + a `catalogs.md` line.

**Catalog representation rules (don't regress):** the index lists **only what's accessible**
(every dataset has published data — no "catalogued / convert-on-demand" entries). If we
transform a source we go **all the way to an Iceberg table** (`v2.*` vectors + `v3.*` native;
`tab.*` for non-spatial) — never a half-way GeoParquet file. A source already published as
cloud-native GeoParquet stays a **remote GeoParquet** (Overture, on its own S3). Rasters are
raquet files. Converter build inputs live in the bucket's `sources/` prefix, **not** under
`catalog/`.

## Claim framing — IMPORTANT, do not regress
The deck and demo deliberately avoid overclaiming. When editing any user-facing copy:
- **Sovereignty:** say "European / sovereign infrastructure", not "no data leaves the
  country" (Copernicus + the AI model are EU, not in-country).
- **Grid:** distance to a power line is "favorable proximity, subject to capacity and
  permitting" — never "cheap/excellent grid connection".
- **Flood:** prefer **SYKE's flood-hazard zones** (now a queryable Iceberg table) — report
  distance to / inside a mapped zone with its return period. Use the DEM only as a fallback
  ("no obvious topographic flood concern from the DEM") — never infer "low flood risk" from
  elevation alone.
- **Provenance is the point:** every number must keep its publisher + re-runnable query.
- Always frame the assessment as an **initial spatial screening, not a permitting decision.**

## Secrets
None are committed. `demo/ingest_official.py` reads an NLS API key from a local file; any
`*.env`, `*apikey*`, `*credentials*` are gitignored. Keep it that way.

## Conventions
- No build step, no framework — plain HTML/CSS/JS + vendored reveal.js in `vendor/`.
- Match surrounding style. Keep the deck offline-capable (vendored reveal; the webapp uses
  MapLibre from a CDN).
