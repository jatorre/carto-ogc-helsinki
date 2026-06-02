# The Next Spatial Data Infrastructure
### Geospatial Sovereignty in the Age of AI — CARTO @ OGC Connect Helsinki

A 10-minute keynote **and** a live, federated, cloud-native demo, kept in one repo
so it doubles as a **leave-behind** for the audience.

🔗 **Slides:** https://jatorre.github.io/carto-ogc-helsinki/
🔗 **Live demo:** https://jatorre.github.io/carto-ogc-helsinki/webapp/
📄 **How the live agent demo works:** [`docs/how-it-works.md`](docs/how-it-works.md) — the skill-driven, progressively-discovering SDI flow (part of **Portolan**).

> **Thesis:** SDIs were built to serve geospatial *experts*. In the AI era they must
> also serve *agents* — and agents answer questions directly for everyone. That makes
> SDIs **more** relevant, not less, but it changes the requirements. The next SDI must be
> **AI-ready · cheap & fast · interoperable · sovereign.**

## The demo
Ask a plain-language question about a place in Finland and an agent answers it from a
federation of three real publishers' catalogues — **National Land Survey of Finland
(NLS)**, **SYKE** (Finnish Environment Institute), and **Copernicus** — re-exposed as a
cloud-native, self-describing **Apache Iceberg + STAC** catalog on **UpCloud** object
storage (hosted in Finland), queried in place with **DuckDB**. Every figure carries a
publisher, a dataset, and a re-runnable query.

The walkthrough scenario: *"Microsoft is planning data centres near Helsinki — what does
the data say about a contested site?"* → grid proximity, terrain, water, land cover and
protected areas for Espoo / Hepokorpi.

> This is an **initial spatial screening, not a permitting decision.** Claims are framed
> conservatively on purpose — e.g. distance to a power line is *favorable proximity,
> subject to capacity and permitting* (not "cheap grid"); flatness from the DEM is *no
> obvious topographic flood concern* (SYKE flood maps would sharpen it).

## Run it locally
Everything is static — just serve the repo root:
```bash
python3 -m http.server 8765
# Slides:  http://localhost:8765/index.html      (press S = speaker notes, F = fullscreen)
# Demo:    http://localhost:8765/webapp/
```

## Layout
| Path | What |
|---|---|
| [`index.html`](index.html) | The 10-slide deck (reveal.js, vendored — offline-ready). Each slide is a 16:9 frame of the source Google Slides; the full speaker script is in the presenter notes. |
| [`slides/img/`](slides/img/) | The committed 16:9 slide images. |
| [`webapp/`](webapp/) | Precomputed demo site: `index.html` (federation index of 18 datasets across 9 publishers) + `app.html` (the scripted Q&A with map). Serves committed `webapp/data/*.json` — no backend. |
| [`.claude/skills/sdi/`](.claude/skills/sdi/) | The `sdi` Claude skill — `SKILL.md` (generic progressive-discovery loop) + `catalogs.md` (the catalog registry). The agent attaches the catalogs, reads each dataset's metadata (OSI semantics + schema) and **composes its own query**, runs it with the DuckDB CLI, and builds an HTML artifact — live. (A `query_hint` ships only for genuinely tricky access — raquet rasters and the remote Overture GeoParquet.) |
| [`demo/build_iceberg_catalog.py`](demo/build_iceberg_catalog.py) | Builds the static GeoIceberg/STAC catalog published to UpCloud. |
| [`demo/ingest_official.py`](demo/ingest_official.py) | Pulls official NLS/SYKE/Copernicus source data (needs a local API key, not committed). |

## The sovereign stack on stage
Storage **UpCloud** (Finland) · data **GeoParquet · Apache Iceberg · STAC · COG** ·
compute **DuckDB** · AI **Mistral** (or self-hosted) · GIS platform **CARTO**.
Everything runs on European infrastructure.

## Accuracy notes
- **SEAL** = the EU Cloud Sovereignty Framework's *Sovereignty Effectiveness Assurance
  Levels*. **SEAL-4** is a high bar (EU control end to end). Frame CARTO's position as
  *working toward* SEAL-4-level sovereignty, not an achieved certification.
- Sovereignty here means **European / sovereign infrastructure**, not a claim that no
  byte ever leaves the country (Copernicus and the AI model are EU, not in-country).
