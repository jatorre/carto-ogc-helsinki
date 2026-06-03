# Portolan — live demo script

A runbook for demonstrating the whole experiment **live in a fresh Claude Code session**.
Read the **SAY** lines to the audience; type the **PROMPT** blocks verbatim into Claude Code.

> The live demo *is* the explanation — it shows discovery → query → fix-by-PR → report-intent,
> all against the real sovereign federation. Deck appendix slides 14–16 mirror it as a backup.

---

## Pre-flight (2 minutes, before the talk)

1. Open a **new Claude Code session in the `helsinki_demo` repo** (so the `/sdi` skill loads).
2. Confirm tools: `duckdb`, `gh` (authenticated as the catalog owner), `python3`. Network on.
3. Confirm the federation is live:
   ```bash
   curl -s https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog/stac.json | head -c 200
   ```
4. Optional warm-up (pre-installs DuckDB extensions so the first live query is fast):
   ```bash
   duckdb -unsigned -c "INSTALL iceberg;INSTALL httpfs;INSTALL spatial;"
   ```
5. **Backup plan:** if live discovery is slow/flaky, open the precomputed `webapp/app.html` — it
   replays a scripted run from committed data, no network needed.

---

## Act 1 — One URL in, a sourced answer out  *(discover + query)*

**SAY:** "I'll hand an AI agent a single sovereign URL — no portal, no logins — and ask a real
question."

**PROMPT:**
```
I'm evaluating a site for a data centre near Espoo, Finland (about 60.20°N, 24.65°E).
Using the spatial data infrastructure, what does the data say about this location —
people nearby, land use, environmental constraints, and proximity to power?
Cite the publisher and the exact query behind every figure.
```

**WHAT HAPPENS:** the `/sdi` skill reads the catalog-of-catalogs, **bbox-prefilters** to the
Finnish catalogs, `ATTACH`es several publishers (NLS, SYKE, Statistics Finland, Copernicus,
Location Finland), composes its own SQL, and returns an answer where every number carries its
source + query (often as an HTML artifact).

**SAY:** "It started from one URL, discovered nine independent publishers, and queried them *in
place* with DuckDB. No data was copied, no server was involved, and every figure is reproducible."

---

## Act 2 — Compose across the federation  *(the powerful part)*

**SAY:** "Now something no single dataset can answer — a question that spans publishers and tiers."

**PROMPT:**
```
Assume a 100 MW data centre. What would it pay for electricity here per year, and how does
that compare to Germany? And how many people live within 2 km of the site?
```

**WHAT HAPPENS:** joins **Eurostat** industrial electricity price (Finland vs Germany,
non-geospatial) × load × hours, and the **Statistics Finland** 1 km population grid within 2 km —
arithmetic across an EU statistics table and a Finnish spatial grid.

**SAY:** "It's federating across tiers — a regional grid, a national statistic, an EU table —
and doing the math, every step sourced. That's the catalog-of-catalogs paying off."

---

## Act 3 — It fixes the data by pull request  *(data-as-code)*

**SAY:** "The catalogs are git repos. Watch the agent improve one — like fixing code."

**PROMPT:**
```
While exploring, did any catalog's metadata look thin, wrong, or inconsistent? Pick the
clearest example, make a concrete improvement to that catalog's source, and open a pull
request on its GitHub repo. Show me the PR and whether its validation passed.
```

**WHAT HAPPENS:** the agent edits the source repo (`datasets/<id>.json` or metadata), regenerates
STAC, runs `validate.py`, opens a **real PR**; the repo's **validate workflow runs on the PR**.

**SAY:** "A fix to *data* went through the same review and CI as a code change — and the catalog's
own validation gate guarded it. That's the contribution loop closing."

> Tip: to guarantee a clean target, you can name one, e.g. *"improve the description of the
> Overture `places` dataset"*. Creates a real PR — close it afterward if you don't want it
> lingering (see Cleanup).

---

## Act 4 — It reports *why*  *(opt-in intent telemetry)*

**SAY:** "Finally — a signal a data portal never had: not which bytes were fetched, but *why*."

**PROMPT:**
```
File an opt-in usage report on the catalog you relied on most, describing what I was trying
to do. Paraphrase it, include nothing confidential, and show me the issue.
```

**WHAT HAPPENS:** the agent files a `usage-report` issue (dataset, paraphrased intent, use-case,
access method) on that publisher's repo.

**SAY:** "Opt-in, paraphrased, public — the publisher learns the real use case and can prioritize
its data accordingly."

---

## Act 5 — Same files, four ways  *(optional, visual)*

**SAY:** "Everything you saw is static files on EU object storage, read four ways."

- **Human:** open the explorer in a browser —
  `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/repo/portolan-statfi-catalog/index.html`
- **Engine (live one-liner):**
  ```bash
  duckdb -unsigned -json -c "INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;
  ATTACH 'c'(TYPE iceberg,ENDPOINT 'https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/repo/portolan-statfi-catalog',AUTHORIZATION_TYPE 'none');
  SELECT count(*) FROM c.v2.statfi_popgrid;"
  ```
- **Standards:** show `…/catalog.json` (STAC) and `…/records/catalog.json` (OGC API - Records).

**SAY:** "ATTACH, iceberg_scan, STAC/Records, direct download — and a human web page. One open
artifact, no server, sovereign by where it's hosted."

---

## The four take-aways (land these)

- **AI-Ready** — the agent did all of this from one URL, including contributing back.
- **Interoperable** — the same files read by DuckDB *and* Snowflake; STAC + OGC API - Records.
- **Sovereign** — EU object storage, open formats, open engine; host anywhere.
- **Cost-effective** — static files scale to any traffic with no servers; cost barely moves.

---

## Cleanup (after the demo, optional)

```bash
# close the demo PR / issue if you don't want them lingering (swap in the real numbers):
gh pr close <N>    --repo jatorre/portolan-<x>-catalog --delete-branch
gh issue close <N> --repo jatorre/portolan-<x>-catalog
```

## If something fails live

- Discovery slow → fall back to `webapp/app.html` (precomputed replay).
- A publisher endpoint hiccups → the agent will route around it; or name a specific catalog
  (e.g. Statistics Finland) and ask the question against just that one.
- PR/issue step blocked (auth) → skip Act 3/4 and show the *already-open* PR
  `jatorre/portolan-overture-catalog#1` and issue `jatorre/portolan-statfi-catalog#1` as evidence.
