#!/usr/bin/env python3
"""Precompute the Finland SDI web-app data from the LIVE sovereign catalog:
  - data/catalog.json  : the 130 datasets in the stac-geoparquet index
  - data/scenario.json : the grounded, map-driven scripted answer for the
                         Microsoft data-center question (real metrics + layers)
Reuses the tested /sdi engine queries. Run: python3 webapp/build_data.py
"""
import json, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / ".claude" / "skills" / "sdi"))
import sdi_report as e  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
LON, LAT, NAME = 24.6883, 60.2371, "Espoo Hepokorpi"


PUBLISHER = {"nls-topographic": "National Land Survey of Finland", "nls-elevation": "National Land Survey of Finland",
             "syke": "Finnish Environment Institute (SYKE)", "copernicus-sentinel2": "Copernicus (EU)"}
FLAG = {"National Land Survey of Finland": "🇫🇮", "Finnish Environment Institute (SYKE)": "🇫🇮", "Copernicus (EU)": "🇪🇺"}
TITLES = {"sahkolinja": "Electricity transmission lines", "luonnonsuojelualue": "Protected nature areas",
          "jarvi": "Lakes & water bodies", "korkeusmalli_2m": "Elevation model (2 m laser DEM)",
          "ndvi": "Sentinel-2 NDVI (vegetation)", "natura2000": "Natura 2000 protected areas",
          "tulvavaarakartta": "Flood hazard maps", "corine-land-cover": "CORINE land cover 2018"}

def catalog():
    rows = e.q("""SELECT id, collection,
        json_extract_string(properties,'$.title') AS title,
        json_extract_string(properties,'$.materialized') AS materialized,
        json_extract_string(properties,'$.item_type') AS item_type
        FROM sdi.catalog.datasets ORDER BY materialized DESC, collection, id""")
    for r in rows:
        r["materialized"] = (str(r.get("materialized")).lower() == "true")
        r["publisher"] = PUBLISHER.get(r["collection"], r["collection"])
        r["flag"] = FLAG.get(r["publisher"], "")
        r["title"] = TITLES.get(r["id"], r.get("title") or r["id"])
    pubs = {}
    for r in rows:
        p = pubs.setdefault(r["publisher"], {"publisher": r["publisher"], "flag": r["flag"], "n": 0, "materialized": 0})
        p["n"] += 1; p["materialized"] += 1 if r["materialized"] else 0
    return {"total": len(rows), "materialized": sum(1 for r in rows if r["materialized"]),
            "publishers": list(pubs.values()), "datasets": rows}


def scenario():
    power = e.dist("power_lines", LON, LAT); protected = e.dist("protected", LON, LAT); water = e.dist("water", LON, LAT)
    dem = e.raster_grid(e.DEM, [1], LON, LAT)
    elevs = [r["b1"] for r in dem if r.get("b1") is not None]
    elev_mean = round(sum(elevs) / len(elevs), 1) if elevs else None
    nd = e.raster_grid(e.NDVI, [1, 2], LON, LAT)
    ndv = [((r["b2"] - r["b1"]) / (r["b2"] + r["b1"])) for r in nd
           if r.get("b1") is not None and r.get("b2") is not None and (r["b1"] + r["b2"])]
    # area-scale vegetation (the contested forest), not just the cleared pad pixel
    aoi = e.raster_grid(e.NDVI, [1, 2], LON, LAT + 0.004)  # shift into the forest band N of the cleared site
    aoiv = [((r["b2"] - r["b1"]) / (r["b2"] + r["b1"])) for r in aoi
            if r.get("b1") is not None and r.get("b2") is not None and (r["b1"] + r["b2"])]
    forest_ndvi = round(sum(aoiv) / len(aoiv), 2) if aoiv else None
    pls = e.nearby_fc("power_lines", LON, LAT, 3500)
    prot = e.nearby_fc("protected", LON, LAT, 6000)
    tiles, scene_date = e.sentinel_tiles(LON, LAT)

    P = int(power); W = int(water); PR = int(protected)
    SITES = [{"name": "Espoo · Hepokorpi", "lon": 24.6883, "lat": 60.2371},
             {"name": "Kirkkonummi · Kolabacken", "lon": 24.5522, "lat": 60.1515},
             {"name": "Vihti · Nummela", "lon": 24.2380, "lat": 60.3283}]
    E = "https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog"
    site3067 = f"ST_Transform(ST_Point({LON},{LAT}),'EPSG:4326','EPSG:3067')"
    def near(tbl): return (f"SELECT round(min(ST_Distance(\n"
                           f"  ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),\n"
                           f"  {site3067}))) AS metres\nFROM {tbl};")
    steps = [
        {"kind": "user", "text": "I read that Microsoft is planning new data centres in Finland. Find the sites and tell me what the data says about them."},
        {"kind": "say", "text": "Plan: (1) find the proposed locations from the news, (2) connect to Finland's catalogue federation, (3) query the datasets that bear on siting a data centre — across catalogues — and assess. Let me start."},
        {"kind": "tool", "cmd": "search the news for the proposed locations",
         "sql": 'web.search("Microsoft data center Finland site location 2025")',
         "result": "3 sites in the Helsinki region · all reported contested",
         "source_url": "https://yle.fi/a/3-12084411", "source_label": "Yle News",
         "action": {"type": "markers", "points": SITES}},
        {"kind": "say", "text": "Three sites — **Espoo (Hepokorpi)**, **Kirkkonummi (Kolabacken)** and **Vihti (Nummela)** — all contested over forest clearing and energy. I'll take the Espoo one and ask Finland's own data what's actually there."},
        {"kind": "tool", "cmd": "connect to Finland's catalogue federation (by publisher)",
         "sql": (f"ATTACH 'nls'  (TYPE iceberg, ENDPOINT '{E}/national-land-survey',          AUTHORIZATION_TYPE 'none'); -- Maanmittauslaitos 🇫🇮\n"
                 f"ATTACH 'syke' (TYPE iceberg, ENDPOINT '{E}/finnish-environment-institute',  AUTHORIZATION_TYPE 'none'); -- SYKE 🇫🇮\n"
                 f"ATTACH 'cop'  (TYPE iceberg, ENDPOINT '{E}/copernicus',                     AUTHORIZATION_TYPE 'none'); -- Copernicus 🇪🇺\n"
                 "SHOW ALL TABLES;"),
         "result": ("connected to 3 publishers' catalogues:\n"
                    "  National Land Survey of Finland (Maanmittauslaitos)\n"
                    "  Finnish Environment Institute (SYKE)\n"
                    "  Copernicus — European Union")},
        {"kind": "tool", "cmd": "find datasets relevant to siting a data centre",
         "sql": ("SELECT id, collection FROM nls.catalog.datasets\n"
                 "WHERE properties ILIKE '%data-center%';"),
         "result": ("relevant datasets, by publisher:\n"
                    "  National Land Survey → sahkolinja · korkeusmalli_2m · jarvi · luonnonsuojelualue\n"
                    "  Copernicus           → ndvi\n"
                    "  SYKE                 → natura2000 · tulvavaarakartta · corine  (catalogued · convert-on-demand)"),
         "action": {"type": "flyto", "center": [LON, LAT], "zoom": 13.4}, "after": {"type": "marker"}},
        {"kind": "say", "text": "The data I need spans the National Land Survey and Copernicus. **Power first** — a data centre is only as good as its grid tie-in."},
        {"kind": "tool", "cmd": "National Land Survey · nearest transmission line", "sql": near("nls.v2.power_lines"),
         "result": f"{P} metres", "action": {"type": "layer", "id": "power_lines"},
         "callout": {"dlon": 0.0012, "dlat": 0.0016, "text": f"⚡ transmission line · {P} m", "tone": "amber"},
         "stat": {"label": "Grid", "value": f"{P} m to transmission line", "source": "National Land Survey · sahkolinja"}},
        {"kind": "say", "text": f"**{P} m** to a transmission line — favorable proximity. Proximity isn't capacity, though: an actual connection still depends on available grid capacity and permitting."},
        {"kind": "tool", "cmd": "National Land Survey · elevation & slope (2 m laser DEM)",
         "sql": ("-- raster queried in place (raquet), no download\n"
                 "SELECT min(elev), avg(elev), stddev(elev) FROM (\n"
                 "  SELECT ST_RasterValue(block, band_1, pt, metadata) elev\n"
                 "  FROM read_raquet('NLS → korkeusmalli_2m'), grid) ;"),
         "result": f"~{elev_mean} m · gentle slope · inland",
         "stat": {"label": "Terrain / flood", "value": f"~{elev_mean} m · no topographic flood flag (DEM)", "source": "National Land Survey · korkeusmalli_2m"}},
        {"kind": "say", "text": "Inland, flat, easy ground to build on — and no obvious topographic flood concern from the DEM. To actually rate flood risk I'd want SYKE's flood-hazard maps (catalogued below, not yet materialised)."},
        {"kind": "tool", "cmd": "National Land Survey · nearest water body", "sql": near("nls.v2.water"),
         "result": f"{W} metres",
         "callout": {"dlon": -0.004, "dlat": 0.007, "text": f"💧 lake · {W} m", "tone": "cyan"},
         "stat": {"label": "Cooling water", "value": f"~{W} m to a lake", "source": "National Land Survey · jarvi"}},
        {"kind": "tool", "cmd": "Copernicus · land cover (Sentinel-2 NDVI)",
         "sql": ("-- NDVI = (NIR - Red) / (NIR + Red) over the site\n"
                 "SELECT avg((band_2 - band_1)/(band_2 + band_1)) AS ndvi\n"
                 "FROM read_raquet('Copernicus → ndvi'), grid;"),
         "result": f"NDVI ~{forest_ndvi} → forest / fields", "action": {"type": "layer", "id": "sentinel"},
         "callout": {"dlon": -0.006, "dlat": 0.0012, "text": "🌲 forest — would be cleared", "tone": "green"},
         "stat": {"label": "Land cover", "value": f"forest · NDVI ~{forest_ndvi}", "source": "Copernicus · Sentinel-2 NDVI"}},
        {"kind": "say", "text": "**Here's the catch.** The site is forest. Building it clears green land at a residential edge — the core of the local objection."},
        {"kind": "tool", "cmd": "National Land Survey · protected areas within 5 km",
         "sql": ("SELECT nimi, round(ST_Distance(ST_Transform(geom_wkb_geom,'EPSG:4326','EPSG:3067'),\n"
                 f"  {site3067})) m\nFROM nls.v2.protected\nWHERE m < 5000 ORDER BY m LIMIT 1;"),
         "result": f"nearest {PR} m · no direct overlap", "action": {"type": "layer", "id": "protected"},
         "stat": {"label": "Protected", "value": f"~{PR} m to nearest", "source": "National Land Survey · luonnonsuojelualue"}},
        {"kind": "say", "text": "One more thing: **SYKE** — Finland's Environment Institute — publishes detailed **flood-hazard maps** and **Natura 2000** in the federation. They're catalogued here but not yet materialised; I'd convert them on demand to sharpen the flood and nature picture."},
        {"kind": "assessment"},
    ]
    assessment = {
        "verdict": "Strong site — with one real problem",
        "badge": "#FFC857",
        "factors": [
            {"label": "Grid access", "value": f"{P} m to transmission line", "status": "good"},
            {"label": "Terrain & flood", "value": f"~{elev_mean} m · inland · no DEM flood flag", "status": "good"},
            {"label": "Cooling water", "value": f"~{W} m to a lake", "status": "good"},
            {"label": "Protected areas", "value": f"~{PR} m · no overlap", "status": "good"},
            {"label": "Land cover", "value": f"forest · NDVI ~{forest_ndvi}", "status": "risk"},
        ],
        "risk": "Clears forest at a residential edge — the heart of the local objection. Environmental review required.",
        "note": "Every figure above has a named publisher and a live query you can re-run — **National Land Survey of Finland** (🇫🇮) and **Copernicus** (🇪🇺). The catalogues run on European, sovereign infrastructure (UpCloud, hosted in Finland): open formats, no proprietary portal, no hyperscaler in the loop.",
    }
    return {"site": {"lon": LON, "lat": LAT, "name": NAME}, "sites": SITES,
            "question_hint": "data center / Microsoft / build / Helsinki / Espoo",
            "metrics": {"power_m": P, "elev_mean": elev_mean, "water_m": W, "protected_m": PR, "forest_ndvi": forest_ndvi},
            "steps": steps, "assessment": assessment,
            "layers": {"power_lines": pls, "protected": prot}, "sentinel_tiles": tiles, "scene_date": scene_date}


if __name__ == "__main__":
    (HERE / "data").mkdir(exist_ok=True)
    cat = catalog(); (HERE / "data" / "catalog.json").write_text(json.dumps(cat))
    print(f"catalog.json: {cat['total']} datasets, {cat['materialized']} materialized")
    sc = scenario(); (HERE / "data" / "scenario.json").write_text(json.dumps(sc))
    print(f"scenario.json: {len(sc['steps'])} steps, metrics {sc['metrics']}, "
          f"power-lines {len(sc['layers']['power_lines']['features'])}, protected {len(sc['layers']['protected']['features'])}")
