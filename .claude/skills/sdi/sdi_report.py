#!/usr/bin/env python3
"""SDI report engine — attach the sovereign Iceberg/STAC catalog, discover +
query the cloud-native data with DuckDB, and render an HTML site-feasibility
report. Default intent: data-center siting.

Uses the DuckDB 1.5.3 CLI (the raquet community extension is built for it;
the python duckdb module is 1.5.0 and crashes loading it).

  python3 .claude/skills/sdi/sdi_report.py --lon 24.6883 --lat 60.2371 \
      --name "Espoo Hepokorpi" --question "Microsoft wants to build a data center here — what does the data say?"
"""
import argparse, json, pathlib, subprocess, sys, urllib.request, urllib.parse

ENDPOINT = "https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/catalog"
DEM = f"{ENDPOINT}/data/raster/dem_2m.parquet"
NDVI = f"{ENDPOINT}/data/raster/ndvi.parquet"
OUT = pathlib.Path(__file__).resolve().parents[3] / "demo" / "output" / "sdi_report.html"
TITILER = "https://titiler.xyz/cog/tiles/WebMercatorQuad/{z}/{x}/{y}?url="
SETUP = ("INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;"
         "INSTALL raquet FROM community;LOAD raquet;SET geometry_always_xy=true;"
         f"ATTACH 'sdi' AS sdi (TYPE iceberg, ENDPOINT '{ENDPOINT}', AUTHORIZATION_TYPE 'none');")


def q(sql):
    r = subprocess.run(["duckdb", "-unsigned", "-json", "-c", SETUP + sql], capture_output=True, text=True)
    out = r.stdout.strip()
    if not out:
        if r.returncode != 0:
            print("SQL error:", r.stderr.strip()[-400:], file=sys.stderr)
        return []
    try:
        return json.loads(out)
    except Exception:
        i = out.rfind("\n[")
        try:
            return json.loads(out[i + 1:]) if i >= 0 else []
        except Exception:
            return []


def q1(sql):
    rows = q(sql)
    return list(rows[0].values())[0] if rows else None


def dist(table, lon, lat):
    return q1(f"""SELECT round(min(ST_Distance(
        ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),
        ST_Transform(ST_Point({lon},{lat}),'EPSG:4326','EPSG:3067')))) AS m FROM sdi.v2.{table}""")


def raster_grid(url, bands, lon, lat):
    grid = f"SELECT {lon}+(i-3)*0.0009 lon,{lat}+(j-3)*0.00045 lat FROM range(0,7) a(i),range(0,7) b(j)"
    sel = ", ".join(f"arg_min(ST_RasterValue(r.block,r.band_{b},ST_Point(g.lon,g.lat),r.metadata),"
                    f"ST_Area(ST_GeomFromQuadbin(r.block))) AS b{b}" for b in bands)
    return q(f"""WITH grid AS ({grid}),
        s AS (SELECT g.lon,g.lat,{sel} FROM grid g, read_raquet('{url}') r
              WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)) GROUP BY g.lon,g.lat)
        SELECT * FROM s WHERE b{bands[0]} IS NOT NULL""")


def nearby_fc(table, lon, lat, radius):
    rows = q(f"""SELECT ST_AsGeoJSON(ST_GeomFromWKB(geom_wkb)) AS g FROM sdi.v2.{table}
        WHERE ST_Distance(ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),
                          ST_Transform(ST_Point({lon},{lat}),'EPSG:4326','EPSG:3067')) < {radius}""")
    def _geom(g): return json.loads(g) if isinstance(g, str) else g
    return {"type": "FeatureCollection",
            "features": [{"type": "Feature", "properties": {}, "geometry": _geom(r["g"])} for r in rows if r.get("g")]}


def sentinel_tiles(lon, lat):
    try:
        body = json.dumps({"collections": ["sentinel-2-l2a"],
                           "intersects": {"type": "Point", "coordinates": [lon, lat]},
                           "query": {"eo:cloud_cover": {"lt": 30}},
                           "sortby": [{"field": "properties.datetime", "direction": "desc"}], "limit": 1}).encode()
        req = urllib.request.Request("https://earth-search.aws.element84.com/v1/search", data=body,
                                     headers={"Content-Type": "application/json"})
        feat = json.loads(urllib.request.urlopen(req, timeout=25).read())["features"][0]
        return TITILER + urllib.parse.quote(feat["assets"]["visual"]["href"], safe=""), feat["properties"]["datetime"][:10]
    except Exception:
        return "", None


def _std(xs):
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5


def assess(power, flat, elev_min, water, protected, ndvi, pct_veg):
    cons, env, reasons = [], False, []
    if power is not None:
        if power < 500: reasons.append(f"✓ Favorable grid proximity — ~{int(power)} m to a transmission line (subject to capacity and permitting).")
        elif power < 2500: reasons.append(f"• Transmission line ~{int(power)} m away — proximity only; capacity and permitting still to confirm.")
        else: cons.append("grid"); reasons.append(f"✗ ~{power/1000:.1f} km to the nearest transmission line — connection likely costly.")
    if flat is not None:
        if flat < 1.5: reasons.append(f"✓ Very flat (elevation σ {flat} m over the site).")
        elif flat < 4: reasons.append(f"• Gently undulating (σ {flat} m) — workable.")
        else: cons.append("terrain"); reasons.append(f"✗ Uneven terrain (σ {flat} m) — significant earthworks.")
    if elev_min is not None:
        if elev_min > 10 and (water or 9999) > 300:
            reasons.append(f"✓ No obvious topographic flood concern from the available DEM — min elevation {elev_min} m, ~{int(water)} m to water. SYKE flood-hazard maps would sharpen this.")
        elif elev_min < 3 or (water or 9999) < 150:
            cons.append("flood"); reasons.append(f"✗ Low-lying / close to water (min elevation {elev_min} m, ~{int(water)} m) — flood review needed; SYKE flood-hazard maps would confirm.")
        else:
            reasons.append(f"• Elevation {elev_min} m, ~{int(water)} m to water — no clear topographic flood flag from the DEM; SYKE flood-hazard maps would sharpen this.")
    if ndvi is not None and (ndvi > 0.45 or (pct_veg or 0) > 40):
        env = True; reasons.append(f"⚠ Currently vegetated/forest (NDVI {ndvi}, ~{int(pct_veg)}% vegetated) — building clears green land; environmental review needed.")
    elif protected is not None and protected < 500:
        env = True; reasons.append(f"⚠ Adjacent to a protected area (~{int(protected)} m).")
    elif protected is not None:
        reasons.append(f"• Nearest protected area ~{int(protected)} m; vegetation NDVI {ndvi}.")
    if cons:
        return "Constrained", "#FF6B6B", reasons
    if env:
        return "Suitable — but environmentally contested", "#FFC857", reasons
    return "Favourable", "#16E0C8", reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lon", type=float, default=24.6883)
    ap.add_argument("--lat", type=float, default=60.2371)
    ap.add_argument("--name", default="Espoo Hepokorpi")
    ap.add_argument("--question", default="Is this a good site for a data center?")
    a = ap.parse_args()
    lon, lat = a.lon, a.lat

    print("① discovering datasets in the sovereign Iceberg/STAC catalog…", file=sys.stderr)
    total = q1("SELECT count(*) AS n FROM sdi.catalog.datasets")
    disc = q("""SELECT id, collection FROM sdi.catalog.datasets
        WHERE properties ILIKE '%data-center%' AND properties ILIKE '%"materialized": true%' ORDER BY collection""")
    used = [d["id"] for d in disc]
    print(f"   {total} catalogued; {len(used)} materialized & relevant: {', '.join(used)}", file=sys.stderr)

    print("② querying cloud-native data with DuckDB (GeoParquet + raquet)…", file=sys.stderr)
    power, protected, water = dist("power_lines", lon, lat), dist("protected", lon, lat), dist("water", lon, lat)
    dem = raster_grid(DEM, [1], lon, lat)
    elevs = [r["b1"] for r in dem if r.get("b1") is not None]
    elev_min = round(min(elevs), 1) if elevs else None
    flat = round(_std(elevs), 2) if elevs else None
    nd = raster_grid(NDVI, [1, 2], lon, lat)
    ndv = [((r["b2"] - r["b1"]) / (r["b2"] + r["b1"])) for r in nd
           if r.get("b1") is not None and r.get("b2") is not None and (r["b1"] + r["b2"])]
    ndvi = round(sum(ndv) / len(ndv), 2) if ndv else None
    pct_veg = round(100 * sum(1 for v in ndv if v > 0.45) / len(ndv)) if ndv else None
    print(f"   grid {power} m · flat σ {flat} · min-elev {elev_min} m · water {water} m · protected {protected} m · NDVI {ndvi}", file=sys.stderr)

    pls, prot = nearby_fc("power_lines", lon, lat, 3500), nearby_fc("protected", lon, lat, 5000)
    tiles, scene_date = sentinel_tiles(lon, lat)
    verdict, badge, reasons = assess(power, flat, elev_min, water, protected, ndvi, pct_veg)
    findings = {"site": {"lon": lon, "lat": lat, "name": a.name}, "question": a.question, "verdict": verdict,
                "metrics": {"power_m": power, "flatness_sigma_m": flat, "min_elev_m": elev_min, "water_m": water,
                            "protected_m": protected, "ndvi": ndvi, "pct_vegetated": pct_veg},
                "reasons": reasons, "catalog_total": total, "datasets_used": used, "scene_date": scene_date}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    (OUT.parent / "sdi_findings.json").write_text(json.dumps(findings, indent=2))
    OUT.write_text(render(findings, badge, tiles, pls, prot))
    print(f"③ verdict: {verdict.upper()}  →  {OUT}", file=sys.stderr)
    print(OUT)


def render(f, badge, tiles, pls, prot):
    s = f["site"]; mx = f["metrics"]
    reasons = "".join(f"<li>{r}</li>" for r in f["reasons"])
    rationale = "".join(f"<li>{t}</li>" for t in [
        "Connected to a sovereign Iceberg catalog on EU object storage (UpCloud 🇫🇮) — anonymous, no server.",
        f"Searched its STAC index (stac-geoparquet): {f['catalog_total']} datasets catalogued; selected the relevant materialized ones.",
        "Queried the cloud-native data directly with DuckDB — GeoParquet for vectors, raquet for the rasters.",
        "Combined grid · terrain · flood · water · protected land · vegetation into one assessment.",
        "Ran on European, sovereign infrastructure: data on a Finnish cloud, queried by an open engine, open formats, no proprietary portal."])
    def i(v): return str(int(v)) if v is not None else "—"
    return (TEMPLATE
            .replace("__CENTER__", json.dumps([s["lon"], s["lat"]])).replace("__SITE__", json.dumps(s))
            .replace("__PLS__", json.dumps(pls)).replace("__PROT__", json.dumps(prot)).replace("__TILES__", tiles)
            .replace("__NAME__", s["name"]).replace("__Q__", f["question"]).replace("__VERDICT__", f["verdict"]).replace("__BADGE__", badge)
            .replace("__GRID__", f"{i(mx['power_m'])} m to transmission line")
            .replace("__TERRAIN__", f"σ {mx['flatness_sigma_m']} m · min {mx['min_elev_m']} m")
            .replace("__WATER__", f"~{i(mx['water_m'])} m to water")
            .replace("__PROTM__", f"~{i(mx['protected_m'])} m to protected")
            .replace("__NDVI__", f"NDVI {mx['ndvi']} · ~{i(mx['pct_vegetated'])}% vegetated")
            .replace("__REASONS__", reasons).replace("__RATIONALE__", rationale)
            .replace("__USED__", " · ".join(f["datasets_used"])).replace("__SCENE__", f["scene_date"] or "—"))


TEMPLATE = r"""<!doctype html><html><head><meta charset="utf-8">
<title>SDI report — __NAME__</title><meta name="viewport" content="width=device-width,initial-scale=1">
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
<style>
 html,body,#map{margin:0;height:100%;width:100%;background:#07101F;font-family:Inter,system-ui,sans-serif}
 .panel{position:absolute;top:18px;left:18px;z-index:5;width:400px;max-height:93%;overflow:auto;
   background:rgba(10,18,32,.94);border:1px solid rgba(138,160,189,.25);border-radius:16px;padding:22px 24px;color:#F4F8FF;backdrop-filter:blur(8px)}
 h1{font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:#16E0C8;margin:0 0 6px;font-weight:700}
 h2{font-size:21px;margin:0 0 4px;letter-spacing:-.01em} .q{font-size:13px;color:#8AA0BD;margin:0 0 14px;font-style:italic}
 .badge{display:inline-block;font-weight:800;font-size:13px;color:#07101F;background:__BADGE__;border-radius:8px;padding:5px 13px;margin-bottom:14px}
 .sec{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#8AA0BD;margin:13px 0 5px;font-weight:600}
 .grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;font-size:12px}
 .kv{background:#0E1D33;border:1px solid rgba(138,160,189,.2);border-radius:9px;padding:8px 11px}
 .kv b{color:#4DA3FF;display:block;font-size:10px;letter-spacing:.05em;text-transform:uppercase;margin-bottom:2px}
 ul{margin:5px 0 0;padding-left:2px;list-style:none;font-size:12.5px;line-height:1.5} ul li{margin:5px 0;color:#E7EEFA}
 ul.rat{counter-reset:s} ul.rat li{position:relative;padding-left:24px;color:#9FB3CE;font-size:12px}
 ul.rat li:before{counter-increment:s;content:counter(s);position:absolute;left:0;top:0;width:17px;height:17px;border-radius:50%;
   background:rgba(22,224,200,.15);color:#16E0C8;font-size:10px;font-weight:700;text-align:center;line-height:17px}
 .meta{margin-top:14px;font-size:11px;color:#8AA0BD;line-height:1.5;border-top:1px solid rgba(138,160,189,.2);padding-top:11px} .meta b{color:#16E0C8}
</style></head><body>
<div id="map"></div>
<div class="panel">
 <h1>Sovereign SDI · site assessment</h1><h2>__NAME__</h2><p class="q">"__Q__"</p>
 <span class="badge">__VERDICT__</span>
 <div class="grid">
   <div class="kv"><b>Grid</b>__GRID__</div><div class="kv"><b>Terrain</b>__TERRAIN__</div>
   <div class="kv"><b>Water / flood</b>__WATER__</div><div class="kv"><b>Protected</b>__PROTM__</div>
   <div class="kv" style="grid-column:1/-1"><b>Vegetation (Sentinel-2)</b>__NDVI__</div>
 </div>
 <div class="sec">Assessment</div><ul>__REASONS__</ul>
 <div class="sec">How the SDI agent worked</div><ul class="rat">__RATIONALE__</ul>
 <div class="meta">Datasets: <b>__USED__</b> · Sentinel-2 __SCENE__<br>
   Source: sovereign Iceberg/STAC catalog on UpCloud (🇫🇮), queried by DuckDB. Initial screen from open data — not a permit decision.</div>
</div>
<script>
const site=__SITE__, pls=__PLS__, prot=__PROT__;
const map=new maplibregl.Map({container:'map',style:'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',center:__CENTER__,zoom:13.3});
map.addControl(new maplibregl.NavigationControl(),'top-right');
map.on('load',()=>{
  const t="__TILES__"; if(t){map.addSource('s2',{type:'raster',tiles:[t],tileSize:256,attribution:'Sentinel-2 / Copernicus'});
    map.addLayer({id:'s2',type:'raster',source:'s2',paint:{'raster-opacity':.85}});}
  if(prot&&prot.features.length){map.addSource('prot',{type:'geojson',data:prot});
    map.addLayer({id:'pf',type:'fill',source:'prot',paint:{'fill-color':'#16E0C8','fill-opacity':.22}});
    map.addLayer({id:'pl2',type:'line',source:'prot',paint:{'line-color':'#16E0C8','line-width':1.2}});}
  if(pls&&pls.features.length){map.addSource('pls',{type:'geojson',data:pls});
    map.addLayer({id:'plw',type:'line',source:'pls',paint:{'line-color':'#FFC857','line-width':2.5,'line-dasharray':[2,1]}});}
  map.addSource('site',{type:'geojson',data:{type:'Feature',geometry:{type:'Point',coordinates:[site.lon,site.lat]}}});
  map.addLayer({id:'st',type:'circle',source:'site',paint:{'circle-radius':10,'circle-color':'#FF6B6B','circle-stroke-color':'#fff','circle-stroke-width':3}});
});
</script></body></html>"""


if __name__ == "__main__":
    main()
