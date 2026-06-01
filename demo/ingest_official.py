#!/usr/bin/env python3
"""
ingest_official.py — copy OFFICIAL Finnish national data into optimized
cloud-native GeoParquet on the sovereign UpCloud bucket. This conversion is the
talk's thesis in code: the data exists officially, but behind OGC API / WFS / WCS
services; we convert it to GeoParquet (via geoparquet-io / `gpio`) so an agent +
DuckDB can query it directly.

Pipeline:  NLS OGC API Features  →  page to GeoJSON  →  gpio (ZSTD+Hilbert+bbox)  →  mc → bucket

Source: National Land Survey of Finland Topographic Database (free API key).
Key:    ~/Desktop/nls_apikey.txt   (the key string; read in-shell, never printed)

Notes:
  • Service native CRS is EPSG:3067; request items in CRS84 (lon/lat) via bbox=.
  • `gpio` writes the geometry column as `geom` (ogr2ogr used `geometry`).
  • Useful collections: sahkolinja (power lines), muuntaja (transformers),
    luonnonsuojelualue / kansallispuisto (protected areas), rakennus (buildings),
    korkeuskayra (contours), tieviiva (roads).

Usage:
  python3 demo/ingest_official.py collections
  python3 demo/ingest_official.py sahkolinja          energy/power_lines.parquet 24.40,60.05,25.40,60.50
  python3 demo/ingest_official.py luonnonsuojelualue   environment/protected_national.parquet 24.70,60.05,25.30,60.40
"""
import base64, json, pathlib, re, subprocess, sys, urllib.request

KEY_FILE = pathlib.Path.home() / "Desktop" / "nls_apikey.txt"
BASE = "https://avoin-paikkatieto.maanmittauslaitos.fi/maastotiedot/features/v1"
BUCKET = "carto-ogc-connect-helsinki"
GPIO = str(pathlib.Path.home() / ".local" / "bin" / "gpio")


def auth():
    key = re.findall(r"[A-Za-z0-9._-]{20,}", KEY_FILE.read_text())[0]
    return "Basic " + base64.b64encode(f"{key}:".encode()).decode()


def get(url):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers={"Authorization": auth()}), timeout=60).read())


def collections():
    for c in get(BASE + "/collections?f=json").get("collections", []):
        cid, title = c.get("id", "?"), (c.get("title") or "")
        flag = " ⚡" if any(k in (cid + title).lower() for k in ("sähkö", "sahko", "voima", "johto", "muunta")) else ""
        print(f"  {cid:<28} {title}{flag}")


def ingest(collection, dest, bbox, limit=1000):
    feats, url = [], f"{BASE}/collections/{collection}/items?bbox={bbox}&limit={limit}&f=json"
    while url:
        d = get(url)
        feats += d.get("features", [])
        nxt = [l["href"] for l in d.get("links", []) if l.get("rel") == "next"]
        url = nxt[0] if (nxt and d.get("features")) else None
        if len(feats) > 50000:
            break
    gj = f"/tmp/{collection}.geojson"
    json.dump({"type": "FeatureCollection", "features": feats}, open(gj, "w"))
    print(f"{collection}: {len(feats)} features pulled")
    pq = f"/tmp/{collection}.parquet"
    subprocess.run([GPIO, "convert", "geoparquet", gj, pq], check=True)        # ZSTD + Hilbert + bbox
    subprocess.run(["mc", "cp", pq, f"upcloud/{BUCKET}/portolan/{dest}"], check=True)
    print(f"✓ optimized → upcloud/{BUCKET}/portolan/{dest}")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "collections":
        collections()
    else:
        coll, dest, bbox = sys.argv[1], sys.argv[2], sys.argv[3]
        ingest(coll, dest, bbox)
