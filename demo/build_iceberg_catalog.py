#!/usr/bin/env python3
"""Build a static Iceberg REST catalog of the Espoo data-center datasets on
UpCloud. Three layers:
  - v2/* : GeoIceberg V2 vector tables (WKB + flat bbox) — DuckDB-portable
  - v3/* : native V3 geometry vector tables (geoarrow) — Snowflake/CARTO
  - catalog.datasets : a stac-geoparquet (STAC-in-Iceberg) index of ALL source
    datasets (128 NLS + DEM + Sentinel), with a `materialized` flag.
Reuses iceberg-geo-testbed's write_static_catalog.

Run with the testbed venv (pyiceberg + geoarrow):
  ~/workspace/iceberg-geo-testbed/.venv/bin/python demo/build_iceberg_catalog.py [--publish]
"""
from __future__ import annotations
import json, struct, subprocess, sys, tempfile, shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import geoarrow.pyarrow as ga
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, DoubleType,
                             BinaryType, StructType, TimestamptzType)

TESTBED = Path("/Users/jatorre/workspace/iceberg-geo-testbed")
sys.path.insert(0, str(TESTBED))
from testbed._static_catalog import write_static_catalog  # noqa: E402

BUCKET = "carto-ogc-connect-helsinki"
PREFIX = "catalog"
BASE_URI = f"https://8et4c.upcloudobjects.com/{BUCKET}/{PREFIX}"
IRC_PREFIX = "sdi"
STAGING = Path("/tmp/sdi_catalog")
SRC = Path("/tmp")
GEOM_EXT = ga.wkb().with_crs(ga.OGC_CRS84)

def dle(v): return struct.pack("<d", float(v))
def xy(x, y): return struct.pack("<dd", float(x), float(y))
def _fmeta(i): return {"PARQUET:field_id": str(i)}

DATASETS = {
    "power_lines": dict(
        title="Electricity transmission lines (NLS Topographic Database)", theme="energy / grid",
        semantics=dict(describes="Overhead high-voltage electricity transmission lines (NLS).",
                       answers=["grid proximity", "distance to power infrastructure", "can-i-build-a-data-center"],
                       geometry="LineString")),
    "protected": dict(
        title="Protected nature areas (NLS luonnonsuojelualue)", theme="environment / constraint",
        semantics=dict(describes="Statutory nature protection areas (NLS).",
                       answers=["environmental constraint", "distance to protected area", "can-i-build-a-data-center"],
                       geometry="Polygon")),
    "water": dict(
        title="Water bodies / lakes (NLS jarvi)", theme="environment / water",
        semantics=dict(describes="Inland water bodies / lakes (NLS).",
                       answers=["water proximity", "flood / cooling context", "can-i-build-a-data-center"],
                       geometry="Polygon")),
}
COLS = ["id", "kohderyhma", "kohdeluokka", "fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax", "geom_wkb"]


def _ext(t):
    return (pc.min(t["fp_xmin"]).as_py(), pc.min(t["fp_ymin"]).as_py(),
            pc.max(t["fp_xmax"]).as_py(), pc.max(t["fp_ymax"]).as_py())
def _props(info, variant):
    sem = dict(info["semantics"])
    sem["query_recipe"] = (
        "nearest distance (m): ST_Distance(ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'), site_3067)"
        if variant == "v2" else
        "nearest distance (m): ST_Distance(ST_Transform(geom,'EPSG:4326','EPSG:3067'), site_3067)")
    return {"theme": info["theme"], "title": info["title"], "semantics": json.dumps(sem)}


def build_v2(name, info):
    t = pq.read_table(SRC / f"{name}_src.parquet")
    schema = pa.schema([
        pa.field("id", pa.string(), metadata=_fmeta(1)),
        pa.field("kohderyhma", pa.int32(), metadata=_fmeta(2)),
        pa.field("kohdeluokka", pa.int32(), metadata=_fmeta(3)),
        pa.field("fp_xmin", pa.float64(), metadata=_fmeta(4)),
        pa.field("fp_ymin", pa.float64(), metadata=_fmeta(5)),
        pa.field("fp_xmax", pa.float64(), metadata=_fmeta(6)),
        pa.field("fp_ymax", pa.float64(), metadata=_fmeta(7)),
        pa.field("geom_wkb", pa.binary(), metadata=_fmeta(8)),
    ])
    tbl = pa.table({c: t[c] for c in COLS}, schema=schema)
    root = STAGING / "data" / "v2" / name
    (root / "data").mkdir(parents=True, exist_ok=True)
    pqpath = root / "data" / f"{name}.parquet"
    pq.write_table(tbl, pqpath, compression="zstd")
    ice = Schema(
        NestedField(1, "id", StringType(), required=False),
        NestedField(2, "kohderyhma", IntegerType(), required=False),
        NestedField(3, "kohdeluokka", IntegerType(), required=False),
        NestedField(4, "fp_xmin", DoubleType(), required=False),
        NestedField(5, "fp_ymin", DoubleType(), required=False),
        NestedField(6, "fp_xmax", DoubleType(), required=False),
        NestedField(7, "fp_ymax", DoubleType(), required=False),
        NestedField(8, "geom_wkb", BinaryType(), required=False))
    fields = [{"id": 1, "name": "id", "required": False, "type": "string"},
              {"id": 2, "name": "kohderyhma", "required": False, "type": "int"},
              {"id": 3, "name": "kohdeluokka", "required": False, "type": "int"},
              {"id": 4, "name": "fp_xmin", "required": False, "type": "double"},
              {"id": 5, "name": "fp_ymin", "required": False, "type": "double"},
              {"id": 6, "name": "fp_xmax", "required": False, "type": "double"},
              {"id": 7, "name": "fp_ymax", "required": False, "type": "double"},
              {"id": 8, "name": "geom_wkb", "required": False, "type": "binary"}]
    namemap = [{"field-id": i + 1, "names": [n]} for i, n in enumerate(COLS)]
    geo = {"version": "1.0", "primary_column": "geom_wkb", "columns": {"geom_wkb": {
        "encoding": "WKB", "crs": "OGC:CRS84", "edges": "planar",
        "bbox_columns": ["fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax"]}}}
    lo = {4: dle(pc.min(t["fp_xmin"]).as_py()), 5: dle(pc.min(t["fp_ymin"]).as_py()),
          6: dle(pc.min(t["fp_xmax"]).as_py()), 7: dle(pc.min(t["fp_ymax"]).as_py())}
    up = {4: dle(pc.max(t["fp_xmin"]).as_py()), 5: dle(pc.max(t["fp_ymin"]).as_py()),
          6: dle(pc.max(t["fp_xmax"]).as_py()), 7: dle(pc.max(t["fp_ymax"]).as_py())}
    df = [{"path": f"data/{name}.parquet", "size": pqpath.stat().st_size, "rows": t.num_rows, "lower": lo, "upper": up}]
    mp = write_static_catalog(table_root=root, iceberg_schema=ice, schema_json_fields=fields,
                              name_mapping=namemap, data_files=df, format_version_in_metadata=2,
                              location_uri=f"{BASE_URI}/data/v2/{name}",
                              extra_properties={"geo": json.dumps(geo), **_props(info, "v2")})
    return json.loads(Path(mp).read_text())


def build_v3(name, info):
    t = pq.read_table(SRC / f"{name}_src.parquet")
    geom = GEOM_EXT.wrap_array(t["geom_wkb"].combine_chunks())
    schema = pa.schema([
        pa.field("id", pa.string(), metadata=_fmeta(1)),
        pa.field("kohderyhma", pa.int32(), metadata=_fmeta(2)),
        pa.field("kohdeluokka", pa.int32(), metadata=_fmeta(3)),
        pa.field("geom", GEOM_EXT, metadata=_fmeta(4)),
    ])
    tbl = pa.table({"id": t["id"], "kohderyhma": t["kohderyhma"], "kohdeluokka": t["kohdeluokka"], "geom": geom}, schema=schema)
    root = STAGING / "data" / "v3" / name
    (root / "data").mkdir(parents=True, exist_ok=True)
    pqpath = root / "data" / f"{name}.parquet"
    pq.write_table(tbl, pqpath, compression="zstd", store_schema=True, write_statistics=True)
    ice = Schema(
        NestedField(1, "id", StringType(), required=False),
        NestedField(2, "kohderyhma", IntegerType(), required=False),
        NestedField(3, "kohdeluokka", IntegerType(), required=False),
        NestedField(4, "geom", BinaryType(), required=False))
    fields = [{"id": 1, "name": "id", "required": False, "type": "string"},
              {"id": 2, "name": "kohderyhma", "required": False, "type": "int"},
              {"id": 3, "name": "kohdeluokka", "required": False, "type": "int"},
              {"id": 4, "name": "geom", "required": False, "type": "geometry"}]
    namemap = [{"field-id": i + 1, "names": [n]} for i, n in enumerate(["id", "kohderyhma", "kohdeluokka", "geom"])]
    x0, y0, x1, y1 = _ext(t)
    idmin = pc.min(t["id"]).as_py().encode(); idmax = pc.max(t["id"]).as_py().encode()
    df = [{"path": f"data/{name}.parquet", "size": pqpath.stat().st_size, "rows": t.num_rows,
           "lower": {1: idmin, 4: xy(x0, y0)}, "upper": {1: idmax, 4: xy(x1, y1)},
           "value_counts": {1: t.num_rows, 4: t.num_rows}, "null_value_counts": {1: 0, 4: 0}}]
    mp = write_static_catalog(table_root=root, iceberg_schema=ice, schema_json_fields=fields,
                              name_mapping=namemap, data_files=df, format_version_in_metadata=3,
                              location_uri=f"{BASE_URI}/data/v3/{name}", extra_properties=_props(info, "v3"))
    return json.loads(Path(mp).read_text())


# --- catalog.datasets : stac-geoparquet (STAC Items in Iceberg) -------------
# Federation by real PUBLISHER. Each publisher converts ITS OWN data, so each gets its
# OWN filtered STAC index. Materialized vector data is a base table name (power_lines/
# protected/water); the href is rendered per scope — combined catalog (attached as
# `sdi`) uses `sdi.v2.<t>`, a publisher catalog uses `v2.<t>` (relative to its alias).
MATERIALIZED = {  # NLS id -> (v2 table base, format, recipe, keywords)
    "sahkolinja": ("power_lines", "geoparquet",
                   "ST_Distance to ST_GeomFromWKB(geom_wkb) in EPSG:3067", "grid proximity; can-i-build-a-data-center"),
    "luonnonsuojelualue": ("protected", "geoparquet",
                   "ST_Distance to protected polygons in EPSG:3067", "environmental constraint; can-i-build-a-data-center"),
    "jarvi": ("water", "geoparquet",
                   "ST_Distance to lakes in EPSG:3067", "water/cooling/flood context; can-i-build-a-data-center"),
}
RELEVANT = {"rakennus": "buildings; residential proximity", "tieviiva": "roads; access",
            "muuntaja": "transformer/substation; grid", "korkeuskayra": "contours; terrain"}

# collection prefix -> publisher sub-catalog (who converted it)
def publisher_of(collection):
    if collection.startswith("nls"):
        return "national-land-survey"
    if collection == "syke":
        return "finnish-environment-institute"
    return "copernicus"


def _wkb_box(x0, y0, x1, y1):
    if None in (x0, y0, x1, y1):
        return None
    b = struct.pack("<BIII", 1, 3, 1, 5)  # LE, Polygon, 1 ring, 5 pts
    for x, y in [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]:
        b += struct.pack("<dd", float(x), float(y))
    return b


# A concrete, runnable example query carried IN the dataset metadata — so an agent
# learns how to query each dataset by reading its own STAC properties (self-describing),
# not from the skill. `<catalog>` = the alias you ATTACH this catalog as; :lon/:lat = site.
_GRID = ("WITH grid AS (SELECT :lon+(i-3)*0.0009 lon, :lat+(j-3)*0.00045 lat "
         "FROM range(0,7) a(i), range(0,7) b(j)) ")
def _example_query(mat):
    if not mat:
        return None
    kind, val, _fmt = mat
    if kind == "table":  # vector (GeoParquet, WKB) — nearest distance in metric CRS
        return ("SELECT round(min(ST_Distance("
                "ST_Transform(ST_GeomFromWKB(geom_wkb),'EPSG:4326','EPSG:3067'),"
                "ST_Transform(ST_Point(:lon,:lat),'EPSG:4326','EPSG:3067')))) AS metres "
                f"FROM <catalog>.v2.{val};")
    if "ndvi" in val:  # raster (raquet) — NDVI = (NIR-Red)/(NIR+Red)
        return _GRID + ("SELECT avg((b2-b1)/(b2+b1)) AS ndvi FROM ("
                        "SELECT ST_RasterValue(r.block,r.band_2,ST_Point(g.lon,g.lat),r.metadata) b2,"
                        "ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) b1 "
                        f"FROM grid g, read_raquet('{val}') r "
                        "WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)) AND r.band_1 IS NOT NULL);")
    return _GRID + ("SELECT min(elev) AS min_m, stddev(elev) AS slope_sigma FROM ("
                    "SELECT ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) elev "
                    f"FROM grid g, read_raquet('{val}') r "
                    "WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)));")


def collect_rows():
    """All STAC items as a row dict R, plus a parallel `publisher` list and a `mat`
    list of (kind, value, format) — kind in {'table','url',None} — so each index can
    render assets/hrefs for its own scope."""
    cols = json.loads(Path("/tmp/nls_all.json").read_text())["collections"]
    R = {k: [] for k in ("id", "collection", "geometry", "xmin", "ymin", "xmax", "ymax",
                         "datetime", "properties", "assets_recipe", "stac_version", "type",
                         "publisher", "mat")}
    def add(cid, coll, title, desc, item_type, bbox, crs, mat, recipe, kw):
        # mat: ('table','power_lines','geoparquet') | ('url',<href>,'raquet') | None
        x0, y0, x1, y1 = (list(bbox) + [None] * 4)[:4] if bbox else (None, None, None, None)
        R["id"].append(cid); R["collection"].append(coll)
        R["geometry"].append(_wkb_box(x0, y0, x1, y1))
        R["xmin"].append(x0); R["ymin"].append(y0); R["xmax"].append(x1); R["ymax"].append(y1)
        R["datetime"].append(None)
        R["properties"].append(json.dumps({
            "title": title, "description": (desc or "")[:400],
            "keywords": kw.split("; ") if kw else [], "item_type": item_type, "crs": crs,
            "materialized": mat is not None, "data_format": (mat[2] if mat else None),
            "access_recipe": (recipe if recipe else "convert on demand: publisher OGC API Features -> gpio -> bucket"),
            "example_query": _example_query(mat)}))
        R["stac_version"].append("1.1.0"); R["type"].append("Feature")
        R["publisher"].append(publisher_of(coll)); R["mat"].append(mat)
    for c in cols:
        sp = (((c.get("extent") or {}).get("spatial") or {}).get("bbox") or [None])
        bbox = sp[0] if (sp and isinstance(sp[0], list)) else None
        m = MATERIALIZED.get(c["id"])
        add(c["id"], "nls-topographic", c.get("title"), c.get("description"), c.get("itemType"),
            bbox, "OGC:CRS84", (("table", m[0], m[1]) if m else None), (m[2] if m else None),
            m[3] if m else RELEVANT.get(c["id"], ""))
    add("korkeusmalli_2m", "nls-elevation", "Elevation model 2 m (laser DEM)",
        "NLS 2 m laser-scanned elevation model — flatness + flood for data-center siting.", "coverage",
        [24.62, 60.21, 24.76, 60.27], "EPSG:3067",
        ("url", f"{BASE_URI}/data/raster/dem_2m.parquet", "raquet"),
        "read_raquet + ST_RasterValue grid-sample (finest zoom via ST_GeomFromQuadbin)",
        "flatness; flood; elevation; can-i-build-a-data-center")
    add("ndvi", "copernicus-sentinel2", "Sentinel-2 NDVI (red+NIR)",
        "Copernicus Sentinel-2 red/NIR for NDVI — vegetation/forest that would be cleared.", "coverage",
        [24.62, 60.21, 24.76, 60.27], "EPSG:3857",
        ("url", f"{BASE_URI}/data/raster/ndvi.parquet", "raquet"),
        "read_raquet; NDVI=(band_2-band_1)/(band_2+band_1)",
        "vegetation; forest clearing; land cover; can-i-build-a-data-center")
    # SYKE — Finnish Environment Institute: real publisher, datasets CATALOGUED (convert-on-demand)
    FIN = [19.0, 59.5, 31.6, 70.1]
    add("natura2000", "syke", "Natura 2000 protected areas",
        "EU Natura 2000 network sites in Finland, published by SYKE.", "feature", FIN, "OGC:CRS84", None, None,
        "environmental constraint; protected; can-i-build-a-data-center")
    add("tulvavaarakartta", "syke", "Flood hazard maps",
        "Flood risk / hazard zones, published by SYKE.", "feature", FIN, "OGC:CRS84", None, None,
        "flood risk; can-i-build-a-data-center")
    add("corine-land-cover", "syke", "CORINE Land Cover 2018",
        "Pan-European CORINE land cover for Finland, published by SYKE.", "coverage", FIN, "OGC:CRS84", None, None,
        "land cover; vegetation; can-i-build-a-data-center")
    return R


def _render_assets(mat, scope):
    """scope: 'combined' -> sdi.v2.<t> ; 'publisher' -> v2.<t> (relative to alias)."""
    if mat is None:
        return json.dumps({})
    kind, val, fmt = mat
    href = (f"sdi.v2.{val}" if scope == "combined" else f"v2.{val}") if kind == "table" else val
    return json.dumps({"data": {"href": href, "type": fmt, "roles": ["data"]}})


# stac-geoparquet Iceberg schema (shared by every index table)
_BBOX_T = pa.struct([pa.field("xmin", pa.float64(), metadata=_fmeta(10)),
                     pa.field("ymin", pa.float64(), metadata=_fmeta(11)),
                     pa.field("xmax", pa.float64(), metadata=_fmeta(12)),
                     pa.field("ymax", pa.float64(), metadata=_fmeta(13))])
_IDX_SCHEMA = pa.schema([
    pa.field("id", pa.string(), metadata=_fmeta(1)),
    pa.field("collection", pa.string(), metadata=_fmeta(2)),
    pa.field("geometry", pa.binary(), metadata=_fmeta(3)),
    pa.field("bbox", _BBOX_T, metadata=_fmeta(4)),
    pa.field("datetime", pa.timestamp("us", tz="UTC"), metadata=_fmeta(5)),
    pa.field("properties", pa.string(), metadata=_fmeta(6)),
    pa.field("assets", pa.string(), metadata=_fmeta(7)),
    pa.field("stac_version", pa.string(), metadata=_fmeta(8)),
    pa.field("type", pa.string(), metadata=_fmeta(9))])
_IDX_ICE = Schema(
    NestedField(1, "id", StringType(), required=False),
    NestedField(2, "collection", StringType(), required=False),
    NestedField(3, "geometry", BinaryType(), required=False),
    NestedField(4, "bbox", StructType(
        NestedField(10, "xmin", DoubleType(), required=False),
        NestedField(11, "ymin", DoubleType(), required=False),
        NestedField(12, "xmax", DoubleType(), required=False),
        NestedField(13, "ymax", DoubleType(), required=False)), required=False),
    NestedField(5, "datetime", TimestamptzType(), required=False),
    NestedField(6, "properties", StringType(), required=False),
    NestedField(7, "assets", StringType(), required=False),
    NestedField(8, "stac_version", StringType(), required=False),
    NestedField(9, "type", StringType(), required=False))
_IDX_FIELDS = [{"id": 1, "name": "id", "required": False, "type": "string"},
               {"id": 2, "name": "collection", "required": False, "type": "string"},
               {"id": 3, "name": "geometry", "required": False, "type": "binary"},
               {"id": 4, "name": "bbox", "required": False, "type": {"type": "struct", "fields": [
                   {"id": 10, "name": "xmin", "required": False, "type": "double"},
                   {"id": 11, "name": "ymin", "required": False, "type": "double"},
                   {"id": 12, "name": "xmax", "required": False, "type": "double"},
                   {"id": 13, "name": "ymax", "required": False, "type": "double"}]}},
               {"id": 5, "name": "datetime", "required": False, "type": "timestamptz"},
               {"id": 6, "name": "properties", "required": False, "type": "string"},
               {"id": 7, "name": "assets", "required": False, "type": "string"},
               {"id": 8, "name": "stac_version", "required": False, "type": "string"},
               {"id": 9, "name": "type", "required": False, "type": "string"}]
_IDX_NAMEMAP = [{"field-id": 1, "names": ["id"]}, {"field-id": 2, "names": ["collection"]},
                {"field-id": 3, "names": ["geometry"]},
                {"field-id": 4, "names": ["bbox"], "fields": [{"field-id": 10, "names": ["xmin"]},
                 {"field-id": 11, "names": ["ymin"]}, {"field-id": 12, "names": ["xmax"]}, {"field-id": 13, "names": ["ymax"]}]},
                {"field-id": 5, "names": ["datetime"]}, {"field-id": 6, "names": ["properties"]},
                {"field-id": 7, "names": ["assets"]}, {"field-id": 8, "names": ["stac_version"]}, {"field-id": 9, "names": ["type"]}]
_IDX_GEO = {"version": "1.0", "primary_column": "geometry", "columns": {"geometry": {
    "encoding": "WKB", "crs": "OGC:CRS84", "edges": "planar", "bbox_columns": ["bbox"]}}}


def write_index(R, idxs, storage_key, scope, title):
    """Write a (filtered) stac-geoparquet index table for the given row indices."""
    pick = lambda col: [R[col][i] for i in idxs]
    bbox_arr = pa.StructArray.from_arrays(
        [pa.array(pick("xmin"), pa.float64()), pa.array(pick("ymin"), pa.float64()),
         pa.array(pick("xmax"), pa.float64()), pa.array(pick("ymax"), pa.float64())], fields=_BBOX_T)
    assets = [_render_assets(R["mat"][i], scope) for i in idxs]
    tbl = pa.table({"id": pick("id"), "collection": pick("collection"),
                    "geometry": pa.array(pick("geometry"), pa.binary()), "bbox": bbox_arr,
                    "datetime": pa.array(pick("datetime"), pa.timestamp("us", tz="UTC")),
                    "properties": pick("properties"), "assets": assets,
                    "stac_version": pick("stac_version"), "type": pick("type")}, schema=_IDX_SCHEMA)
    root = STAGING / "data" / storage_key
    (root / "data").mkdir(parents=True, exist_ok=True)
    pqpath = root / "data" / "datasets.parquet"
    pq.write_table(tbl, pqpath, compression="zstd")
    qcat = "sdi" if scope == "combined" else "<this catalog>"
    props = {"geo": json.dumps(_IDX_GEO), "theme": "catalog-index", "format": "stac-geoparquet", "title": title,
             "semantics": json.dumps({"describes": "STAC Items for this catalog's datasets (stac-geoparquet). properties.materialized=true means cloud-native data is published (assets.data.href); others catalogued & convertible on demand.",
                                      "answers": ["dataset discovery", "what data exists", "is X available"],
                                      "query_recipe": f"SELECT id, collection, assets FROM {qcat}.catalog.datasets WHERE properties ILIKE '%data-center%'"})}
    mp = write_static_catalog(table_root=root, iceberg_schema=_IDX_ICE, schema_json_fields=_IDX_FIELDS,
                              name_mapping=_IDX_NAMEMAP, data_files=[{"path": "data/datasets.parquet",
                              "size": pqpath.stat().st_size, "rows": tbl.num_rows, "lower": {}, "upper": {}}],
                              format_version_in_metadata=2, location_uri=f"{BASE_URI}/data/{storage_key}",
                              extra_properties=props, last_column_id_override=13)
    return json.loads(Path(mp).read_text()), tbl.num_rows


# --- IRC surface ------------------------------------------------------------
# tables: list of (namespace, name, meta, storage_key). metadata-location is derived
# from storage_key so a publisher index (e.g. idx/nls) can appear as catalog.datasets.
def make_surface(tables):
    s = {}
    def put(k, b): s[k] = json.dumps(b, indent=2)
    ns_tables = {}
    for ns, name, meta, key in tables:
        ns_tables.setdefault(ns, []).append((name, meta, key))
    put("v1/config", {"defaults": {}, "overrides": {"prefix": IRC_PREFIX},
                      "endpoints": [f"GET /v1/{IRC_PREFIX}/namespaces",
                                    f"GET /v1/{IRC_PREFIX}/namespaces/{{namespace}}",
                                    f"GET /v1/{IRC_PREFIX}/namespaces/{{namespace}}/tables",
                                    f"GET /v1/{IRC_PREFIX}/namespaces/{{namespace}}/tables/{{table}}"]})
    put(f"v1/{IRC_PREFIX}/namespaces", {"namespaces": [[n] for n in ns_tables]})
    for ns, items in ns_tables.items():
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}", {"namespace": [ns], "properties": {}})
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables",
            {"identifiers": [{"namespace": [ns], "name": nm} for nm, _, _ in items]})
        for nm, meta, key in items:
            loc = f"{BASE_URI}/data/{key}/metadata/v1.metadata.json"
            put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables/{nm}",
                {"metadata-location": loc, "metadata": meta, "config": {}})
    return s


def publish(catalogs):
    """catalogs: {sub_path_or_'': surface_dict}. '' = combined catalog at the root."""
    dst = f"upcloud/{BUCKET}/{PREFIX}"
    with tempfile.TemporaryDirectory() as tmp:
        def push(key, text):
            f = Path(tmp) / "o.json"; f.write_text(text)
            subprocess.run(["mc", "cp", "--quiet", str(f), f"{dst}/{key}"], check=True)
        for name, surf in catalogs.items():
            base = "" if name == "" else f"{name}/"
            for key, text in sorted(surf.items()):
                push(f"{base}{key}", text)
    subprocess.run(["mc", "cp", "--quiet", "--recursive", f"{STAGING}/data/", f"{dst}/data/"], check=True)
    for src, name in [("portolan/terrain/dem_2m.parquet", "dem_2m.parquet"),
                      ("portolan/eo/ndvi.parquet", "ndvi.parquet")]:
        subprocess.run(["mc", "cp", "--quiet", f"upcloud/{BUCKET}/{src}", f"{dst}/data/raster/{name}"], check=True)
    subprocess.run(["mc", "anonymous", "set", "download", dst], check=True)
    print(f"published combined + {len(catalogs)-1} publisher catalogs (anonymous-read) → {BASE_URI}/<catalog>")


def main():
    if STAGING.exists(): shutil.rmtree(STAGING)
    # 1) NLS vector data tables (v2 = WKB/DuckDB, v3 = native geom)
    data_meta = {}
    for name, info in DATASETS.items():
        data_meta[f"v2/{name}"] = build_v2(name, info)
        data_meta[f"v3/{name}"] = build_v3(name, info)
        print(f"built v2+v3: {name}")

    # 2) STAC indexes — one combined (for the `sdi` catalog / web-app) + one per publisher
    R = collect_rows()
    all_idx = list(range(len(R["id"])))
    by_pub = lambda p: [i for i in all_idx if R["publisher"][i] == p]
    combined_meta, n_all = write_index(R, all_idx, "catalog/datasets", "combined",
                                       "STAC catalog index — all publishers (stac-geoparquet)")
    nls_meta, n_nls = write_index(R, by_pub("national-land-survey"), "idx/nls", "publisher",
                                  "National Land Survey of Finland — STAC index")
    syke_meta, n_syke = write_index(R, by_pub("finnish-environment-institute"), "idx/syke", "publisher",
                                    "Finnish Environment Institute (SYKE) — STAC index")
    cop_meta, n_cop = write_index(R, by_pub("copernicus"), "idx/cop", "publisher",
                                  "Copernicus (EU) — STAC index")
    print(f"indexes: combined {n_all} · NLS {n_nls} · SYKE {n_syke} · Copernicus {n_cop}")

    # 3) IRC surfaces
    vtables = [(k.split("/")[0], k.split("/")[1], m, k) for k, m in data_meta.items()]  # v2/v3 tables
    combined = make_surface(vtables + [("catalog", "datasets", combined_meta, "catalog/datasets")])
    nls = make_surface(vtables + [("catalog", "datasets", nls_meta, "idx/nls")])
    syke = make_surface([("catalog", "datasets", syke_meta, "idx/syke")])
    cop = make_surface([("catalog", "datasets", cop_meta, "idx/cop")])
    catalogs = {"": combined, "national-land-survey": nls,
                "finnish-environment-institute": syke, "copernicus": cop}

    for cname, surf in catalogs.items():
        d = STAGING / "_surface" / (cname or "_combined")
        d.mkdir(parents=True, exist_ok=True)
        for k, v in surf.items():
            (d / (k.replace("/", "__") + ".json")).write_text(v)
    print(f"staged combined + 3 publisher catalogs ({len(data_meta)} data tables) under {STAGING}")
    if "--publish" in sys.argv:
        publish(catalogs)


if __name__ == "__main__":
    main()
