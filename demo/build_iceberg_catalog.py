#!/usr/bin/env python3
"""Build a static Iceberg REST catalog of Helsinki-region open-data datasets on
UpCloud. Three layers:
  - v2/* : GeoIceberg V2 vector tables (WKB + flat bbox) — DuckDB-portable
  - v3/* : native V3 geometry vector tables (geoarrow) — Snowflake/CARTO
  - catalog.datasets : a stac-geoparquet (STAC-in-Iceberg) index listing ONLY the
    datasets with cloud-native data actually published (assets.data.href). Sources
    we haven't materialized are not catalogued — the index is what you can query.
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
from pyiceberg.types import (NestedField, StringType, IntegerType, LongType, FloatType,
                             DoubleType, BooleanType, BinaryType, StructType, TimestamptzType)

TESTBED = Path("/Users/jatorre/workspace/iceberg-geo-testbed")
sys.path.insert(0, str(TESTBED))
from testbed._static_catalog import write_static_catalog  # noqa: E402

BUCKET = "carto-ogc-connect-helsinki"
PREFIX = "catalog"
BASE_URI = f"https://8et4c.upcloudobjects.com/{BUCKET}/{PREFIX}"
EXTRA = f"{BASE_URI}/data/extra"   # materialized cloud-native GeoParquet files (gpio output, native geom)
IRC_PREFIX = "sdi"
STAGING = Path("/tmp/sdi_catalog")
SRC = Path("/tmp")
CONV = Path("/tmp/sdi_convert")   # scratch for GeoParquet→Iceberg normalization
DUCKDB = "duckdb"                  # CLI (1.5.3) used to normalize source GeoParquet
GEOM_EXT = ga.wkb().with_crs(ga.OGC_CRS84)

def dle(v): return struct.pack("<d", float(v))
def xy(x, y): return struct.pack("<dd", float(x), float(y))
def _fmeta(i): return {"PARQUET:field_id": str(i)}

DATASETS = {
    "power_lines": dict(
        title="Electricity transmission lines (NLS Topographic Database)", theme="energy / grid",
        semantics=dict(describes="Overhead high-voltage electricity transmission lines (NLS).",
                       answers=["distance to electricity transmission lines", "power infrastructure mapping"],
                       geometry="LineString")),
    "protected": dict(
        title="Protected nature areas (NLS luonnonsuojelualue)", theme="environment / nature conservation",
        semantics=dict(describes="Statutory nature protection areas (NLS).",
                       answers=["distance to protected areas", "nature conservation"],
                       geometry="Polygon")),
    "water": dict(
        title="Water bodies / lakes (NLS jarvi)", theme="environment / water",
        semantics=dict(describes="Inland water bodies / lakes (NLS).",
                       answers=["distance to inland waters", "hydrology / shoreline"],
                       geometry="Polygon")),
}
COLS = ["id", "kohderyhma", "kohdeluokka", "fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax", "geom_wkb"]

# Per-column documentation, written into the Iceberg schema field `doc` (the standard,
# self-describing place for column metadata — readable by any Iceberg client via loadTable).
VEC_DOC = {
 "id": "Source feature identifier (NLS mtk_id).",
 "kohderyhma": "NLS feature-group code (kohderyhmä).",
 "kohdeluokka": "NLS feature-class code (kohdeluokka).",
 "fp_xmin": "Feature bounding-box minimum longitude, WGS84 (used for spatial pruning).",
 "fp_ymin": "Feature bounding-box minimum latitude, WGS84 (used for spatial pruning).",
 "fp_xmax": "Feature bounding-box maximum longitude, WGS84 (used for spatial pruning).",
 "fp_ymax": "Feature bounding-box maximum latitude, WGS84 (used for spatial pruning).",
 "geom_wkb": "Geometry — WKB encoding, CRS OGC:CRS84 (EPSG:4326).",
 "geom": "Geometry — native geoarrow encoding, CRS OGC:CRS84 (EPSG:4326).",
}
IDX_DOC = {
 "id": "STAC item id (the dataset identifier).",
 "collection": "STAC collection — the publisher / theme grouping.",
 "geometry": "Dataset spatial footprint, WKB (OGC:CRS84).",
 "bbox": "Dataset bounding box (WGS84).",
 "xmin": "Bounding-box minimum longitude (WGS84).", "ymin": "Bounding-box minimum latitude (WGS84).",
 "xmax": "Bounding-box maximum longitude (WGS84).", "ymax": "Bounding-box maximum latitude (WGS84).",
 "datetime": "STAC item datetime (null if the dataset has no single timestamp).",
 "properties": "STAC properties (JSON): title, description, keywords, crs, materialized, provider, license, OSI semantics, and query_hint where the access pattern is non-obvious.",
 "assets": "STAC assets (JSON): data href (an Iceberg table reference or a cloud-native file URL) and type.",
 "stac_version": "STAC specification version.",
 "type": "STAC item type (Feature).",
}
def _annotate(meta, docs):
    """Write per-column `doc` into an Iceberg table-metadata dict, by field name (incl. nested struct fields)."""
    def walk(fields):
        for fld in fields:
            if fld.get("name") in docs:
                fld["doc"] = docs[fld["name"]]
            t = fld.get("type")
            if isinstance(t, dict) and t.get("type") == "struct":
                walk(t.get("fields", []))
    for sc in meta.get("schemas", []):
        walk(sc.get("fields", []))
    return meta
def _finalize(mp, docs):
    """Annotate the table metadata with per-column docs AND write it back to the staged
    metadata.json so the published file (not just the inline IRC surface) carries them."""
    meta = _annotate(json.loads(Path(mp).read_text()), docs)
    Path(mp).write_text(json.dumps(meta))
    return meta


def _ext(t):
    return (pc.min(t["fp_xmin"]).as_py(), pc.min(t["fp_ymin"]).as_py(),
            pc.max(t["fp_xmax"]).as_py(), pc.max(t["fp_ymax"]).as_py())
def _props(info, variant):
    # vector tables carry only semantics (what they are) + geo metadata; the agent writes
    # its own spatial SQL — no query recipe shipped for these straightforward cases.
    return {"theme": info["theme"], "title": info["title"], "semantics": json.dumps(info["semantics"])}


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
    return _finalize(mp, VEC_DOC)


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
    return _finalize(mp, VEC_DOC)


# --- generic GeoParquet → Iceberg conversion --------------------------------
# When WE transform a source, we go all the way to Iceberg tables (v2 WKB + v3 native),
# exactly like the NLS vectors — never a half-way GeoParquet file. (A source that is
# already cloud-native GeoParquet, e.g. Overture, is left as a remote GeoParquet.)
def _ice_field(field, fid):
    """Map a pyarrow field → (pyiceberg NestedField, Iceberg JSON field, 'iceberg-type')."""
    t = field.type
    if pa.types.is_boolean(t):                     it, js = BooleanType(), "boolean"
    elif pa.types.is_int64(t):                      it, js = LongType(), "long"
    elif pa.types.is_integer(t):                    it, js = IntegerType(), "int"
    elif pa.types.is_float64(t):                   it, js = DoubleType(), "double"
    elif pa.types.is_float32(t):                    it, js = FloatType(), "float"
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t): it, js = BinaryType(), "binary"
    else:                                          it, js = StringType(), "string"  # incl. temporal (stringified)
    return (NestedField(fid, field.name, it, required=False),
            {"id": fid, "name": field.name, "required": False, "type": js})

def _normalize(name, src_url, geom):
    """Use the DuckDB CLI to read the source (Geo)Parquet and write a normalized parquet:
    for geometry sources → attributes + geom_wkb (WKB) + flat fp_* bbox; for non-spatial →
    the columns as-is. Then load it with pyarrow, drop the OGC_FID export artifact, and
    stringify temporal columns (keeps the static-catalog writer to a simple type set)."""
    CONV.mkdir(parents=True, exist_ok=True)
    out = CONV / f"{name}.parquet"
    if geom:
        sel = ("SELECT * EXCLUDE(geom, bbox), ST_AsWKB(geom) AS geom_wkb, "
               "bbox.xmin AS fp_xmin, bbox.ymin AS fp_ymin, bbox.xmax AS fp_xmax, bbox.ymax AS fp_ymax")
    else:
        sel = "SELECT *"
    sql = (f"INSTALL spatial;LOAD spatial;INSTALL httpfs;LOAD httpfs;"
           f"COPY ({sel} FROM read_parquet('{src_url}')) TO '{out}' (FORMAT parquet);")
    subprocess.run([DUCKDB, "-c", sql], check=True, capture_output=True)
    t = pq.read_table(out)
    if "OGC_FID" in t.column_names:
        t = t.drop(["OGC_FID"])
    cast = {f.name: pc.cast(t[f.name], pa.string()) for f in t.schema if pa.types.is_temporal(f.type)}
    for n, col in cast.items():
        t = t.set_column(t.schema.get_field_index(n), pa.field(n, pa.string()), col)
    return t

def _semprops(info):
    return {"theme": info["theme"], "title": info["title"], "semantics": json.dumps(info["semantics"])}

def build_geo_generic(name, src_url, info, docs):
    """Build BOTH v2 (WKB + flat bbox) and v3 (native geoarrow geom) Iceberg tables from a
    source GeoParquet, preserving every attribute column. Returns (v2_meta, v3_meta)."""
    t = _normalize(name, src_url, geom=True)
    attr = [c for c in t.column_names if c not in ("geom_wkb", "fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax")]
    # ---- v2: attributes + geom_wkb + fp_* (column order = attrs, geom_wkb, fp_*) ----
    v2_cols = attr + ["geom_wkb", "fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax"]
    v2t = t.select(v2_cols)
    ice, fields, namemap = [], [], []
    for i, n in enumerate(v2_cols, 1):
        f = v2t.schema.field(n)
        nf, jf = _ice_field(f, i)
        ice.append(nf); fields.append(jf); namemap.append({"field-id": i, "names": [n]})
    fid = {n: i for i, n in enumerate(v2_cols, 1)}
    root = STAGING / "data" / "v2" / name; (root / "data").mkdir(parents=True, exist_ok=True)
    pqpath = root / "data" / f"{name}.parquet"
    # re-id the arrow schema field metadata so PARQUET:field_id matches the Iceberg ids
    v2t = v2t.replace_schema_metadata(None).cast(pa.schema(
        [pa.field(n, v2t.schema.field(n).type, metadata=_fmeta(fid[n])) for n in v2_cols]))
    pq.write_table(v2t, pqpath, compression="zstd")
    geo = {"version": "1.0", "primary_column": "geom_wkb", "columns": {"geom_wkb": {
        "encoding": "WKB", "crs": "OGC:CRS84", "edges": "planar",
        "bbox_columns": ["fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax"]}}}
    lo = {fid[c]: dle(pc.min(t[c]).as_py()) for c in ("fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax")}
    up = {fid[c]: dle(pc.max(t[c]).as_py()) for c in ("fp_xmin", "fp_ymin", "fp_xmax", "fp_ymax")}
    df = [{"path": f"data/{name}.parquet", "size": pqpath.stat().st_size, "rows": t.num_rows, "lower": lo, "upper": up}]
    v2mp = write_static_catalog(table_root=root, iceberg_schema=Schema(*ice), schema_json_fields=fields,
                                name_mapping=namemap, data_files=df, format_version_in_metadata=2,
                                location_uri=f"{BASE_URI}/data/v2/{name}",
                                extra_properties={"geo": json.dumps(geo), **_semprops(info)})
    v2_meta = _finalize(v2mp, docs)
    # ---- v3: attributes + native geom (geoarrow WKB extension type) ----
    v3_cols = attr + ["geom"]
    arrays = {c: t[c] for c in attr}
    arrays["geom"] = GEOM_EXT.wrap_array(t["geom_wkb"].combine_chunks())
    ice, fields, namemap = [], [], []
    for i, n in enumerate(v3_cols, 1):
        if n == "geom":
            ice.append(NestedField(i, "geom", BinaryType(), required=False))
            fields.append({"id": i, "name": "geom", "required": False, "type": "geometry"})
        else:
            nf, jf = _ice_field(v2t.schema.field(n), i); ice.append(nf); fields.append(jf)
        namemap.append({"field-id": i, "names": [n]})
    fid3 = {n: i for i, n in enumerate(v3_cols, 1)}
    v3schema = pa.schema([pa.field(n, (GEOM_EXT if n == "geom" else t.schema.field(n).type),
                                    metadata=_fmeta(fid3[n])) for n in v3_cols])
    v3t = pa.table({n: arrays[n] for n in v3_cols}, schema=v3schema)
    root3 = STAGING / "data" / "v3" / name; (root3 / "data").mkdir(parents=True, exist_ok=True)
    pq3 = root3 / "data" / f"{name}.parquet"
    pq.write_table(v3t, pq3, compression="zstd", store_schema=True, write_statistics=True)
    x0, y0 = pc.min(t["fp_xmin"]).as_py(), pc.min(t["fp_ymin"]).as_py()
    x1, y1 = pc.max(t["fp_xmax"]).as_py(), pc.max(t["fp_ymax"]).as_py()
    g = fid3["geom"]
    df3 = [{"path": f"data/{name}.parquet", "size": pq3.stat().st_size, "rows": t.num_rows,
            "lower": {g: xy(x0, y0)}, "upper": {g: xy(x1, y1)},
            "value_counts": {g: t.num_rows}, "null_value_counts": {g: 0}}]
    v3mp = write_static_catalog(table_root=root3, iceberg_schema=Schema(*ice), schema_json_fields=fields,
                                name_mapping=namemap, data_files=df3, format_version_in_metadata=3,
                                location_uri=f"{BASE_URI}/data/v3/{name}", extra_properties=_semprops(info))
    return v2_meta, _finalize(v3mp, docs)

def build_tab_generic(name, src_url, info, docs):
    """Build a NON-spatial Iceberg table (no geometry) from a source parquet → `tab` namespace."""
    t = _normalize(name, src_url, geom=False)
    cols = t.column_names
    ice, fields, namemap = [], [], []
    for i, n in enumerate(cols, 1):
        nf, jf = _ice_field(t.schema.field(n), i); ice.append(nf); fields.append(jf)
        namemap.append({"field-id": i, "names": [n]})
    fid = {n: i for i, n in enumerate(cols, 1)}
    t = t.cast(pa.schema([pa.field(n, t.schema.field(n).type, metadata=_fmeta(fid[n])) for n in cols]))
    root = STAGING / "data" / "tab" / name; (root / "data").mkdir(parents=True, exist_ok=True)
    pqpath = root / "data" / f"{name}.parquet"
    pq.write_table(t, pqpath, compression="zstd")
    df = [{"path": f"data/{name}.parquet", "size": pqpath.stat().st_size, "rows": t.num_rows, "lower": {}, "upper": {}}]
    mp = write_static_catalog(table_root=root, iceberg_schema=Schema(*ice), schema_json_fields=fields,
                              name_mapping=namemap, data_files=df, format_version_in_metadata=2,
                              location_uri=f"{BASE_URI}/data/tab/{name}", extra_properties=_semprops(info))
    return _finalize(mp, docs)


# --- catalog.datasets : stac-geoparquet (STAC Items in Iceberg) -------------
# Federation by real PUBLISHER. Each publisher converts ITS OWN data, so each gets its
# OWN filtered STAC index. Materialized vector data is a base table name (power_lines/
# protected/water); the href is rendered per scope — combined catalog (attached as
# `sdi`) uses `sdi.v2.<t>`, a publisher catalog uses `v2.<t>` (relative to its alias).
MATERIALIZED = {  # NLS id -> (v2 table base, format, recipe, keywords)
    "sahkolinja": ("power_lines", "geoparquet",
                   "ST_Distance to ST_GeomFromWKB(geom_wkb) in EPSG:3067", "electricity transmission; power grid; energy infrastructure"),
    "luonnonsuojelualue": ("protected", "geoparquet",
                   "ST_Distance to protected polygons in EPSG:3067", "protected areas; nature conservation; environment"),
    "jarvi": ("water", "geoparquet",
                   "ST_Distance to lakes in EPSG:3067", "lakes; water bodies; hydrology"),
}
RELEVANT = {"tieviiva": "roads; access",
            "muuntaja": "transformer/substation; grid", "korkeuskayra": "contours; terrain"}

# Sources WE transform → full Iceberg tables (we go all the way; never half-way GeoParquet
# files). id -> (table name, source (Geo)Parquet url, geom?). Geo → v2+v3; non-geo → `tab`.
CONVERT = {
    "rakennus":               ("buildings",          f"{EXTRA}/buildings.parquet",          True),
    "tulvavaarakartta":       ("flood_hazard",       f"{EXTRA}/flood_hazard.parquet",       True),
    "natura2000":             ("natura2000",         f"{EXTRA}/natura2000.parquet",         True),
    "pohjavesialue":          ("groundwater",        f"{EXTRA}/groundwater.parquet",        True),
    "urbanatlas":             ("urbanatlas",         f"{EXTRA}/urbanatlas.parquet",         True),
    "seuturamava_kortteli":   ("hsy_zoning",         f"{EXTRA}/hsy_zoning.parquet",         True),
    "paavo_vaesto":           ("statfi_paavo",       f"{EXTRA}/statfi_paavo.parquet",       True),
    "vaestoruutu_1km":        ("statfi_popgrid",     f"{EXTRA}/statfi_popgrid.parquet",     True),
    "ykr_urban_structure":    ("lf_ykr",             f"{EXTRA}/lf_ykr.parquet",             True),
    "electricity_consumption":("fingrid_consumption",f"{EXTRA}/fingrid_consumption.parquet",False),
    "electricity_prices":     ("eurostat_elec",      f"{EXTRA}/eurostat_elec.parquet",      False),
}
def tbl_mat(stac_id):
    """mat tuple for a dataset we converted to an Iceberg table."""
    name, _src, geom = CONVERT[stac_id]
    return ("table", name, "geoparquet" if geom else "parquet")

# Overture is already cloud-native GeoParquet at planet scale → left as a REMOTE GeoParquet
# on Overture's public bucket (queried in place; the S3/secret/hive/bbox access is the one
# tricky bit we ship a query_hint for).
OVERTURE_HREF = "s3://overturemaps-us-west-2/release/2026-05-20.0/theme=places/type=place/*"

# Per-column docs for the converted tables (Iceberg field `doc`). Shared geo/key columns
# below; the rest carry their source field name (already descriptive) with no extra doc.
SHARED_DOC = {
    "id": "Source feature identifier.",
    "geom_wkb": "Geometry — WKB encoding, CRS OGC:CRS84 (EPSG:4326).",
    "geom": "Geometry — native geoarrow encoding, CRS OGC:CRS84 (EPSG:4326).",
    "fp_xmin": "Feature bounding-box minimum longitude, WGS84 (spatial pruning).",
    "fp_ymin": "Feature bounding-box minimum latitude, WGS84 (spatial pruning).",
    "fp_xmax": "Feature bounding-box maximum longitude, WGS84 (spatial pruning).",
    "fp_ymax": "Feature bounding-box maximum latitude, WGS84 (spatial pruning).",
}
TBL_DOC = {
    "buildings": {"mtk_id": "NLS building identifier.", "kohdeluokka": "NLS feature-class code.",
                  "kerrosluku": "Number of storeys.", "kayttotarkoitus": "Building use code.",
                  "alkupvm": "Record start date."},
    "flood_hazard": {"tulvasuojtoistuvuus": "Flood return period (years).", "area_m2": "Zone area (m²).",
                     "perimeter_m": "Zone perimeter (m)."},
    "natura2000": {"naturaTunnus": "Natura 2000 site code.", "nimiSuomi": "Site name (Finnish).",
                   "alueTyyppi": "Site type (SAC habitats / SPA birds).", "paatosPAla_ha": "Designated area (ha)."},
    "groundwater": {"pvaluenimi": "Groundwater area name.", "pvalueluokka": "Classification class.",
                    "kunta": "Municipality.", "tilamaara": "Quantitative status.", "area_m2": "Area (m²)."},
    "urbanatlas": {"class_2018": "Urban Atlas 2018 land-use class.", "code_2018": "Urban Atlas 2018 class code."},
    "hsy_zoning": {"kunta": "Municipality.", "korttunnus": "Plan-block identifier.",
                   "rakerayht": "Built floor area, total (m²).", "laskvar_yh": "Unused building-rights reserve, total (m²).",
                   "laskvar_t": "Reserve, industrial use T (m²).", "laskvar_ak": "Reserve, blocks-of-flats AK (m²).",
                   "laskvar_ap": "Reserve, low-rise residential AP (m²)."},
    "statfi_paavo": {"nimi": "Postal area name.", "postinumeroalue": "Postal code.", "he_vakiy": "Resident population.",
                     "hr_mtu": "Median income (EUR).", "tp_tyopy": "Jobs (workplaces).", "pt_tyott": "Unemployed persons."},
    "statfi_popgrid": {"vaesto": "Inhabitants in the 1 km cell.", "ika_0_14": "Age 0–14.",
                       "ika_15_64": "Age 15–64.", "ika_65_": "Age 65+.", "kunta": "Municipality."},
    "lf_ykr": {"Luokka": "YKR urban-structure zone class (1 = inner urban → higher = more peripheral)."},
    "fingrid_consumption": {"start_time": "Interval start (UTC).", "consumption_mw": "National electricity load (MW)."},
    "eurostat_elec": {"geo": "Country code.", "country": "Country name.", "period": "Reporting period.",
                      "price_eur_per_kwh": "Industrial electricity price incl. taxes (EUR/kWh)."},
}

# collection prefix -> publisher sub-catalog (who converted it)
def publisher_of(collection):
    if collection.startswith("nls"):
        return "national-land-survey"
    if collection == "syke":
        return "finnish-environment-institute"
    if collection.startswith("hsy"):
        return "helsinki-region-hsy"
    if collection.startswith("statfi") or collection.startswith("tilastokeskus"):
        return "statistics-finland"
    if collection.startswith("fingrid"):
        return "fingrid"
    if collection.startswith("lf"):
        return "location-finland"
    if collection.startswith("overture"):
        return "overture-maps"
    if collection.startswith("eurostat"):
        return "eurostat"
    return "copernicus"

# publisher -> (endpoint sub-path, idx storage slug). The combined catalog is at the root.
PUBLISHERS = {
    "national-land-survey":          ("national-land-survey", "nls"),
    "copernicus":                    ("copernicus", "cop"),
    "finnish-environment-institute": ("finnish-environment-institute", "syke"),
    "helsinki-region-hsy":           ("helsinki-region-hsy", "hsy"),
    "statistics-finland":            ("statistics-finland", "statfi"),
    "fingrid":                       ("fingrid", "fingrid"),
    "location-finland":              ("location-finland", "lf"),
    "overture-maps":                 ("overture-maps", "overture"),
    "eurostat":                      ("eurostat", "eurostat"),
}
PROVIDER = {"national-land-survey":"National Land Survey of Finland","finnish-environment-institute":"Finnish Environment Institute (SYKE)","copernicus":"Copernicus / European Union","helsinki-region-hsy":"Helsinki Region Environmental Services (HSY)","statistics-finland":"Statistics Finland","fingrid":"Fingrid","location-finland":"Location Finland","overture-maps":"Overture Maps Foundation","eurostat":"Eurostat"}
LICENSE = {"overture-maps":"CDLA-Permissive-2.0 / ODbL","copernicus":"Copernicus open licence","eurostat":"Eurostat licence"}


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
    # Hints are ONLY for tricky cloud-native access patterns the agent wouldn't reasonably
    # guess: raquet rasters (block/quadbin sampling) and the remote Overture GeoParquet
    # (S3 secret + hive partitioning + bbox prune). Iceberg vector/non-spatial tables ship
    # NO query — the agent composes its own SQL from the schema + geo-metadata + OSI semantics.
    if not mat:
        return None
    kind, val, fmt = mat
    if kind == "overture":  # remote GeoParquet on Overture's public S3 — read in place
        return ("CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-west-2'); "
                f"SELECT count(*) AS places_within_1km FROM read_parquet('{val}', hive_partitioning=1) "
                "WHERE bbox.xmin BETWEEN :lon-0.03 AND :lon+0.03 AND bbox.ymin BETWEEN :lat-0.03 AND :lat+0.03 "
                "AND ST_DWithin(ST_Transform(geometry,'EPSG:4326','EPSG:3067'),"
                "ST_Transform(ST_Point(:lon,:lat),'EPSG:4326','EPSG:3067'), 1000);")
    if fmt != "raquet":
        return None
    if kind == "climate":  # raster (raquet) point-sample of a climate variable, averaged over a small grid
        return (_GRID + "SELECT round(avg(t),1) AS mean_annual_temp_c FROM ("
                "SELECT ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) t "
                f"FROM grid g, read_raquet('{val}') r "
                "WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat))) WHERE t > -9999;")
    if "ndvi" in val:  # raster (raquet) — NDVI = (NIR-Red)/(NIR+Red)
        return _GRID + ("SELECT avg((b2-b1)/(b2+b1)) AS ndvi FROM ("
                        "SELECT ST_RasterValue(r.block,r.band_2,ST_Point(g.lon,g.lat),r.metadata) b2,"
                        "ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) b1 "
                        f"FROM grid g, read_raquet('{val}') r "
                        "WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)) AND r.band_1 IS NOT NULL);")
    return _GRID + ("SELECT min(elev) AS min_m, stddev(elev) AS slope_sigma FROM ("  # DEM (2 m laser)
                    "SELECT ST_RasterValue(r.block,r.band_1,ST_Point(g.lon,g.lat),r.metadata) elev "
                    f"FROM grid g, read_raquet('{val}') r "
                    "WHERE ST_Contains(ST_GeomFromQuadbin(r.block),ST_Point(g.lon,g.lat)));")


# OSI — Open Semantic Interchange: machine-readable semantics so an agent understands what
# a dataset MEANS and what it answers, not just its schema. id -> (label, describes, answers, unit)
SEM = {
 "sahkolinja":("Electricity transmission lines","High-voltage overhead power lines (NLS Topographic Database).","distance to electricity transmission lines","metres"),
 "jarvi":("Lakes & water bodies","Inland lakes and water bodies (NLS).","distance to inland water bodies","metres"),
 "luonnonsuojelualue":("Protected nature areas","Statutory nature-protection areas (NLS).","distance to protected areas","metres"),
 "korkeusmalli_2m":("Elevation model — 2 m laser DEM","NLS 2 m laser-scanned terrain elevation.","terrain elevation & slope","metres"),
 "ndvi":("Sentinel-2 NDVI","Copernicus Sentinel-2 vegetation index.","vegetation / forest cover","index (−1..1)"),
 "urbanatlas":("Urban Atlas land use","Copernicus Urban Atlas 2018 land-use class per parcel.","land-use classification","class"),
 "tulvavaarakartta":("Flood-hazard zones","SYKE flood inundation zones by return period.","flood risk","metres / return period"),
 "natura2000":("Natura 2000 sites","EU Natura 2000 protected network (SAC + SPA), via SYKE.","protected nature","metres"),
 "pohjavesialue":("Classified groundwater areas","SYKE classified groundwater areas (VHS2022).","distance to classified groundwater areas","metres"),
 "rakennus":("Buildings","NLS building footprints.","distance to buildings","metres"),
 "seuturamava_kortteli":("Zoning · building-rights reserve","HSY per-plan-block land-use category and unused building-rights reserve.","permitted land use & building-rights reserve","m² floor area"),
 "temperature":("Mean annual air temperature","Location Finland climate coverage (°C).","mean annual air temperature","°C"),
 "paavo_vaesto":("Postal-area demographics","Statistics Finland Paavo: population, income, employment, age.","population, income & employment by postal area","persons / EUR"),
 "vaestoruutu_1km":("Population grid · 1 km","Statistics Finland inhabitants per 1 km cell.","population per 1 km cell","persons"),
 "ykr_urban_structure":("Urban-structure zones (YKR)","Location Finland settlement-structure classification.","settlement-structure classification","class"),
 "electricity_consumption":("Electricity consumption (national grid load)","Fingrid national electricity load, 15-min values in MW.","national electricity demand / load","MW"),
 "electricity_prices":("Industrial electricity price","Eurostat industrial electricity price by country (incl. taxes).","energy cost","EUR / kWh"),
 "places":("Points of interest","Overture global places (cloud-native, planet scale).","amenities & activity","count"),
}

def collect_rows():
    """All STAC items as a row dict R, plus a parallel `publisher` list and a `mat`
    list of (kind, value, format) — kind in {'table','url',None} — so each index can
    render assets/hrefs for its own scope."""
    cols = json.loads(Path("/tmp/nls_all.json").read_text())["collections"]
    R = {k: [] for k in ("id", "collection", "geometry", "xmin", "ymin", "xmax", "ymax",
                         "datetime", "properties", "assets_recipe", "stac_version", "type",
                         "publisher", "mat")}
    def add(cid, coll, title, desc, item_type, bbox, crs, mat, recipe, kw):
        # mat: ('table','power_lines','geoparquet') | ('url',<href>,'raquet')
        # We only catalog what is actually accessible: a dataset with no cloud-native
        # data published (mat is None) is not listed at all.
        if mat is None:
            return
        x0, y0, x1, y1 = (list(bbox) + [None] * 4)[:4] if bbox else (None, None, None, None)
        R["id"].append(cid); R["collection"].append(coll)
        R["geometry"].append(_wkb_box(x0, y0, x1, y1))
        R["xmin"].append(x0); R["ymin"].append(y0); R["xmax"].append(x1); R["ymax"].append(y1)
        R["datetime"].append(None)
        sem = SEM.get(cid)
        tricky = bool(mat) and (mat[2] == "raquet" or mat[0] == "overture")  # access patterns we hint
        props = {
            "title": (sem[0] if sem else title), "description": ((sem[1] if sem else desc) or "")[:400],
            "keywords": kw.split("; ") if kw else [], "item_type": item_type, "crs": crs,
            "materialized": mat is not None, "data_format": (mat[2] if mat else None),
            "provider": PROVIDER.get(publisher_of(coll), publisher_of(coll)),
            "license": LICENSE.get(publisher_of(coll), "CC-BY-4.0")}
        if sem:  # Open Semantic Interchange (OSI) block — what it means / answers / unit
            props["semantics"] = {"spec": "Open Semantic Interchange", "label": sem[0],
                                  "describes": sem[1], "answers": sem[2], "unit": sem[3]}
        if tricky:  # raquet raster or remote Overture — ship a hint the agent wouldn't guess
            if recipe:
                props["access_recipe"] = recipe
            props["query_hint"] = _example_query(mat)
        # Iceberg tables carry NO query: the agent composes it from schema + geo + semantics
        R["properties"].append(json.dumps(props))
        R["stac_version"].append("1.1.0"); R["type"].append("Feature")
        R["publisher"].append(publisher_of(coll)); R["mat"].append(mat)
    for c in cols:
        sp = (((c.get("extent") or {}).get("spatial") or {}).get("bbox") or [None])
        bbox = sp[0] if (sp and isinstance(sp[0], list)) else None
        m = MATERIALIZED.get(c["id"])
        if m:
            mat, recipe, kw = ("table", m[0], m[1]), m[2], m[3]
        elif c["id"] in CONVERT:
            mat, recipe, kw = tbl_mat(c["id"]), None, "buildings; building footprints; built environment"
        else:
            mat, recipe, kw = None, None, RELEVANT.get(c["id"], "")
        add(c["id"], "nls-topographic", c.get("title"), c.get("description"), c.get("itemType"),
            bbox, "OGC:CRS84", mat, recipe, kw)
    add("korkeusmalli_2m", "nls-elevation", "Elevation model 2 m (laser DEM)",
        "NLS 2 m laser-scanned terrain elevation model.", "coverage",
        [24.62, 60.21, 24.76, 60.27], "EPSG:3067",
        ("url", f"{BASE_URI}/data/raster/dem_2m.parquet", "raquet"),
        "read_raquet + ST_RasterValue grid-sample (finest zoom via ST_GeomFromQuadbin)",
        "elevation; terrain; slope; digital elevation model")
    add("ndvi", "copernicus-sentinel2", "Sentinel-2 NDVI (red+NIR)",
        "Copernicus Sentinel-2 red/NIR for NDVI — vegetation/forest that would be cleared.", "coverage",
        [24.62, 60.21, 24.76, 60.27], "EPSG:3857",
        ("url", f"{BASE_URI}/data/raster/ndvi.parquet", "raquet"),
        "read_raquet; NDVI=(band_2-band_1)/(band_2+band_1)",
        "vegetation; NDVI; land cover; remote sensing")
    add("urbanatlas", "copernicus-urbanatlas", "Urban Atlas 2018 land use (Helsinki FUA)",
        "Copernicus Urban Atlas 2018 land-use polygons for the Helsinki Functional Urban Area — the authoritative land-use class per parcel (e.g. Forests, Discontinuous urban fabric, Industrial).",
        "feature", [24.15, 60.08, 24.80, 60.40], "OGC:CRS84", tbl_mat("urbanatlas"), None,
        "land use; land cover; urban atlas; copernicus")
    # SYKE — Finnish Environment Institute. Flood + Natura + groundwater MATERIALIZED
    # (clipped to the Helsinki-region AOI from SYKE's OGC API Features). CORINE is not materialized, so not listed.
    AOI = [24.15, 60.08, 24.80, 60.40]
    add("tulvavaarakartta", "syke", "Flood hazard zones (basic scenarios)",
        "SYKE flood-hazard inundation zones by return period (tulvavaaravyöhykkeet, perusskenaariot), clipped to the Helsinki-region AOI.",
        "feature", AOI, "OGC:CRS84", tbl_mat("tulvavaarakartta"), None,
        "flood risk; flood hazard")
    add("natura2000", "syke", "Natura 2000 protected areas (SAC + SPA)",
        "EU Natura 2000 network — habitats (SAC) + birds (SPA) directive sites in the AOI, published by SYKE.",
        "feature", AOI, "OGC:CRS84", tbl_mat("natura2000"), None,
        "environmental constraint; protected; natura")
    add("pohjavesialue", "syke", "Groundwater areas (classified, VHS2022)",
        "SYKE classified groundwater areas (pohjavesialueet, VHS2022), delineated for water-supply protection.",
        "feature", AOI, "OGC:CRS84", tbl_mat("pohjavesialue"), None,
        "groundwater; aquifer; water supply; environment")
    add("corine-land-cover", "syke", "CORINE Land Cover 2018",
        "Pan-European CORINE land cover for Finland, published by SYKE.", "coverage", AOI, "OGC:CRS84", None, None,
        "land cover; vegetation")
    # HSY (Helsinki Region Environmental Services) — regional planning. SeutuRAMAVA = per
    # detailed-plan-block land-use category + built vs unused building-rights reserve.
    add("seuturamava_kortteli", "hsy-maankaytto", "Zoning / building-rights reserve by plan block (SeutuRAMAVA)",
        "HSY regional building-land reserve aggregated from municipal detailed plans, per plan block: land-use category, built floor area, and unused building-rights reserve (AK/AP/K/T/Y). Covers Espoo/Vantaa/Kauniainen.",
        "feature", AOI, "OGC:CRS84", tbl_mat("seuturamava_kortteli"), None,
        "zoning; land-use plan; building rights; spatial planning")
    # Fingrid — NON-spatial: national electricity grid load (time-series). Shows the SDI is not geo-only.
    add("electricity_consumption", "fingrid-grid", "Finland electricity consumption (national grid load)",
        "Fingrid national electricity consumption, 15-min values in MW (30-day snapshot). Non-spatial time-series of national electricity demand.",
        "timeseries", None, None, tbl_mat("electricity_consumption"), None,
        "electricity; grid; capacity; power; non-spatial")
    # Statistics Finland — official statistics (Paavo postal-area demographics + 1 km population grid)
    add("paavo_vaesto", "statfi-paavo", "Postal-area demographics (Paavo 2025)",
        "Statistics Finland Paavo open data: population, income, employment and age structure per postal-code area — statistics joined to postal-area geometry.",
        "feature", AOI, "OGC:CRS84", tbl_mat("paavo_vaesto"), None,
        "demographics; population; income; employment; statistics")
    add("vaestoruutu_1km", "statfi-vaesto", "Population grid 1 km (2025)",
        "Statistics Finland 1 km population grid: inhabitants and age groups per cell.",
        "feature", AOI, "OGC:CRS84", tbl_mat("vaestoruutu_1km"), None,
        "population; population density; demographics; statistics")
    # Location Finland (national Location Innovation Hub platform, API-key gateway) — urban structure
    add("ykr_urban_structure", "lf-ykr", "Urban structure zones (YKR)",
        "Location Finland (national geospatial platform): YKR settlement/urban-structure zone classification — places the site on the urban→peripheral gradient (1 = inner urban; higher = more peripheral / rural).",
        "feature", AOI, "OGC:CRS84", tbl_mat("ykr_urban_structure"), None,
        "urban structure; settlement; land use")
    add("temperature", "lf-climate", "Mean annual air temperature (climate grid)",
        "Location Finland climate coverage: mean annual air temperature (°C). Materialized from the OGC API coverage (GeoTIFF) to cloud-native raquet via the DuckDB raquet extension.",
        "coverage", AOI, "EPSG:3067", ("climate", f"{BASE_URI}/data/raster/cli_temperature.parquet", "raquet"),
        "mean annual temperature (°C) at the site, sampled from the raquet raster",
        "climate; air temperature; meteorology")
    # Overture Maps (GLOBAL) — the same DuckDB-over-GeoParquet pattern, at planet scale.
    add("places", "overture-places", "Points of interest (Overture Maps, global)",
        "Overture Maps global places — the planet-scale, cloud-native GeoParquet catalog on Overture's public object storage, queried in place with DuckDB exactly like everything else here. POI density / activity around the site.",
        "feature", [24.15, 60.08, 24.80, 60.40], "OGC:CRS84", ("overture", OVERTURE_HREF, "geoparquet"),
        "remote GeoParquet on Overture's public S3 — read in place (S3 secret + hive partitioning + bbox prune)",
        "points of interest; supermarkets; grocery; amenities; services; activity; global")
    # Eurostat (EUROPEAN) — NON-spatial: EU-wide industrial electricity prices.
    add("electricity_prices", "eurostat-energy", "Electricity prices, industrial (Eurostat)",
        "Eurostat electricity prices for industrial consumers (band 2000–20000 MWh/yr, incl. taxes, €/kWh): Finland vs the EU-27 average, by country. Non-spatial.",
        "table", None, None, tbl_mat("electricity_prices"), None,
        "electricity price; energy cost; non-spatial; european")
    return R


def _render_assets(mat, scope):
    """scope: 'combined' -> sdi.<ns>.<t> ; 'publisher' -> <ns>.<t> (relative to alias).
    Iceberg tables resolve to a table ref (ns = v2 for geometry, tab for non-spatial);
    everything else (raquet rasters, remote Overture GeoParquet) is a direct href."""
    if mat is None:
        return json.dumps({})
    kind, val, fmt = mat
    if kind == "table":
        ns = "v2" if fmt == "geoparquet" else "tab"
        href = f"sdi.{ns}.{val}" if scope == "combined" else f"{ns}.{val}"
    else:
        href = val
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
             "semantics": json.dumps({"describes": "STAC Items for this catalog's datasets (stac-geoparquet). Every dataset listed is accessible: cloud-native data is published at assets.data.href (properties.materialized=true).",
                                      "answers": ["dataset discovery", "what data exists", "is X available"],
                                      "query_recipe": f"SELECT id, collection, assets FROM {qcat}.catalog.datasets WHERE properties ILIKE '%elevation%'"})}
    mp = write_static_catalog(table_root=root, iceberg_schema=_IDX_ICE, schema_json_fields=_IDX_FIELDS,
                              name_mapping=_IDX_NAMEMAP, data_files=[{"path": "data/datasets.parquet",
                              "size": pqpath.stat().st_size, "rows": tbl.num_rows, "lower": {}, "upper": {}}],
                              format_version_in_metadata=2, location_uri=f"{BASE_URI}/data/{storage_key}",
                              extra_properties=props, last_column_id_override=13)
    return _finalize(mp, IDX_DOC), tbl.num_rows


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


def _tbl_info(stac_id, publisher):
    """Table-level GeoIceberg properties (theme/title/semantics) from the dataset's OSI entry."""
    s = SEM.get(stac_id)
    if s:
        return dict(title=s[0], theme=publisher,
                    semantics=dict(describes=s[1], answers=[s[2]], unit=s[3]))
    return dict(title=stac_id, theme=publisher, semantics=dict(describes=stac_id))


def main():
    if STAGING.exists(): shutil.rmtree(STAGING)
    data_meta, owner = {}, {}  # storage_key -> (metadata, owning publisher)
    # 1a) NLS original vector tables (v2 = WKB/DuckDB, v3 = native geom)
    for name, info in DATASETS.items():
        data_meta[f"v2/{name}"] = build_v2(name, info)
        data_meta[f"v3/{name}"] = build_v3(name, info)
        owner[f"v2/{name}"] = owner[f"v3/{name}"] = "national-land-survey"
        print(f"built v2+v3: {name}")

    # 1b) everything WE transform → full Iceberg tables too (per CONVERT). Geo → v2+v3; non-geo → tab.
    R = collect_rows()
    pub_of_id = {R["id"][i]: R["publisher"][i] for i in range(len(R["id"]))}
    for stac_id, (name, src, geom) in CONVERT.items():
        pub = pub_of_id[stac_id]
        info, docs = _tbl_info(stac_id, pub), {**SHARED_DOC, **TBL_DOC.get(name, {})}
        if geom:
            v2m, v3m = build_geo_generic(name, src, info, docs)
            data_meta[f"v2/{name}"], data_meta[f"v3/{name}"] = v2m, v3m
            owner[f"v2/{name}"] = owner[f"v3/{name}"] = pub
            print(f"built v2+v3: {name} ({pub})")
        else:
            data_meta[f"tab/{name}"] = build_tab_generic(name, src, info, docs)
            owner[f"tab/{name}"] = pub
            print(f"built tab:   {name} ({pub})")

    # 2) STAC indexes — one combined (for the `sdi` catalog / web-app) + one per publisher
    all_idx = list(range(len(R["id"])))
    combined_meta, n_all = write_index(R, all_idx, "catalog/datasets", "combined",
                                       "STAC catalog index — all publishers (stac-geoparquet)")
    vtables = [(k.split("/")[0], k.split("/")[1], m, k) for k, m in data_meta.items()]  # all data tables
    catalogs = {"": make_surface(vtables + [("catalog", "datasets", combined_meta, "catalog/datasets")])}

    # 3) one publisher sub-catalog per distinct publisher — each carries ITS OWN tables
    summary = [f"combined {n_all}"]
    for pub in sorted(set(R["publisher"])):
        endpoint, slug = PUBLISHERS[pub]
        idxs = [i for i in all_idx if R["publisher"][i] == pub]
        meta, n = write_index(R, idxs, f"idx/{slug}", "publisher", f"{pub} — STAC index")
        owned = [(k.split("/")[0], k.split("/")[1], data_meta[k], k) for k in data_meta if owner[k] == pub]
        catalogs[endpoint] = make_surface(owned + [("catalog", "datasets", meta, f"idx/{slug}")])
        summary.append(f"{endpoint} {n}/{len(owned)}t")
    print("indexes (datasets/tables): " + " · ".join(summary))

    for cname, surf in catalogs.items():
        d = STAGING / "_surface" / (cname or "_combined")
        d.mkdir(parents=True, exist_ok=True)
        for k, v in surf.items():
            (d / (k.replace("/", "__") + ".json")).write_text(v)
    print(f"staged combined + {len(catalogs)-1} publisher catalogs ({len(data_meta)} data tables) under {STAGING}")
    if "--publish" in sys.argv:
        publish(catalogs)


if __name__ == "__main__":
    main()
