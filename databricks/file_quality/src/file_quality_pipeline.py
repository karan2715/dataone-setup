"""DataOne file data-quality pipeline (Lakeflow Spark Declarative Pipelines).

Profiles every data file that lands in the DataOne uploads volume — whether it was dropped there from the
Databricks UI / CLI or saved by DataOne's Quick Upload — and publishes one row per file to
``file_quality_results``. DataOne reads that table and shows the four dimensions on its Data Quality dashboard.

Supported formats (by extension): csv, tsv, json, jsonl / ndjson, parquet, avro, orc.

Dimensions (0-100; NULL when a dimension cannot be measured — never a made-up number). These match
``backend/app/databricks_assets/notebooks/dataone_data_quality.py`` so file and table scores are comparable:

  completeness  % of cells present (NULL / NaN / blank string = missing)
  uniqueness    % of rows that are distinct (exact-duplicate rows lower it)
  validity      % of non-blank values passing rules inferred from column names
                (email, phone, non-negative amounts/ages/counts, not-in-the-future dates)
  freshness     age of the file vs the SLA: 100 within the SLA, falling linearly to 0 at 7x the SLA
"""
import datetime as dt
import os
import re
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, Column, functions as F, types as T

LANDING_PATH = spark.conf.get("dataone.fq.landing_path").rstrip("/")  # noqa: F821  (spark is injected)
FRESHNESS_SLA_HOURS = float(spark.conf.get("dataone.fq.freshness_sla_hours", "24"))  # noqa: F821

ISSUE_COMPLETENESS_BELOW = 95.0
ISSUE_VALIDITY_BELOW = 99.0
MAX_ISSUES = 20  # DataOne ingest limit per table

# extension -> (spark format, reader options)
FORMATS = {
    ".csv": ("csv", {"header": "true", "multiLine": "true", "escape": '"'}),
    ".tsv": ("csv", {"header": "true", "multiLine": "true", "sep": "\t"}),
    ".json": ("json", {"multiLine": "true"}),
    ".jsonl": ("json", {}),
    ".ndjson": ("json", {}),
    ".parquet": ("parquet", {}),
    ".avro": ("avro", {}),
    ".orc": ("orc", {}),
}

EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_RE = r"^\+?[0-9][0-9\s\-().]{6,19}$"
NON_NEGATIVE_NAME = re.compile(r"(^|_)(amount|price|cost|qty|quantity|salary|age|count|total|revenue)(_|$)", re.I)
EMAIL_NAME = re.compile(r"e[-_]?mail", re.I)
PHONE_NAME = re.compile(r"(phone|mobile|fax)", re.I)
PAST_DATE_NAME = re.compile(r"(created|updated|modified|ingested|loaded|order|birth|hire|signup|registered)", re.I)
NUMERIC_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)

ISSUE_TYPE = T.StructType([
    T.StructField("column", T.StringType()),
    T.StructField("check", T.StringType()),
    T.StructField("detail", T.StringType()),
])
RESULT_SCHEMA = T.StructType([
    T.StructField("file_path", T.StringType()),
    T.StructField("file_name", T.StringType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("file_format", T.StringType()),
    T.StructField("size_bytes", T.LongType()),
    T.StructField("row_count", T.LongType()),
    T.StructField("column_count", T.IntegerType()),
    T.StructField("completeness", T.DoubleType()),
    T.StructField("uniqueness", T.DoubleType()),
    T.StructField("validity", T.DoubleType()),
    T.StructField("freshness", T.DoubleType()),
    T.StructField("overall_score", T.DoubleType()),
    T.StructField("last_modified", T.TimestampType()),
    T.StructField("issues", T.ArrayType(ISSUE_TYPE)),
    T.StructField("profiled_at", T.TimestampType()),
])


def q(identifier: str) -> str:
    """Backtick-quote a column name."""
    return "`" + identifier.replace("`", "``") + "`"


def table_name_for(relative_path: str) -> str:
    """``default/titanic.csv`` -> ``default.titanic``; ``titanic.csv`` -> ``titanic``."""
    parts = [p for p in relative_path.split("/") if p]
    stem = os.path.splitext(parts[-1])[0]
    clean = lambda s: re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_").lower() or "file"  # noqa: E731
    return f"{clean(parts[-2])}.{clean(stem)}" if len(parts) >= 2 else clean(stem)


def freshness_score(last_modified: dt.datetime, now: dt.datetime) -> float:
    age_h = (now - last_modified).total_seconds() / 3600
    if age_h <= FRESHNESS_SLA_HOURS:
        return 100.0
    full_decay = FRESHNESS_SLA_HOURS * 7
    if age_h >= full_decay:
        return 0.0
    return round(100.0 * (full_decay - age_h) / (full_decay - FRESHNESS_SLA_HOURS), 1)


def list_landing_files() -> list:
    """[{path, relative, ext, size, modified}] for supported files under the landing volume, oldest first."""
    found = []
    for root, _dirs, files in os.walk(LANDING_PATH):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in FORMATS or name.startswith((".", "_")):
                continue
            full = os.path.join(root, name)
            stat = os.stat(full)
            found.append({
                "path": full,
                "relative": os.path.relpath(full, LANDING_PATH),
                "ext": ext,
                "size": stat.st_size,
                "modified": dt.datetime.fromtimestamp(stat.st_mtime, tz=dt.timezone.utc),
            })
    return sorted(found, key=lambda f: f["modified"])


def present_expr(field: T.StructField) -> Column:
    """1 when the cell holds a real value, else 0 (NULL / NaN / blank string = missing)."""
    c = F.col(q(field.name))
    missing = c.isNull()
    if isinstance(field.dataType, (T.FloatType, T.DoubleType)):
        missing = missing | F.isnan(c)
    elif isinstance(field.dataType, T.StringType):
        missing = missing | (F.trim(c) == "")
    return F.when(missing, 0).otherwise(1)


def validity_rules(fields) -> list:
    """[(column, rule_name, boolean Column)] inferred from column name + type. Evaluated on present values only."""
    rules = []
    now = F.current_timestamp()
    for f in fields:
        c = F.col(q(f.name))
        if isinstance(f.dataType, T.StringType):
            if EMAIL_NAME.search(f.name):
                rules.append((f.name, "valid_email", c.rlike(EMAIL_RE)))
            elif PHONE_NAME.search(f.name):
                rules.append((f.name, "valid_phone", c.rlike(PHONE_RE)))
            elif NON_NEGATIVE_NAME.search(f.name):
                # CSV/TSV cells arrive as text: a non-numeric value in an amount/age column is invalid
                rules.append((f.name, "non_negative", F.expr(f"try_cast({q(f.name)} AS DOUBLE) >= 0")))
        elif isinstance(f.dataType, NUMERIC_TYPES) and NON_NEGATIVE_NAME.search(f.name):
            rules.append((f.name, "non_negative", c >= 0))
        elif isinstance(f.dataType, (T.DateType, T.TimestampType)) and PAST_DATE_NAME.search(f.name):
            rules.append((f.name, "not_in_future", c.cast("timestamp") <= now + F.expr("INTERVAL 1 DAY")))
    return rules


def _pct(part: Column, whole: Column) -> Column:
    return F.when(whole > 0, F.round(100.0 * part / whole, 1))


def profile_file(info: dict, now: dt.datetime) -> DataFrame:
    fmt, opts = FORMATS[info["ext"]]
    df = spark.read.format(fmt).options(**opts).load(info["path"])  # noqa: F821
    fields = df.schema.fields
    rules = validity_rules(fields)

    # One scan: row count, per-column presence and validity counters.
    aggs = [F.count(F.lit(1)).alias("rows")]
    for i, f in enumerate(fields):
        aggs.append(F.sum(present_expr(f)).alias(f"p{i}"))
    for i, (col_name, _, cond) in enumerate(rules):
        present = present_expr(next(f for f in fields if f.name == col_name)) == 1
        aggs.append(F.sum(F.when(present, 1).otherwise(0)).alias(f"vn{i}"))
        aggs.append(F.sum(F.when(present & cond, 1).otherwise(0)).alias(f"vp{i}"))
    stats = df.agg(*aggs)

    # Exact-duplicate rows. Map columns cannot be compared, so they are excluded.
    comparable = [f.name for f in fields if not isinstance(f.dataType, T.MapType)]
    if comparable:
        distinct = df.select(*[q(c) for c in comparable]).distinct().agg(F.count(F.lit(1)).alias("distinct_rows"))
        stats = stats.crossJoin(distinct)
    else:
        stats = stats.withColumn("distinct_rows", F.lit(None).cast("long"))

    rows = F.col("rows")
    n_cols = len(fields)
    present_total = reduce(lambda a, b: a + b, [F.coalesce(F.col(f"p{i}"), F.lit(0)) for i in range(n_cols)], F.lit(0))
    completeness = _pct(present_total, rows * n_cols) if n_cols else F.lit(None).cast("double")
    uniqueness = _pct(F.col("distinct_rows"), rows)

    checked = reduce(lambda a, b: a + b, [F.coalesce(F.col(f"vn{i}"), F.lit(0)) for i in range(len(rules))], F.lit(0))
    passed = reduce(lambda a, b: a + b, [F.coalesce(F.col(f"vp{i}"), F.lit(0)) for i in range(len(rules))], F.lit(0))
    validity = _pct(passed, checked) if rules else F.lit(None).cast("double")

    fresh = freshness_score(info["modified"], now)

    # Issues: worst findings, capped to what DataOne ingests.
    issue_exprs = []
    dupes = rows - F.col("distinct_rows")
    issue_exprs.append(F.when(dupes > 0, F.struct(
        F.lit(None).cast("string").alias("column"), F.lit("uniqueness").alias("check"),
        F.concat(dupes.cast("string"), F.lit(" duplicate rows")).alias("detail"))))
    if fresh < 100:
        age_h = (now - info["modified"]).total_seconds() / 3600
        issue_exprs.append(F.struct(
            F.lit(None).cast("string").alias("column"), F.lit("freshness").alias("check"),
            F.lit(f"file last modified {age_h:.0f}h ago (SLA {FRESHNESS_SLA_HOURS:.0f}h)").alias("detail")))
    for i, (col_name, rule_name, _) in enumerate(rules):
        vn, vp = F.coalesce(F.col(f"vn{i}"), F.lit(0)), F.coalesce(F.col(f"vp{i}"), F.lit(0))
        issue_exprs.append(F.when((vn > 0) & (100.0 * vp / vn < ISSUE_VALIDITY_BELOW), F.struct(
            F.lit(col_name).alias("column"), F.lit(rule_name).alias("check"),
            F.concat(F.round(100.0 * vp / vn, 1).cast("string"), F.lit("% valid ("),
                     (vn - vp).cast("string"), F.lit(" of "), vn.cast("string"),
                     F.lit(f" fail {rule_name})")).alias("detail"))))
    for i, f in enumerate(fields):
        p = F.coalesce(F.col(f"p{i}"), F.lit(0))
        issue_exprs.append(F.when((rows > 0) & (100.0 * p / rows < ISSUE_COMPLETENESS_BELOW), F.struct(
            F.lit(f.name).alias("column"), F.lit("completeness").alias("check"),
            F.concat(F.round(100.0 * p / rows, 1).cast("string"), F.lit("% of values present ("),
                     (rows - p).cast("string"), F.lit(" missing)")).alias("detail"))))
    issues = F.slice(F.filter(F.array(*issue_exprs), lambda x: x.isNotNull()), 1, MAX_ISSUES)

    scored = [completeness, uniqueness, validity, F.lit(fresh)]
    overall = F.round(
        reduce(lambda a, b: a + b, [F.coalesce(s, F.lit(0.0)) for s in scored])
        / reduce(lambda a, b: a + b, [F.when(s.isNotNull(), 1).otherwise(0) for s in scored]), 1)

    return stats.select(
        F.lit(info["path"]).alias("file_path"),
        F.lit(os.path.basename(info["path"])).alias("file_name"),
        F.lit(table_name_for(info["relative"])).alias("table_name"),
        F.lit(info["ext"].lstrip(".")).alias("file_format"),
        F.lit(info["size"]).cast("long").alias("size_bytes"),
        rows.cast("long").alias("row_count"),
        F.lit(n_cols).cast("int").alias("column_count"),
        completeness.cast("double").alias("completeness"),
        uniqueness.cast("double").alias("uniqueness"),
        validity.cast("double").alias("validity"),
        F.lit(fresh).cast("double").alias("freshness"),
        overall.cast("double").alias("overall_score"),
        F.lit(info["modified"]).cast("timestamp").alias("last_modified"),
        issues.alias("issues"),
        F.current_timestamp().alias("profiled_at"),
    )


def failed_file(info: dict, error: Exception) -> DataFrame:
    """A file that cannot be read is reported with no scores and a profile_error issue, not dropped."""
    row = (
        info["path"], os.path.basename(info["path"]), table_name_for(info["relative"]), info["ext"].lstrip("."),
        info["size"], None, None, None, None, None, None, None,
        info["modified"],
        [(None, "profile_error", str(error)[:480])],
        dt.datetime.now(dt.timezone.utc),
    )
    return spark.createDataFrame([row], RESULT_SCHEMA)  # noqa: F821


@dp.materialized_view(
    name="file_quality_results",
    comment="One row per data file in the DataOne uploads volume: completeness, uniqueness, validity, freshness.",
)
@dp.expect_all({
    "completeness_in_range": "completeness IS NULL OR completeness BETWEEN 0 AND 100",
    "uniqueness_in_range": "uniqueness IS NULL OR uniqueness BETWEEN 0 AND 100",
    "validity_in_range": "validity IS NULL OR validity BETWEEN 0 AND 100",
    "freshness_in_range": "freshness IS NULL OR freshness BETWEEN 0 AND 100",
})
def file_quality_results() -> DataFrame:
    now = dt.datetime.now(dt.timezone.utc)
    frames = []
    for info in list_landing_files():
        try:
            frames.append(profile_file(info, now))
        except Exception as exc:  # unreadable / corrupt file: report it, keep profiling the rest
            frames.append(failed_file(info, exc))
    if not frames:
        return spark.createDataFrame([], RESULT_SCHEMA)  # noqa: F821
    return reduce(lambda a, b: a.unionByName(b), frames)
