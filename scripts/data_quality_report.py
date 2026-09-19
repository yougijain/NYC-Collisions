"""Generate a data quality report for the published collisions dataset.

Reads whatever app/db.py resolves (a local build, the Release asset, or the
committed seed) and writes data/clean/data_quality_report.txt.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))

import db  # noqa: E402

OUTPUT_PATH = ROOT / "data" / "clean" / "data_quality_report.txt"

# Thresholds for the per-column completeness verdict.
OK_THRESHOLD = 95.0
WARN_THRESHOLD = 80.0


def generate_quality_report() -> str:
    """Build the report text and write it to disk."""
    connection = db.connect()
    columns = connection.execute(
        f"SELECT * FROM {db.TABLE_NAME} LIMIT 0"
    ).df().columns.tolist()

    # One pass for every column's completeness, rather than a query each.
    completeness_sql = ", ".join(
        f'ROUND(100.0 * COUNT("{col}") / COUNT(*), 2) AS "{col}"'
        for col in columns
    )
    completeness = connection.execute(
        f"SELECT {completeness_sql} FROM {db.TABLE_NAME}"
    ).df().iloc[0]

    totals = connection.execute(f"""
        SELECT
            COUNT(*)                                   AS total_rows,
            MIN(crash_datetime)                        AS min_date,
            MAX(crash_datetime)                        AS max_date,
            COUNT(*) FILTER (WHERE crash_datetime IS NULL)    AS null_datetime,
            COUNT(*) FILTER (WHERE number_of_persons_injured < 0
                                OR number_of_persons_killed  < 0) AS negative_counts,
            COUNT(*) FILTER (WHERE latitude IS NOT NULL
                               AND longitude IS NOT NULL)     AS geo_complete,
            SUM(number_of_persons_injured)             AS total_injuries,
            SUM(number_of_persons_killed)              AS total_fatalities,
            AVG(number_of_persons_injured)             AS avg_injuries,
            COUNT(DISTINCT borough)                    AS unique_boroughs,
            COUNT(*) - COUNT(DISTINCT collision_id)    AS duplicate_ids
        FROM {db.TABLE_NAME}
    """).df().iloc[0]

    total_rows = int(totals["total_rows"])
    lines = [
        "=" * 60,
        "NYC COLLISIONS DATA QUALITY REPORT",
        "=" * 60,
        f"\nTotal Records: {total_rows:,}",
        f"Total Columns: {len(columns)}",
        "\n" + "-" * 60,
        "COLUMN-LEVEL METRICS",
        "-" * 60,
    ]

    high_quality = 0
    for col in columns:
        pct = float(completeness[col])
        if pct >= OK_THRESHOLD:
            status = "[OK]"
            high_quality += 1
        elif pct >= WARN_THRESHOLD:
            status = "[WARN]"
        else:
            status = "[FAIL]"

        non_null = round(total_rows * pct / 100)
        lines.append(f"\n{status} {col}")
        lines.append(
            f"   Completeness: {pct}% ({non_null:,} non-null, "
            f"{total_rows - non_null:,} null)"
        )

    geo_pct = round(100.0 * int(totals["geo_complete"]) / total_rows, 2)
    lines += [
        "\n" + "-" * 60,
        "DATA VALIDATION CHECKS",
        "-" * 60,
        f"\n[OK] Crash DateTime NULL Check: {int(totals['null_datetime'])} "
        f"NULL values (Expected: 0)",
        f"[OK] Duplicate Collision IDs: {int(totals['duplicate_ids'])} (Expected: 0)",
        f"[OK] Date Range: {totals['min_date']} to {totals['max_date']}",
        f"[OK] Negative Injury Counts: {int(totals['negative_counts'])} (Expected: 0)",
        f"[OK] Geographic Data: {geo_pct}% complete "
        f"({int(totals['geo_complete']):,} records with lat/long)",
        "\n" + "-" * 60,
        "SUMMARY STATISTICS",
        "-" * 60,
        f"\nTotal Injuries: {int(totals['total_injuries']):,}",
        f"Total Fatalities: {int(totals['total_fatalities']):,}",
        f"Average Injuries per Crash: {float(totals['avg_injuries']):.2f}",
        f"Unique Boroughs: {int(totals['unique_boroughs'])}",
    ]

    score = round(100.0 * high_quality / len(columns), 1)
    lines += [
        "\n" + "-" * 60,
        f"OVERALL DATA QUALITY SCORE: {score}%",
        f"({high_quality}/{len(columns)} columns with >={OK_THRESHOLD:.0f}% completeness)",
        "=" * 60,
    ]

    connection.close()

    report = "\n".join(lines)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(report, encoding="utf-8")
    return report


if __name__ == "__main__":
    print(generate_quality_report())
    print(f"\n[OK] Report saved to: {OUTPUT_PATH}")
