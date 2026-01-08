import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/clean/data.db")
OUTPUT_PATH = Path("data/clean/data_quality_report.txt")

def generate_quality_report():
    """Generate comprehensive data quality report"""
    
    conn = sqlite3.connect(DB_PATH)
    
    #  table info
    df = pd.read_sql_query("SELECT * FROM collisions_clean LIMIT 1", conn)
    total_rows = pd.read_sql_query("SELECT COUNT(*) as count FROM collisions_clean", conn).iloc[0]['count']
    
    report_lines = []
    report_lines.append("=" * 60)
    report_lines.append("NYC COLLISIONS DATA QUALITY REPORT")
    report_lines.append("=" * 60)
    report_lines.append(f"\nTotal Records: {total_rows:,}")
    report_lines.append(f"Total Columns: {len(df.columns)}")
    report_lines.append("\n" + "-" * 60)
    report_lines.append("COLUMN-LEVEL METRICS")
    report_lines.append("-" * 60)
    
    # Column-level analysis
    for col in df.columns:
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT({col}) as non_null_count,
            COUNT(*) - COUNT({col}) as null_count,
            ROUND(100.0 * COUNT({col}) / COUNT(*), 2) as completeness_pct
        FROM collisions_clean
        """
        result = pd.read_sql_query(query, conn).iloc[0]
        
        completeness = result['completeness_pct']
        null_count = result['null_count']
        
        status = "[OK]" if completeness >= 95 else "[WARN]" if completeness >= 80 else "[FAIL]"
        
        report_lines.append(f"\n{status} {col}")
        report_lines.append(f"   Completeness: {completeness}% ({result['non_null_count']:,} non-null, {null_count:,} null)")
    
    report_lines.append("\n" + "-" * 60)
    report_lines.append("DATA VALIDATION CHECKS")
    report_lines.append("-" * 60)
    
    null_datetime = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM collisions_clean WHERE crash_datetime IS NULL", 
        conn
    ).iloc[0]['count']
    report_lines.append(f"\n[OK] Crash DateTime NULL Check: {null_datetime} NULL values (Expected: 0)")
    
    # range validity
    date_range = pd.read_sql_query(
        "SELECT MIN(crash_datetime) as min_date, MAX(crash_datetime) as max_date FROM collisions_clean",
        conn
    ).iloc[0]
    report_lines.append(f"[OK] Date Range: {date_range['min_date']} to {date_range['max_date']}")
    
    # negative injury counts
    negative_injuries = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM collisions_clean WHERE number_of_persons_injured < 0",
        conn
    ).iloc[0]['count']
    report_lines.append(f"[OK] Negative Injury Counts: {negative_injuries} (Expected: 0)")
    
    geo_complete = pd.read_sql_query(
        "SELECT COUNT(*) as count FROM collisions_clean WHERE latitude IS NOT NULL AND longitude IS NOT NULL",
        conn
    ).iloc[0]['count']
    geo_pct = round(100.0 * geo_complete / total_rows, 2)
    report_lines.append(f"[OK] Geographic Data: {geo_pct}% complete ({geo_complete:,} records with lat/long)")
    
    report_lines.append("\n" + "-" * 60)
    report_lines.append("SUMMARY STATISTICS")
    report_lines.append("-" * 60)
    
    stats = pd.read_sql_query("""
        SELECT 
            SUM(number_of_persons_injured) as total_injuries,
            SUM(number_of_persons_killed) as total_fatalities,
            AVG(number_of_persons_injured) as avg_injuries,
            COUNT(DISTINCT borough) as unique_boroughs
        FROM collisions_clean
    """, conn).iloc[0]
    
    report_lines.append(f"\nTotal Injuries: {int(stats['total_injuries']):,}")
    report_lines.append(f"Total Fatalities: {int(stats['total_fatalities']):,}")
    report_lines.append(f"Average Injuries per Crash: {stats['avg_injuries']:.2f}")
    report_lines.append(f"Unique Boroughs: {int(stats['unique_boroughs'])}")
    
    # Data quality score
    high_quality_cols = sum(1 for col in df.columns 
                           if pd.read_sql_query(
                               f"SELECT ROUND(100.0 * COUNT({col}) / COUNT(*), 2) as pct FROM collisions_clean",
                               conn
                           ).iloc[0]['pct'] >= 95)
    
    quality_score = round(100.0 * high_quality_cols / len(df.columns), 1)
    report_lines.append("\n" + "-" * 60)
    report_lines.append(f"OVERALL DATA QUALITY SCORE: {quality_score}%")
    report_lines.append(f"({high_quality_cols}/{len(df.columns)} columns with >=95% completeness)")
    report_lines.append("=" * 60)
    
    conn.close()
    
    report_text = "\n".join(report_lines)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(report_text, encoding='utf-8')
    
    print(report_text)
    print(f"\n[OK] Report saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_quality_report()
