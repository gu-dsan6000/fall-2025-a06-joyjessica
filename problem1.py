#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, lit
import pandas as pd
import sys

def main():
    # Get bucket name 
    bucket = sys.argv[1] if len(sys.argv) > 1 else "your-bucket-name"
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Problem1-LogLevelDistribution") \
        .getOrCreate()
    
    # Read all log files
    logs_path = f"s3a://{bucket}/data/application_*/container_*.log"
    logs_df = spark.read.text(logs_path)
    
    # Extract log level
    parsed = logs_df.withColumn(
        'log_level',
        regexp_extract('value', r'\s(INFO|WARN|ERROR|DEBUG)\s', 1)
    )
    
    # Filter rows with valid log levels
    valid_logs = parsed.filter(col('log_level') != '')
    
    # Count by log level
    counts = valid_logs.groupBy('log_level') \
        .agg(count('*').alias('count')) \
        .orderBy(col('count').desc())
    
    counts_pd = counts.toPandas()
    counts_pd.to_csv('data/output/problem1_counts.csv', index=False)
    
    # Random sample (10 entries)
    sample = valid_logs.select('value', 'log_level') \
        .withColumnRenamed('value', 'log_entry') \
        .sample(False, 0.001) \
        .limit(10)
    
    sample_pd = sample.toPandas()
    sample_pd.to_csv('data/output/problem1_sample.csv', index=False)
    
    # Summary statistics
    total_lines = logs_df.count()
    total_valid = valid_logs.count()
    unique_levels = counts.count()
    
    counts_collected = counts.collect()
    
    summary = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_valid:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]
    
    for row in counts_collected:
        level = row['log_level']
        cnt = row['count']
        pct = (cnt / total_valid) * 100
        summary.append(f"  {level:5s}: {cnt:10,} ({pct:5.2f}%)")
    
    with open('data/output/problem1_summary.txt', 'w') as f:
        f.write('\n'.join(summary))
    
    print('\n'.join(summary))
    
    spark.stop()

if __name__ == '__main__':
    main()