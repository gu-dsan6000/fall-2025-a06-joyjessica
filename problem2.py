#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, input_file_name, to_timestamp, 
    min as spark_min, max as spark_max, count, col
)
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

def main():
    # Get bucket name
    bucket = sys.argv[1] if len(sys.argv) > 1 else "your-bucket-name"
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Problem2-ClusterUsageAnalysis") \
        .getOrCreate()
    
    # Read logs
    logs_path = f"s3a://{bucket}/data/application_*/container_*.log"
    logs_df = spark.read.text(logs_path)
    
    # Extract metadata from file paths and timestamps
    df = logs_df.withColumn('file_path', input_file_name()) \
        .withColumn('application_id', 
            regexp_extract('file_path', r'application_(\d+)_(\d+)', 0)) \
        .withColumn('cluster_id',
            regexp_extract('file_path', r'application_(\d+)_', 1)) \
        .withColumn('timestamp_str',
            regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1)) \
        .filter(col('timestamp_str') != '')
    
    # Parse timestamps
    df = df.withColumn('timestamp',
        to_timestamp('timestamp_str', 'yy/MM/dd HH:mm:ss'))
    
    # Create timeline data per application
    timeline = df.groupBy('cluster_id', 'application_id') \
        .agg(
            spark_min('timestamp').alias('start_time'),
            spark_max('timestamp').alias('end_time')
        )
    
    # Calculate duration
    timeline = timeline.withColumn('duration_seconds',
        (col('end_time').cast('long') - col('start_time').cast('long')))
    
    # Save timeline
    timeline_pd = timeline.toPandas()
    timeline_pd.to_csv('data/output/problem2_timeline.csv', index=False)
    
    # Cluster summary
    cluster_summary = timeline.groupBy('cluster_id') \
        .agg(count('application_id').alias('app_count')) \
        .orderBy(col('app_count').desc())
    
    cluster_summary_pd = cluster_summary.toPandas()
    cluster_summary_pd.to_csv('data/output/problem2_cluster_summary.csv', index=False)
    
    
    # Statistics
    total_clusters = cluster_summary_pd.shape[0]
    total_apps = timeline_pd.shape[0]
    most_used_cluster = cluster_summary_pd.iloc[0]['cluster_id']
    most_used_count = cluster_summary_pd.iloc[0]['app_count']
    
    stats = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Most heavily used cluster: {most_used_cluster}",
        f"Applications on most used cluster: {most_used_count}",
        "",
        "Top 5 clusters by application count:"
    ]
    
    for idx, row in cluster_summary_pd.head(5).iterrows():
        stats.append(f"  Cluster {row['cluster_id']}: {row['app_count']} applications")
    
    with open('data/output/problem2_stats.txt', 'w') as f:
        f.write('\n'.join(stats))
    
    print('\n'.join(stats))
    
    # Visualization 1: Bar chart of applications per cluster
    plt.figure(figsize=(12, 6))
    top_10 = cluster_summary_pd.head(10)
    sns.barplot(data=top_10, x='cluster_id', y='app_count', hue='cluster_id', palette='viridis', legend=False)
    plt.xlabel('Cluster ID')
    plt.ylabel('Number of Applications')
    plt.title('Top 10 Clusters by Application Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('data/output/problem2_bar_chart.png', dpi=150)
    plt.close()
    
    # Visualization 2: Duration density plot for largest cluster
    plt.figure(figsize=(10, 6))
    largest_cluster_data = timeline_pd[timeline_pd['cluster_id'] == most_used_cluster]
    
    # Filter out extreme outliers and use log scale
    duration_filtered = largest_cluster_data[largest_cluster_data['duration_seconds'] > 0]['duration_seconds']
    
    sns.kdeplot(data=duration_filtered, log_scale=True, fill=True)
    plt.xlabel('Job Duration (seconds, log scale)')
    plt.ylabel('Density')
    plt.title(f'Job Duration Distribution for Cluster {most_used_cluster}')
    plt.tight_layout()
    plt.savefig('data/output/problem2_density_plot.png', dpi=150)
    plt.close()
    
    spark.stop()

if __name__ == '__main__':
    main()