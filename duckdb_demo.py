#!/usr/bin/env python3
"""
DuckDB Demo - Demonstrating efficient processing of large datasets
For MBA BigData course - shows how DuckDB handles large datasets efficiently
"""

import duckdb
import psutil
import time
import sys
from pathlib import Path

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def format_memory(mb):
    """Format memory usage for display"""
    if mb > 1024:
        return f"{mb/1024:.1f} GB"
    return f"{mb:.1f} MB"

def analyze_with_duckdb(url, description):
    """Analyze temperature data using DuckDB"""
    print(f"\n{'='*60}")
    print(f"DUCKDB ANALYSIS: {description}")
    print(f"URL: {url}")
    print(f"{'='*60}")
    
    # No file check needed for URLs
    
    # Memory before processing
    mem_before = get_memory_usage()
    print(f"Memory before processing: {format_memory(mem_before)}")
    
    try:
        # Create DuckDB connection
        conn = duckdb.connect()
        
        print("🦆 Processing data with DuckDB...")
        start_time = time.time()
        
        # DuckDB query that reads CSV and performs aggregation in one step
        query = """
        SELECT 
            station,
            MIN(temperature) as min_temp,
            AVG(temperature) as mean_temp,
            MAX(temperature) as max_temp,
            COUNT(*) as record_count
        FROM read_csv(?, 
            header=false, 
            columns={'station': 'VARCHAR', 'temperature': 'DOUBLE'},
            delim=';'
        )
        GROUP BY station
        ORDER BY station
        """
        
        result = conn.execute(query, [url]).fetchall()
        
        process_time = time.time() - start_time
        mem_after = get_memory_usage()
        
        print(f"✓ Processing completed in {process_time:.2f} seconds")
        print(f"Memory after processing: {format_memory(mem_after)} (+{format_memory(mem_after - mem_before)})")
        
        # Calculate total records
        total_records = sum(row[4] for row in result)
        print(f"Total records processed: {total_records:,}")
        
        # Show sample results
        print(f"\n📈 Results ({len(result)} stations):")
        print(f"{'Station':<20} {'Min':<8} {'Mean':<8} {'Max':<8} {'Count':<10}")
        print("-" * 60)
        
        for i, (station, min_temp, mean_temp, max_temp, count) in enumerate(result[:10]):
            print(f"{station:<20} {min_temp:<8.1f} {mean_temp:<8.1f} {max_temp:<8.1f} {count:<10,}")
        
        if len(result) > 10:
            print("... (showing first 10 stations)")
        
        conn.close()
        
        return {
            'process_time': process_time,
            'peak_memory': mem_after,
            'memory_increase': mem_after - mem_before,
            'records': total_records,
            'stations': len(result)
        }
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def main():
    """Run DuckDB demo on different dataset sizes"""
    print("🦆 DUCKDB BIG DATA DEMO")
    print("Demonstrating efficient processing of large datasets")
    print(f"System Memory: {format_memory(psutil.virtual_memory().total / 1024 / 1024)}")
    print(f"Available Memory: {format_memory(psutil.virtual_memory().available / 1024 / 1024)}")
    
    datasets = [
        ("https://storage.googleapis.com/bigdata2025/duckdb/small_dataset.csv", "Small Dataset (100K records)"),
        ("https://storage.googleapis.com/bigdata2025/duckdb/medium_dataset.csv", "Medium Dataset (10M records)"),
        ("https://storage.googleapis.com/bigdata2025/duckdb/large_dataset.csv", "Large Dataset (100M records)"),
    ]
    
    results = {}
    
    for url, description in datasets:
        result = analyze_with_duckdb(url, description)
        if result:
            results[description] = result
        
        # Give system time to recover
        time.sleep(1)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 DUCKDB PERFORMANCE SUMMARY")
    print(f"{'='*60}")
    
    if results:
        print(f"{'Dataset':<30} {'Records':<12} {'Process Time':<15} {'Memory Usage':<15}")
        print("-" * 80)
        
        for desc, data in results.items():
            dataset_name = desc.split('(')[0].strip()
            print(f"{dataset_name:<30} {data['records']:<12,} {data['process_time']:<15.2f}s {format_memory(data['memory_increase']):<15}")
    
    print(f"\n💡 Key Advantages of DuckDB:")
    print(f"   • Processes data without loading entirely into memory")
    print(f"   • Constant memory usage regardless of dataset size")
    print(f"   • Built-in CSV reading with parallel processing")
    print(f"   • SQL interface for complex analytics")
    print(f"   • Columnar storage for efficient aggregations")
    print(f"   • No separate database server required")

if __name__ == "__main__":
    main()