#!/usr/bin/env python3
"""
Comparison Demo - Direct comparison of pandas vs DuckDB performance
For MBA BigData course - side-by-side performance comparison
"""

import pandas as pd
import duckdb
import psutil
import time
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

def format_memory(mb):
    """Format memory usage for display"""
    if mb > 1024:
        return f"{mb/1024:.1f} GB"
    return f"{mb:.1f} MB"

def test_pandas(url):
    """Test pandas performance"""
    print("  🐼 Testing pandas...")
    
    mem_before = get_memory_usage()
    start_time = time.time()
    
    try:
        # Load data
        df = pd.read_csv(url, sep=';', names=['station', 'temperature'])
        load_time = time.time()
        
        # Perform aggregation
        result = df.groupby('station')['temperature'].agg(['min', 'mean', 'max']).reset_index()
        end_time = time.time()
        
        mem_after = get_memory_usage()
        
        return {
            'success': True,
            'load_time': load_time - start_time,
            'total_time': end_time - start_time,
            'memory_used': mem_after - mem_before,
            'records': len(df),
            'stations': len(result)
        }
        
    except MemoryError:
        return {
            'success': False,
            'error': 'Out of memory',
            'memory_used': get_memory_usage() - mem_before
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'memory_used': get_memory_usage() - mem_before
        }

def test_duckdb(url):
    """Test DuckDB performance"""
    print("  🦆 Testing DuckDB...")
    
    mem_before = get_memory_usage()
    start_time = time.time()
    
    try:
        conn = duckdb.connect()
        
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
        end_time = time.time()
        
        conn.close()
        mem_after = get_memory_usage()
        
        total_records = sum(row[4] for row in result)
        
        return {
            'success': True,
            'total_time': end_time - start_time,
            'memory_used': mem_after - mem_before,
            'records': total_records,
            'stations': len(result)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'memory_used': get_memory_usage() - mem_before
        }

def run_comparison():
    """Run performance comparison"""
    print("⚡ PANDAS vs DUCKDB PERFORMANCE COMPARISON")
    print("=" * 60)
    
    datasets = [
        ("https://storage.googleapis.com/bigdata2025/duckdb/small_dataset.csv", "Small (100K records)", 100_000),
        ("https://storage.googleapis.com/bigdata2025/duckdb/medium_dataset.csv", "Medium (10M records)", 10_000_000),
        ("https://storage.googleapis.com/bigdata2025/duckdb/large_dataset.csv", "Large (100M records)", 100_000_000),
    ]
    
    results = []
    
    for url, description, expected_records in datasets:
        
        print(f"\n📊 Testing {description}")
        print("-" * 40)
        
        # Test pandas
        pandas_result = test_pandas(url)
        time.sleep(1)  # Let system recover
        
        # Test DuckDB
        duckdb_result = test_duckdb(url)
        time.sleep(1)  # Let system recover
        
        # Store results
        results.append({
            'dataset': description,
            'url': url,
            'expected_records': expected_records,
            'pandas': pandas_result,
            'duckdb': duckdb_result
        })
        
        # Print comparison for this dataset
        print(f"\n  📈 Results for {description}:")
        
        if pandas_result['success']:
            print(f"    Pandas:  {pandas_result['total_time']:.2f}s, {format_memory(pandas_result['memory_used'])} memory")
        else:
            print(f"    Pandas:  ❌ {pandas_result['error']} ({format_memory(pandas_result['memory_used'])} memory)")
        
        if duckdb_result['success']:
            print(f"    DuckDB:  {duckdb_result['total_time']:.2f}s, {format_memory(duckdb_result['memory_used'])} memory")
        else:
            print(f"    DuckDB:  ❌ {duckdb_result['error']} ({format_memory(duckdb_result['memory_used'])} memory)")
        
        # Show speedup/efficiency if both succeeded
        if pandas_result['success'] and duckdb_result['success']:
            speedup = pandas_result['total_time'] / duckdb_result['total_time']
            memory_ratio = pandas_result['memory_used'] / max(duckdb_result['memory_used'], 1)
            print(f"    DuckDB is {speedup:.1f}x faster and uses {memory_ratio:.1f}x less memory")
    
    # Summary table
    print(f"\n{'='*80}")
    print("📊 PERFORMANCE SUMMARY")
    print(f"{'='*80}")
    
    print(f"{'Dataset':<20} {'Pandas Time':<15} {'DuckDB Time':<15} {'Pandas Memory':<15} {'DuckDB Memory':<15}")
    print("-" * 80)
    
    for result in results:
        dataset = result['dataset'].split('(')[0].strip()
        
        pandas_time = f"{result['pandas']['total_time']:.2f}s" if result['pandas']['success'] else "FAILED"
        duckdb_time = f"{result['duckdb']['total_time']:.2f}s" if result['duckdb']['success'] else "FAILED"
        pandas_mem = format_memory(result['pandas']['memory_used'])
        duckdb_mem = format_memory(result['duckdb']['memory_used'])
        
        print(f"{dataset:<20} {pandas_time:<15} {duckdb_time:<15} {pandas_mem:<15} {duckdb_mem:<15}")
    
    # Key insights
    print(f"\n💡 KEY INSIGHTS:")
    print(f"   • Pandas: Traditional in-memory processing")
    print(f"     - Fast for small datasets that fit in RAM")
    print(f"     - Memory usage grows linearly with data size")
    print(f"     - Fails when dataset exceeds available memory")
    print(f"   • DuckDB: Modern analytical database")
    print(f"     - Consistent performance regardless of data size")
    print(f"     - Low memory footprint through streaming")
    print(f"     - Scales from KB to TB datasets")
    print(f"   • Business Impact:")
    print(f"     - Cost: Lower infrastructure requirements")
    print(f"     - Speed: Faster insights on large datasets")
    print(f"     - Reliability: No out-of-memory crashes")

def main():
    """Main function"""
    print(f"System Info:")
    print(f"  Total Memory: {format_memory(psutil.virtual_memory().total / 1024 / 1024)}")
    print(f"  Available Memory: {format_memory(psutil.virtual_memory().available / 1024 / 1024)}")
    print(f"  CPU Cores: {psutil.cpu_count()}")
    
    run_comparison()

if __name__ == "__main__":
    main()