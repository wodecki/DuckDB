#!/usr/bin/env python3
"""
Pandas Demo - Demonstrating memory limitations with large datasets
For MBA BigData course - shows how pandas struggles with large datasets
"""

import pandas as pd
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

def analyze_with_pandas(filename, description):
    """Analyze temperature data using pandas"""
    print(f"\n{'='*60}")
    print(f"PANDAS ANALYSIS: {description}")
    print(f"File: {filename}")
    print(f"{'='*60}")
    
    if not Path(filename).exists():
        print(f"❌ File {filename} not found. Please run generate_datasets.py first.")
        return None
    
    # Memory before loading
    mem_before = get_memory_usage()
    print(f"Memory before loading: {format_memory(mem_before)}")
    
    try:
        # Time the data loading
        print("📊 Loading data with pandas...")
        start_time = time.time()
        
        df = pd.read_csv(filename, sep=';', names=['station', 'temperature'])
        
        load_time = time.time() - start_time
        mem_after_load = get_memory_usage()
        
        print(f"✓ Data loaded in {load_time:.2f} seconds")
        print(f"Memory after loading: {format_memory(mem_after_load)} (+{format_memory(mem_after_load - mem_before)})")
        print(f"Dataset shape: {df.shape}")
        
        # Time the aggregation
        print("🔄 Performing aggregation (min, mean, max by station)...")
        start_time = time.time()
        
        result = df.groupby('station')['temperature'].agg(['min', 'mean', 'max']).reset_index()
        result = result.sort_values('station')
        
        agg_time = time.time() - start_time
        mem_after_agg = get_memory_usage()
        
        print(f"✓ Aggregation completed in {agg_time:.2f} seconds")
        print(f"Memory after aggregation: {format_memory(mem_after_agg)}")
        
        # Show sample results
        print(f"\n📈 Results ({len(result)} stations):")
        print(result.head(10).to_string(index=False))
        if len(result) > 10:
            print("... (showing first 10 stations)")
        
        return {
            'load_time': load_time,
            'agg_time': agg_time,
            'total_time': load_time + agg_time,
            'peak_memory': max(mem_after_load, mem_after_agg),
            'memory_increase': mem_after_agg - mem_before,
            'records': len(df)
        }
        
    except MemoryError:
        mem_current = get_memory_usage()
        print(f"❌ MEMORY ERROR: Ran out of memory!")
        print(f"Memory usage when failed: {format_memory(mem_current)}")
        print(f"💡 This demonstrates pandas' limitation with large datasets")
        return None
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def main():
    """Run pandas demo on different dataset sizes"""
    print("🐼 PANDAS BIG DATA DEMO")
    print("Demonstrating memory limitations with large datasets")
    print(f"System Memory: {format_memory(psutil.virtual_memory().total / 1024 / 1024)}")
    print(f"Available Memory: {format_memory(psutil.virtual_memory().available / 1024 / 1024)}")
    
    datasets = [
        ("data/small_dataset.csv", "Small Dataset (100K records)"),
        ("data/medium_dataset.csv", "Medium Dataset (10M records)"),
        ("data/large_dataset.csv", "Large Dataset (100M records)"),
    ]
    
    results = {}
    
    for filename, description in datasets:
        result = analyze_with_pandas(filename, description)
        if result:
            results[description] = result
        
        # Give system time to recover
        time.sleep(2)
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 PANDAS PERFORMANCE SUMMARY")
    print(f"{'='*60}")
    
    if results:
        print(f"{'Dataset':<30} {'Records':<12} {'Load Time':<12} {'Agg Time':<12} {'Peak Memory':<15}")
        print("-" * 85)
        
        for desc, data in results.items():
            dataset_name = desc.split('(')[0].strip()
            print(f"{dataset_name:<30} {data['records']:<12,} {data['load_time']:<12.2f}s {data['agg_time']:<12.2f}s {format_memory(data['peak_memory']):<15}")
    
    print(f"\n💡 Key Observations:")
    print(f"   • Pandas loads entire dataset into memory")
    print(f"   • Memory usage grows linearly with data size")
    print(f"   • Large datasets may cause out-of-memory errors")
    print(f"   • Processing time increases significantly with size")
    print(f"   • System becomes less responsive during processing")

if __name__ == "__main__":
    main()