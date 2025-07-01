# BigData Analysis Demo: Pandas vs DuckDB

A practical demonstration for MBA students showing the limitations of traditional data processing tools (pandas) and the advantages of modern analytical databases (DuckDB) when working with large datasets.

## System Requirements

- **Tested on**: MacOS with 16GB RAM, Intel 2.6 GHz
- **Python**: 3.9+
- **Package Manager**: uv (ultrafast Python package manager)

## Quick Start

1. **Install dependencies**:
   ```bash
   uv sync
   ```

2. **Test GCP bucket access** (if needed):
   ```bash
   # Diagnose any connection issues
   uv run python diagnose_gcp.py
   ```

3. **Run demonstrations** (datasets stream from Google Cloud):
   ```bash
   # Complete guided demo (recommended)
   uv run python run_demo.py
   
   # Or run individual demos:
   # Pandas demo (shows memory limitations)
   uv run python pandas_demo.py
   
   # DuckDB demo (shows efficient processing)
   uv run python duckdb_demo.py
   
   # Side-by-side comparison
   uv run python comparison_demo.py
   ```

## What This Demo Shows

### Dataset Sizes
- **Small**: 100K records (~2MB) - Fits comfortably in memory
- **Medium**: 10M records (~200MB) - Shows memory pressure with pandas
- **Large**: 100M records (~2GB) - Likely to cause out-of-memory errors with pandas

### Key Learning Points

#### Pandas Limitations
- Loads entire dataset into memory
- Memory usage grows linearly with data size
- Processing slows significantly with large datasets
- May crash with "out of memory" errors
- System becomes unresponsive during processing

#### DuckDB Advantages
- Processes data without loading entirely into memory
- Consistent performance regardless of dataset size
- Built-in optimizations for analytical queries
- SQL interface familiar to business users
- No separate database server required

## Business Impact

### Cost Efficiency
- **Pandas**: Requires expensive high-memory servers for large datasets
- **DuckDB**: Runs efficiently on standard hardware

### Performance
- **Pandas**: Processing time grows exponentially with data size
- **DuckDB**: Linear scaling with dataset size

### Reliability
- **Pandas**: Fails unpredictably with large datasets
- **DuckDB**: Consistent performance across all data sizes

## Data Source

All datasets are hosted on Google Cloud Storage with public access:
- **Small**: https://storage.googleapis.com/bigdata2025/duckdb/small_dataset.csv (100K records)
- **Medium**: https://storage.googleapis.com/bigdata2025/duckdb/medium_dataset.csv (10M records)  
- **Large**: https://storage.googleapis.com/bigdata2025/duckdb/large_dataset.csv (100M records)


## Files Description

- `pandas_demo.py` - Demonstrates pandas performance and limitations  
- `duckdb_demo.py` - Shows DuckDB's efficient processing capabilities
- `comparison_demo.py` - Direct side-by-side performance comparison
- `run_demo.py` - Complete guided demonstration runner
- `pyproject.toml` - Project dependencies and configuration

## Sample Output

```
Dataset                 Pandas Time     DuckDB Time     Pandas Memory   DuckDB Memory
--------------------------------------------------------------------------------
Small                   0.05s          0.02s           45.2 MB         2.1 MB
Medium                  4.23s          0.18s           1.8 GB          8.3 MB
Large                   FAILED         1.45s           OUT OF MEMORY   12.7 MB
```

## Key Points 

1. **Technology Selection Impact**: Choice of tools significantly affects scalability and costs
2. **Resource Planning**: Understanding memory requirements prevents production failures  
3. **Modern vs Legacy**: Newer technologies often provide step-change improvements
4. **Total Cost of Ownership**: Include infrastructure, maintenance, and failure costs
5. **Data Strategy**: Plan for data growth from day one

## Next Steps

- Explore DuckDB's advanced features (joins, window functions, etc.)
- Learn about data partitioning strategies
- Understand cloud vs on-premise trade-offs
- Investigate other modern analytical databases (ClickHouse, Apache Spark, etc.)

---

*This demo is designed to provide hands-on experience with BigData challenges commonly faced in business environments.*