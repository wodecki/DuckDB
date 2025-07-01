#!/usr/bin/env python3
"""
Main demo runner - Complete BigData demonstration
For MBA students - runs all demos in sequence with explanations
"""

import sys
import time
import subprocess
from pathlib import Path

def print_header(title):
    """Print formatted header"""
    print(f"\n{'='*60}")
    print(f"{title:^60}")
    print(f"{'='*60}")

def print_section(title):
    """Print formatted section"""
    print(f"\n{'-'*40}")
    print(f"{title}")
    print(f"{'-'*40}")

def wait_for_user():
    """Wait for user to press Enter"""
    input("\n⏸️  Press Enter to continue...")

def run_script(script_name, description):
    """Run a Python script and handle errors"""
    print_section(f"Running: {description}")
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True, 
                              check=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with error code {e.returncode}")
        return False
    except FileNotFoundError:
        print(f"❌ Script {script_name} not found")
        return False

def main():
    """Run complete BigData demonstration"""
    
    print_header("MBA BIGDATA DEMONSTRATION")
    print("This demo shows the difference between traditional (pandas)")
    print("and modern (DuckDB) approaches to big data processing.")
    print(f"\nSystem: MacOS, 16GB RAM, Intel 2.6 GHz")
    print("Datasets: 100K, 10M, and 100M temperature measurements")
    
    print("\n✅ Using datasets from Google Cloud Storage")
    print("No local dataset generation needed - data streams directly from GCP!")
    
    print("\n📚 STEP 1: Understanding the Problem")
    print("We'll analyze temperature data from weather stations worldwide.")
    print("Task: Calculate min, mean, and max temperature for each station.")
    print("Challenge: How do different tools handle increasing data volumes?")
    wait_for_user()
    
    print("\n🐼 STEP 2: Pandas Demo")
    print("Traditional approach: Load all data into memory, then process.")
    print("Watch memory usage - it will grow significantly!")
    print("Expect the large dataset to fail with 'Out of Memory' error.")
    wait_for_user()
    
    run_script("pandas_demo.py", "Pandas Performance Test")
    
    print("\n🦆 STEP 3: DuckDB Demo")
    print("Modern approach: Stream data processing without loading everything.")
    print("Notice how memory usage stays low regardless of dataset size!")
    print("This is the power of modern analytical databases.")
    wait_for_user()
    
    run_script("duckdb_demo.py", "DuckDB Performance Test")
    
    print("\n⚡ STEP 4: Direct Comparison")
    print("Side-by-side performance comparison with detailed metrics.")
    print("Pay attention to speed and memory usage differences!")
    wait_for_user()
    
    run_script("comparison_demo.py", "Performance Comparison")
    
    print_header("DEMONSTRATION COMPLETE")
    
    print("🎓 KEY TAKEAWAYS FOR MBA STUDENTS:")
    print()
    print("1. 📊 SCALABILITY MATTERS")
    print("   • Traditional tools (pandas) fail as data grows")
    print("   • Modern tools (DuckDB) scale linearly")
    print()
    print("2. 💰 COST IMPLICATIONS")
    print("   • Memory requirements drive infrastructure costs")
    print("   • Efficient tools reduce hardware needs")
    print()
    print("3. 🚀 PERFORMANCE IMPACT")
    print("   • Processing time affects business agility")
    print("   • Faster analytics enable quicker decisions")
    print()
    print("4. 🔒 RELIABILITY CONCERNS")
    print("   • Out-of-memory crashes disrupt operations")
    print("   • Predictable performance enables planning")
    print()
    print("5. 🎯 STRATEGIC CHOICES")
    print("   • Technology selection affects long-term success")
    print("   • Plan for data growth from day one")
    print()
    print("💡 BUSINESS QUESTIONS TO CONSIDER:")
    print("   • How much data do we expect in 2-3 years?")
    print("   • What's the cost of system downtime?")
    print("   • How quickly do we need insights?")
    print("   • What's our tolerance for technical debt?")
    print()
    print("📈 NEXT STEPS:")
    print("   • Explore cloud vs on-premise options")
    print("   • Learn about data lakes and warehouses")
    print("   • Understand distributed computing (Spark, etc.)")
    print("   • Consider real-time vs batch processing needs")
    
    print(f"\n{'='*60}")
    print("Thank you for attending the BigData demonstration!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()