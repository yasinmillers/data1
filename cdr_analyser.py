mport pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import argparse
import os

class CDRAnalyzer:
    def __init__(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.df = pd.read_csv(file_path)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        self.df['hour'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.day_name()
        self.df['date'] = self.df['timestamp'].dt.date
        
    def summary_stats(self):
        print("\n=== CDR Summary Statistics ===")
        print(f"Total Records: {len(self.df)}")
        print(f"Date Range: {self.df['date'].min()} to {self.df['date'].max()}")
        print(f"Average Duration: {self.df['duration'].mean():.2f} seconds")
        
        # Success rates
        results = self.df['result'].value_counts(normalize=True) * 100
        print("\nCall Outcomes:")
        for res, perc in results.items():
            print(f"  {res:12}: {perc:.2f}%")
            
        print("\nTop 5 Callers (Volume):")
        print(self.df['caller'].value_counts().head(5).to_string())
        
        print("\nTop 5 Callees (Volume):")
        print(self.df['callee'].value_counts().head(5).to_string())
        
    def detect_anomalies(self):
        print("\n=== Anomaly Detection ===")
        
        # 1. High Frequency, Short Duration (Potential Robocalls)
        stats = self.df.groupby('caller').agg({
            'record_id': 'count',
            'duration': ['mean', 'min', 'max']
        })
        stats.columns = ['call_count', 'avg_duration', 'min_duration', 'max_duration']
        
        # Criteria: > 5 calls and avg duration < 10s
        robocalls = stats[(stats['call_count'] > 5) & (stats['avg_duration'] < 10)]
        print(f"\nPotential Robocalls detected: {len(robocalls)}")
        if not robocalls.empty:
            print(robocalls.sort_values('call_count', ascending=False).head(10))
            
        # 2. Sequential/Burst Calling (Same caller, multiple calls in short time)
        # We'll check for callers with > 10 calls in any 5-minute window
        self.df = self.df.sort_values(['caller', 'timestamp'])
        self.df['time_diff'] = self.df.groupby('caller')['timestamp'].diff().dt.total_seconds()
        
        bursts = self.df[self.df['time_diff'] < 60] # Calls within 60s of each other
        burst_stats = bursts.groupby('caller').size()
        high_burst = burst_stats[burst_stats > 5]
        
        print(f"\nPotential Burst Calling detected: {len(high_burst)}")
        if not high_burst.empty:
            print(high_burst.sort_values(ascending=False).head(10))

    def plot_patterns(self):
        sns.set_theme(style="whitegrid")
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Plot 1: Hourly Distribution
        sns.histplot(self.df['hour'], bins=24, kde=True, ax=axes[0, 0], color='skyblue')
        axes[0, 0].set_title('Call Volume by Hour of Day')
        axes[0, 0].set_xlabel('Hour (0-23)')
        
        # Plot 2: Day of Week Distribution
        days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        sns.countplot(data=self.df, x='day_of_week', order=days_order, ax=axes[0, 1], hue='day_of_week', palette='viridis', legend=False)
        axes[0, 1].set_title('Call Volume by Day of Week')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # Plot 3: Call Results (Pie Chart)
        outcomes = self.df['result'].value_counts()
        axes[1, 0].pie(outcomes, labels=outcomes.index, autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
        axes[1, 0].set_title('Call Outcome Distribution')
        
        # Plot 4: Duration Distribution (log scale for better visibility)
        sns.histplot(self.df['duration'], bins=50, ax=axes[1, 1], color='salmon', kde=True)
        axes[1, 1].set_title('Call Duration Distribution')
        axes[1, 1].set_xlabel('Duration (seconds)')
        axes[1, 1].set_xscale('log')
        
        plt.tight_layout()
        plt.savefig('cdr_analysis_dashboard.png')
        print("\nVisualization saved to 'cdr_analysis_dashboard.png'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CDR Pattern Analyzer')
    parser.add_argument('--file', type=str, default='sample_cdr.csv', help='Path to the CDR CSV file')
    args = parser.parse_args()
    
    try:
        analyzer = CDRAnalyzer(args.file)
        analyzer.summary_stats()
        analyzer.detect_anomalies()
        analyzer.plot_patterns()
        print("\nAnalysis Complete.")
    except Exception as e:
        print(f"Error: {e}")
