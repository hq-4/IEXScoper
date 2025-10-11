#!/usr/bin/env python3
"""
IEX Tick Data Visualization Tool

This script loads tick-level stock data from CSV files into Polars DataFrames
and provides various visualization options using datashader, hvplot, and bokeh.
"""

import polars as pl
import numpy as np
import pandas as pd
from pathlib import Path
import argparse
from typing import Optional, List
import warnings

# Visualization imports
import datashader as ds
import datashader.transfer_functions as tf
import holoviews as hv
import hvplot.pandas
import hvplot.polars
from bokeh.plotting import show, save, output_file
from bokeh.models import HoverTool
from bokeh.layouts import column, row
import plotly.express as px
import plotly.graph_objects as go

# Enable hvplot extension
hv.extension('bokeh')

class IEXDataLoader:
    """Load and process IEX tick data from CSV files."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
    def load_csv(self, file_path: str) -> pl.DataFrame:
        """Load CSV file into Polars DataFrame with proper schema inference."""
        try:
            # First, try to read with automatic schema inference
            df = pl.read_csv(file_path, try_parse_dates=True)
            
            print(f"Original columns: {df.columns}")
            print(f"Original schema: {df.schema}")
            
            # Map IEX-specific columns to standard format
            column_mapping = {
                'Exchange Timestamp': 'timestamp',
                'Packet Capture Time': 'packet_time', 
                'Send Time': 'send_time',
                'Symbol': 'symbol',
                'Price': 'price', 
                'Size': 'volume',
                'Tick Type': 'tick_type',
                'Trade ID': 'trade_id',
                'Sale Condition': 'sale_condition'
            }
            
            # Rename columns if they exist
            for old_name, new_name in column_mapping.items():
                if old_name in df.columns:
                    df = df.rename({old_name: new_name})
            
            # Handle timestamp conversion - prefer Exchange Timestamp
            # Check if timestamp is integer (UNIX timestamp) or string
            if 'timestamp' in df.columns:
                if df['timestamp'].dtype == pl.Int64:
                    # Convert from nanoseconds to datetime in EST
                    df = df.with_columns(
                        pl.from_epoch(pl.col('timestamp'), time_unit='ns')
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
                else:
                    df = df.with_columns(
                        pl.col('timestamp').str.to_datetime()
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
            elif 'packet_time' in df.columns:
                if df['packet_time'].dtype == pl.Int64:
                    df = df.with_columns(
                        pl.from_epoch(pl.col('packet_time'), time_unit='ns')
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
                else:
                    df = df.with_columns(
                        pl.col('packet_time').str.to_datetime()
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
            elif 'send_time' in df.columns:
                if df['send_time'].dtype == pl.Int64:
                    df = df.with_columns(
                        pl.from_epoch(pl.col('send_time'), time_unit='ns')
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
                else:
                    df = df.with_columns(
                        pl.col('send_time').str.to_datetime()
                        .dt.convert_time_zone('America/New_York')
                        .alias('timestamp')
                    )
            
            # Ensure required columns exist with defaults if missing
            if 'symbol' not in df.columns:
                df = df.with_columns(pl.lit('UNKNOWN').alias('symbol'))
            if 'volume' not in df.columns and 'Size' in df.columns:
                df = df.rename({'Size': 'volume'})
            if 'volume' not in df.columns:
                df = df.with_columns(pl.lit(100).alias('volume'))
            if 'price' not in df.columns and 'Price' in df.columns:
                df = df.rename({'Price': 'price'})
                
            # Add side column based on tick type or default
            if 'side' not in df.columns:
                if 'tick_type' in df.columns:
                    # Map tick types to buy/sell if possible
                    df = df.with_columns(
                        pl.when(pl.col('tick_type').str.contains('(?i)buy|bid'))
                        .then(pl.lit('B'))
                        .when(pl.col('tick_type').str.contains('(?i)sell|ask'))
                        .then(pl.lit('S'))
                        .otherwise(pl.lit('T'))  # Trade
                        .alias('side')
                    )
                else:
                    df = df.with_columns(pl.lit('T').alias('side'))  # Default to Trade
            
            print(f"Loaded {len(df)} rows from {file_path}")
            print(f"Mapped columns: {df.columns}")
            print(f"Final schema: {df.schema}")
            print(f"Sample data:\n{df.head()}")
            
            # Debug price values
            if 'price' in df.columns:
                print(f"\nPrice statistics:")
                print(f"Min price: {df['price'].min()}")
                print(f"Max price: {df['price'].max()}")
                print(f"Mean price: {df['price'].mean()}")
                print(f"Sample prices: {df['price'].head(10).to_list()}")
            
            return df
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            raise
    
    def get_sample_data(self, symbol: str = "AAPL", num_points: int = 100000) -> pl.DataFrame:
        """Generate sample tick data for testing if no CSV is available."""
        print(f"Generating sample data for {symbol} with {num_points} points...")
        
        # Generate realistic tick data
        np.random.seed(42)
        base_price = 150.0
        
        timestamps = pd.date_range(
            start='2024-01-01 09:30:00',
            end='2024-01-01 16:00:00',
            periods=num_points
        )
        
        # Generate price movements using random walk
        price_changes = np.random.normal(0, 0.01, num_points)
        prices = base_price + np.cumsum(price_changes)
        
        # Generate volumes
        volumes = np.random.exponential(100, num_points).astype(int)
        
        # Create DataFrame
        df = pl.DataFrame({
            'timestamp': timestamps,
            'symbol': [symbol] * num_points,
            'price': prices,
            'volume': volumes,
            'side': np.random.choice(['B', 'S'], num_points)  # Buy/Sell
        })
        
        return df

class IEXVisualizer:
    """Visualize IEX tick data using various plotting libraries."""
    
    def __init__(self, df: pl.DataFrame):
        self.df = df
        
    def plot_price_timeseries(self, output_file: str = "price_timeseries.html", aggregation: str = "minute"):
        """Plot price time series using hvplot with aggregation options."""
        print(f"Creating price time series plot (aggregation: {aggregation})...")
        
        # Convert to pandas for hvplot compatibility
        df_pd = self.df.to_pandas()
        
        if aggregation == "raw":
            # Raw tick data - can be very dense
            plot_data = df_pd
            title = 'Price Time Series (Raw Ticks)'
        elif aggregation == "second":
            # Aggregate to 1-second bars
            plot_data = df_pd.set_index('timestamp').resample('1s').agg({
                'price': 'last',
                'volume': 'sum'
            }).dropna().reset_index()
            title = 'Price Time Series (1-Second Bars)'
        else:  # minute aggregation (default)
            # Aggregate to 1-minute bars to match volume chart
            plot_data = df_pd.set_index('timestamp').resample('1min').agg({
                'price': 'last',
                'volume': 'sum'
            }).dropna().reset_index()
            title = 'Price Time Series (1-Minute Bars)'
        
        plot = plot_data.hvplot.line(
            x='timestamp', 
            y='price',
            title=title,
            width=1000,
            height=400,
            tools=['hover', 'pan', 'wheel_zoom', 'box_zoom', 'reset']
        )
        
        hvplot.save(plot, output_file)
        print(f"Saved price time series to {output_file}")
        return plot
    
    def plot_volume_bars(self, output_file: str = "volume_bars.html"):
        """Plot volume bars using hvplot."""
        print("Creating volume bar chart...")
        
        df_pd = self.df.to_pandas()
        
        # Resample to minute bars for better visualization
        df_resampled = df_pd.set_index('timestamp').resample('1min').agg({
            'volume': 'sum',
            'price': 'last'
        }).dropna()
        
        plot = df_resampled.hvplot.bar(
            y='volume',
            title='Volume by Minute',
            width=1000,
            height=300,
            color='orange',
            alpha=0.7
        )
        
        hvplot.save(plot, output_file)
        print(f"Saved volume bars to {output_file}")
        return plot
    
    def plot_datashader_scatter(self, output_file: str = "datashader_scatter.html"):
        """Create high-performance scatter plot using datashader."""
        print("Creating datashader scatter plot...")
        
        df_pd = self.df.to_pandas()
        
        # Convert timestamp to float (UNIX epoch in seconds)
        # Handle both timezone-aware and timezone-naive datetimes
        if pd.api.types.is_datetime64_any_dtype(df_pd['timestamp']):
            df_pd = df_pd.copy()
            # Convert to UTC first if timezone-aware, then to epoch
            if hasattr(df_pd['timestamp'].dtype, 'tz') and df_pd['timestamp'].dtype.tz is not None:
                df_pd['timestamp_float'] = df_pd['timestamp'].dt.tz_convert('UTC').astype('int64') / 1e9
            else:
                df_pd['timestamp_float'] = df_pd['timestamp'].astype('int64') / 1e9  # seconds
            x_col = 'timestamp_float'
        else:
            x_col = 'timestamp'
        
        # Create datashader canvas
        canvas = ds.Canvas(plot_width=1000, plot_height=600)
        
        # Aggregate points
        agg = canvas.points(df_pd, x_col, 'price')
        
        # Create image
        img = tf.shade(agg, cmap=['lightblue', 'darkblue'])
        
        # Save as PNG
        png_path = str(output_file).replace('.html', '.png')
        img.to_pil().save(png_path)
        print(f"Saved datashader plot to {png_path}")
        return png_path
    
    def plot_ohlc_bars(self, freq: str = '1min', output_file: str = "ohlc_bars.html"):
        """Create OHLC bars from tick data."""
        print(f"Creating OHLC bars with {freq} frequency...")
        
        df_pd = self.df.to_pandas()
        
        # Resample to OHLC
        ohlc = df_pd.set_index('timestamp').resample(freq).agg({
            'price': ['first', 'max', 'min', 'last'],
            'volume': 'sum'
        }).dropna()
        
        # Flatten column names
        ohlc.columns = ['open', 'high', 'low', 'close', 'volume']
        ohlc = ohlc.reset_index()
        
        # Create candlestick plot using plotly
        fig = go.Figure(data=go.Candlestick(
            x=ohlc['timestamp'],
            open=ohlc['open'],
            high=ohlc['high'],
            low=ohlc['low'],
            close=ohlc['close'],
            name='OHLC'
        ))
        
        fig.update_layout(
            title='OHLC Candlestick Chart',
            xaxis_title='Time',
            yaxis_title='Price',
            width=1000,
            height=600
        )
        
        fig.write_html(output_file)
        print(f"Saved OHLC chart to {output_file}")
        return fig
    
    def create_dashboard(self, output_file: str = "dashboard.html"):
        """Create a comprehensive dashboard with multiple visualizations."""
        print("Creating comprehensive dashboard...")
        
        # Create individual plots
        price_plot = self.plot_price_timeseries("temp_price.html")
        volume_plot = self.plot_volume_bars("temp_volume.html")
        
        # Combine plots using Holoviews layout
        import holoviews as hv
        dashboard = hv.Layout([price_plot, volume_plot]).cols(1)  # vertical layout
        hv.save(dashboard, output_file)
        print(f"Saved dashboard to {output_file}")
        return dashboard

def main():
    """Main function to run the IEX data visualization tool."""
    parser = argparse.ArgumentParser(description='IEX Tick Data Visualization Tool')
    parser.add_argument('--file', '-f', type=str, help='Path to CSV file')
    parser.add_argument('--symbol', '-s', type=str, default='AAPL', help='Stock symbol for sample data or filter for CSV data')
    parser.add_argument('--sample', action='store_true', help='Use sample data instead of CSV')
    parser.add_argument('--points', '-p', type=int, default=100000, help='Number of sample points')
    parser.add_argument('--output-dir', '-o', type=str, default='output', help='Output directory for plots')
    parser.add_argument('--filter-symbol', action='store_true', help='Filter CSV data to only show specified symbol')
    parser.add_argument('--top-symbols', type=int, default=0, help='Show only top N symbols by volume (0 = all symbols)')
    
    # Pairs analysis arguments
    parser.add_argument('--pairs-analysis', action='store_true', help='Run pairs analysis instead of visualization')
    parser.add_argument('--file1', type=str, help='Path to first CSV file for pairs analysis')
    parser.add_argument('--file2', type=str, help='Path to second CSV file for pairs analysis')
    parser.add_argument('--freq', type=str, default='1min', 
                        choices=['1s', '5s', '1min', '1hr'],
                        help='Aggregation frequency for pairs analysis')
    
    args = parser.parse_args()
    
    # Handle pairs analysis mode
    if args.pairs_analysis:
        # Import pairs analysis module
        try:
            from pairs_analysis import PairDataLoader, PairAnalyzer
        except ImportError as e:
            print(f"Error importing pairs analysis module: {e}")
            print("Please ensure all dependencies are installed.")
            return 1
        
        # Check if both files are provided
        if not args.file1 or not args.file2:
            print("Error: Both --file1 and --file2 are required for pairs analysis.")
            return 1
        
        # Load data
        print(f"Loading data for pairs analysis...")
        print(f"File 1: {args.file1}")
        print(f"File 2: {args.file2}")
        
        loader = PairDataLoader()
        try:
            df1, df2 = loader.load_csv_pair(args.file1, args.file2)
            print(f"Data loaded: {len(df1)} and {len(df2)} rows")
            
            # Analyze pair
            analyzer = PairAnalyzer(df1, df2)
            results = analyzer.analyze_pair(args.freq)
            
            # Print results
            print("\n=== PAIRS ANALYSIS RESULTS ===")
            print(f"Symbols: {results['symbols'][0]} - {results['symbols'][1]}")
            print(f"Frequency: {results['frequency']}")
            print(f"Data Points: {results['data_points']}")
            
            print("\n--- Cointegration Test ---")
            coint_res = results['cointegration']
            if 'error' in coint_res:
                print(f"Error: {coint_res['error']}")
            else:
                print(f"Cointegration t-statistic: {coint_res['coint_t_statistic']:.4f}")
                print(f"Cointegration p-value: {coint_res['coint_p_value']:.4f}")
                print(f"ADF statistic: {coint_res['adf_statistic']:.4f}")
                print(f"ADF p-value: {coint_res['adf_p_value']:.4f}")
                print(f"Is Cointegrated (p<0.05): {coint_res['is_cointegrated']}")
            
            print("\n--- Correlation Analysis ---")
            corr_res = results['correlation']
            print(f"Pearson correlation: {corr_res['pearson_correlation']:.4f}")
            print(f"Pearson p-value: {corr_res['pearson_p_value']:.4f}")
            print(f"Spearman correlation: {corr_res['spearman_correlation']:.4f}")
            print(f"Spearman p-value: {corr_res['spearman_p_value']:.4f}")
            print(f"Spread mean: {corr_res['spread_mean']:.4f}")
            print(f"Spread std: {corr_res['spread_std']:.4f}")
            print(f"Current spread z-score: {corr_res['spread_z_score']:.4f}")
            
            print("\n--- Hedge Ratio ---")
            print(f"Hedge ratio: {results['hedge_ratio']:.4f}")
            
            # Save results to file
            output_file = output_dir / "pairs_analysis_results.txt"
            with open(output_file, 'w') as f:
                f.write("=== PAIRS ANALYSIS RESULTS ===\n")
                f.write(f"Symbols: {results['symbols'][0]} - {results['symbols'][1]}\n")
                f.write(f"Frequency: {results['frequency']}\n")
                f.write(f"Data Points: {results['data_points']}\n\n")
                
                f.write("--- Cointegration Test ---\n")
                if 'error' in coint_res:
                    f.write(f"Error: {coint_res['error']}\n")
                else:
                    f.write(f"Cointegration t-statistic: {coint_res['coint_t_statistic']:.4f}\n")
                    f.write(f"Cointegration p-value: {coint_res['coint_p_value']:.4f}\n")
                    f.write(f"ADF statistic: {coint_res['adf_statistic']:.4f}\n")
                    f.write(f"ADF p-value: {coint_res['adf_p_value']:.4f}\n")
                    f.write(f"Is Cointegrated (p<0.05): {coint_res['is_cointegrated']}\n\n")
                
                f.write("--- Correlation Analysis ---\n")
                f.write(f"Pearson correlation: {corr_res['pearson_correlation']:.4f}\n")
                f.write(f"Pearson p-value: {corr_res['pearson_p_value']:.4f}\n")
                f.write(f"Spearman correlation: {corr_res['spearman_correlation']:.4f}\n")
                f.write(f"Spearman p-value: {corr_res['spearman_p_value']:.4f}\n")
                f.write(f"Spread mean: {corr_res['spread_mean']:.4f}\n")
                f.write(f"Spread std: {corr_res['spread_std']:.4f}\n")
                f.write(f"Current spread z-score: {corr_res['spread_z_score']:.4f}\n\n")
                
                f.write("--- Hedge Ratio ---\n")
                f.write(f"Hedge ratio: {results['hedge_ratio']:.4f}\n")
            
            print(f"\nResults saved to {output_file}")
            
        except Exception as e:
            print(f"Error during pairs analysis: {e}")
            return 1
        
        return 0
    
    # Create output directory for visualization mode
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Load data for visualization mode
    loader = IEXDataLoader()
    
    if args.sample or not args.file:
        print("Using sample data...")
        df = loader.get_sample_data(args.symbol, args.points)
    else:
        print(f"Loading data from {args.file}...")
        df = loader.load_csv(args.file)
    
    print(f"Data shape: {df.shape}")
    print(f"Data preview:\n{df.head()}")
    
    # Apply filtering if requested
    if not args.sample and args.file:
        if args.filter_symbol and 'symbol' in df.columns:
            original_count = len(df)
            df = df.filter(pl.col('symbol') == args.symbol.upper())
            print(f"\nFiltered to symbol {args.symbol.upper()}: {len(df)} rows (was {original_count})")
            
        elif args.top_symbols > 0 and 'symbol' in df.columns:
            # Get top symbols by total volume
            top_symbols = (
                df.group_by('symbol')
                .agg(pl.col('volume').sum().alias('total_volume'))
                .sort('total_volume', descending=True)
                .head(args.top_symbols)
                .select('symbol')
                .to_series()
                .to_list()
            )
            original_count = len(df)
            df = df.filter(pl.col('symbol').is_in(top_symbols))
            print(f"\nFiltered to top {args.top_symbols} symbols by volume: {top_symbols}")
            print(f"Filtered data: {len(df)} rows (was {original_count})")
            
        # Show symbol distribution
        if 'symbol' in df.columns:
            symbol_counts = df.group_by('symbol').len().sort('len', descending=True)
            print(f"\nSymbol distribution:")
            print(symbol_counts.head(10))
    
    # Create visualizations
    visualizer = IEXVisualizer(df)
    
    # Generate all plots
    visualizer.plot_price_timeseries(output_dir / "price_timeseries.html")
    visualizer.plot_volume_bars(output_dir / "volume_bars.html")
    visualizer.plot_datashader_scatter(output_dir / "datashader_scatter.html")
    visualizer.plot_ohlc_bars(output_file=str(output_dir / "ohlc_bars.html"))
    visualizer.create_dashboard(output_dir / "dashboard.html")
    
    print(f"\nAll visualizations saved to {output_dir}/")
    print("Open the HTML files in your browser to view the plots.")

if __name__ == "__main__":
    main()
