#!/usr/bin/env python3
"""
Pairs Analysis Module for IEX Tick Data

This module provides functionality for pairs trading analysis including:
- Cointegration testing
- Correlation analysis
- Statistical tests for pairs trading
- Aggregation support for different time windows
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import polars as pl
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, List
from scipy import stats
from statsmodels.tsa.stattools import coint, adfuller
from statsmodels.regression.linear_model import OLS
import warnings
import argparse

# Import the IEXDataLoader from main module
from main import IEXDataLoader

class PairDataLoader:
    """Load and process IEX tick data for pairs analysis."""
    
    def __init__(self):
        pass
    
    def load_csv_pair(self, file1_path: str, file2_path: str) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Load two CSV files for pairs analysis."""
        loader = IEXDataLoader()  # Reuse existing loader
        df1 = loader.load_csv(file1_path)
        df2 = loader.load_csv(file2_path)
        return df1, df2
    
    
class PairAnalyzer:
    """Analyze pairs of stocks for cointegration and other statistical relationships."""
    
    def __init__(self, df1: pl.DataFrame, df2: pl.DataFrame):
        self.df1 = df1
        self.df2 = df2
        
    def aggregate_data(self, df: pl.DataFrame, freq: str = '1min') -> pl.DataFrame:
        """Aggregate tick data to specified frequency."""
        # Convert to pandas for resampling
        df_pd = df.to_pandas()
        df_pd.set_index('timestamp', inplace=True)
        
        # Resample based on frequency
        if freq.endswith('s') or freq.endswith('min') or freq.endswith('hr'):
            # Convert frequency to pandas format
            if freq == '1s':
                pd_freq = '1S'
            elif freq == '5s':
                pd_freq = '5S'
            elif freq == '1min':
                pd_freq = '1T'
            elif freq == '1hr':
                pd_freq = '1H'
            else:
                pd_freq = freq
                
            df_resampled = df_pd.resample(pd_freq).agg({
                'price': 'last',
                'volume': 'sum'
            }).dropna()
        else:
            df_resampled = df_pd.resample('1T').agg({
                'price': 'last',
                'volume': 'sum'
            }).dropna()
            
        # Convert back to polars
        df_pl = pl.from_pandas(df_resampled.reset_index())
        return df_pl
    
    def synchronize_data(self, freq: str = '1min') -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Synchronize two dataframes to the same time grid."""
        # Aggregate both dataframes to the same frequency
        df1_agg = self.aggregate_data(self.df1, freq)
        df2_agg = self.aggregate_data(self.df2, freq)
        
        # Merge on timestamp to align data
        merged = df1_agg.join(df2_agg, on='timestamp', how='inner', suffix='_2')
        
        # Separate back into two dataframes with synchronized timestamps
        df1_sync = merged.select([
            pl.col('timestamp'),
            pl.col('price').alias('price_1'),
            pl.col('volume').alias('volume_1')
        ])
        
        df2_sync = merged.select([
            pl.col('timestamp'),
            pl.col('price_2'),
            pl.col('volume_2')
        ])
        
        return df1_sync, df2_sync
    
    def calculate_spread(self, freq: str = '1min') -> pl.DataFrame:
        """Calculate the price spread between two symbols."""
        df1_sync, df2_sync = self.synchronize_data(freq)
        
        # Merge dataframes
        merged = df1_sync.join(df2_sync, on='timestamp', how='inner')
        
        # Calculate spread (log price difference)
        merged = merged.with_columns([
            (pl.col('price_1').log() - pl.col('price_2').log()).alias('log_spread'),
            (pl.col('price_1') - pl.col('price_2')).alias('price_spread')
        ])
        
        return merged
    
    def cointegration_test(self, freq: str = '1min') -> dict:
        """Perform cointegration test on the pair."""
        spread_df = self.calculate_spread(freq)
        
        # Extract price series
        prices1 = spread_df['price_1'].to_numpy()
        prices2 = spread_df['price_2'].to_numpy()
        
        # Perform cointegration test
        try:
            coint_t, p_value, crit_value = coint(prices1, prices2)
            
            # Also perform ADF test on the spread
            log_spread = spread_df['log_spread'].to_numpy()
            adf_result = adfuller(log_spread)
            adf_statistic = adf_result[0]
            adf_p_value = adf_result[1]
            
            return {
                'coint_t_statistic': coint_t,
                'coint_p_value': p_value,
                'critical_values': crit_value,
                'adf_statistic': adf_statistic,
                'adf_p_value': adf_p_value,
                'is_cointegrated': p_value < 0.05 and adf_p_value < 0.05
            }
        except Exception as e:
            return {
                'error': str(e),
                'coint_t_statistic': None,
                'coint_p_value': None,
                'critical_values': None,
                'adf_statistic': None,
                'adf_p_value': None,
                'is_cointegrated': False
            }
    
    def correlation_analysis(self, freq: str = '1min') -> dict:
        """Calculate correlation metrics between the pair."""
        spread_df = self.calculate_spread(freq)
        
        # Extract price series
        prices1 = spread_df['price_1'].to_numpy()
        prices2 = spread_df['price_2'].to_numpy()
        log_spread = spread_df['log_spread'].to_numpy()
        
        # Calculate correlations
        pearson_corr, pearson_p = stats.pearsonr(prices1, prices2)
        spearman_corr, spearman_p = stats.spearmanr(prices1, prices2)
        
        # Calculate spread statistics
        spread_mean = np.mean(log_spread)
        spread_std = np.std(log_spread)
        
        return {
            'pearson_correlation': pearson_corr,
            'pearson_p_value': pearson_p,
            'spearman_correlation': spearman_corr,
            'spearman_p_value': spearman_p,
            'spread_mean': spread_mean,
            'spread_std': spread_std,
            'spread_z_score': (log_spread[-1] - spread_mean) / spread_std if spread_std > 0 else 0
        }
    
    def hedge_ratio(self, freq: str = '1min') -> float:
        """Calculate the hedge ratio using OLS regression."""
        spread_df = self.calculate_spread(freq)
        
        # Extract price series
        prices1 = spread_df['price_1'].to_numpy()
        prices2 = spread_df['price_2'].to_numpy()
        
        # Perform OLS regression to find hedge ratio
        # prices1 = beta * prices2 + epsilon
        model = OLS(prices1, prices2).fit()
        hedge_ratio = model.params[0]
        
        return hedge_ratio
    
    def analyze_pair(self, freq: str = '1min') -> dict:
        """Perform comprehensive analysis of the pair."""
        print(f"Analyzing pair with {freq} frequency...")
        
        # Get synchronized data
        df1_sync, df2_sync = self.synchronize_data(freq)
        print(f"Synchronized data points: {len(df1_sync)}")
        
        # Perform cointegration test
        coint_results = self.cointegration_test(freq)
        
        # Perform correlation analysis
        corr_results = self.correlation_analysis(freq)
        
        # Calculate hedge ratio
        hedge_ratio = self.hedge_ratio(freq)
        
        # Get symbol names if available
        symbol1 = self.df1['symbol'][0] if 'symbol' in self.df1.columns else 'Symbol1'
        symbol2 = self.df2['symbol'][0] if 'symbol' in self.df2.columns else 'Symbol2'
        
        results = {
            'symbols': (symbol1, symbol2),
            'frequency': freq,
            'data_points': len(df1_sync),
            'cointegration': coint_results,
            'correlation': corr_results,
            'hedge_ratio': hedge_ratio
        }
        
        return results


def main():
    """Main function to run pairs analysis."""
    parser = argparse.ArgumentParser(description='IEX Pairs Analysis Tool')
    parser.add_argument('--file1', type=str, required=True, help='Path to first CSV file')
    parser.add_argument('--file2', type=str, required=True, help='Path to second CSV file')
    parser.add_argument('--freq', type=str, default='1min', 
                        choices=['1s', '5s', '1min', '1hr'],
                        help='Aggregation frequency')
    parser.add_argument('--output', type=str, default='pairs_analysis_results.txt',
                        help='Output file for results')
    
    args = parser.parse_args()
    
    # Load data
    print(f"Loading data from {args.file1} and {args.file2}...")
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
        with open(args.output, 'w') as f:
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
        
        print(f"\nResults saved to {args.output}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    main()
