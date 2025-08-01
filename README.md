# IEX Tick Data Visualization and Pairs Analysis Tool

A Python project for loading, visualizing, and analyzing tick-level stock data from IEX using Polars, datashader, and modern visualization libraries. The tool also provides pairs trading analysis capabilities with cointegration testing and statistical analysis.

## Features

- **High-performance data loading** with Polars
- **Scalable visualization** using datashader for large datasets
- **Multiple chart types**: time series, volume bars, OHLC candlesticks
- **Interactive dashboards** with hvplot and bokeh
- **Sample data generation** for testing
- **Flexible CSV input** support
- **Pairs trading analysis** with cointegration testing and statistical analysis
- **Multiple aggregation frequencies** for pairs analysis (1s, 5s, 1min, 1hr)

## Setup

### Prerequisites

- Python 3.8+
- [uv](https://github.com/astral-sh/uv) package manager

### Windows Compatibility

This tool is fully compatible with Windows operating systems. When using Windows:

1. **Path Separators**: The tool automatically handles path separators, so you can use either forward slashes (/) or backslashes (\\) in file paths.
2. **Line Endings**: CSV files with either Unix (LF) or Windows (CRLF) line endings are supported.
3. **File Permissions**: Ensure that the user running the tool has read permissions for input files and write permissions for the output directory.

For best results on Windows:
- Use the latest version of Python 3.8 or higher
- Install dependencies using `uv` as described in the setup instructions
- When specifying file paths, you can use either format:
  - Forward slashes: `--file data/ticks.csv`
  - Backslashes: `--file data\ticks.csv` (escape backslashes in some shells)
  - Raw strings: `--file data\ticks.csv` (in PowerShell)

### Installation

1. Clone or navigate to the project directory:
   ```bash
   git clone <repository-url>
   cd IEXScoper
   ```

2. Install dependencies using [uv](https://github.com/astral-sh/uv):
   ```bash
   uv pip install -r requirements.txt
   ```

3. Run the visualization tool:
   ```bash
   python main.py --file data/your_ticks.csv --output-dir plots
   ```

For sample data:
   ```bash
   python main.py --sample --output-dir plots
   ```

### Windows Examples

For Windows users, here are some example commands:

1. **Visualization**:
   ```cmd
   python main.py --file data\ticks.csv --output-dir plots
   ```

2. **Pairs Analysis**:
   ```cmd
   python main.py --pairs-analysis --file1 data\stock1.csv --file2 data\stock2.csv --freq 1min
   ```

3. **Using PowerShell** (note the backtick for line continuation):
   ```powershell
   python main.py --pairs-analysis `
                 --file1 data\stock1.csv `
                 --file2 data\stock2.csv `
                 --freq 1min
   ```

### Command Line Options

- `--file, -f`: Path to CSV file containing tick data
- `--symbol, -s`: Stock symbol for sample data (default: AAPL)
- `--sample`: Use generated sample data instead of CSV
- `--points, -p`: Number of sample data points (default: 100,000)
- `--output-dir, -o`: Output directory for generated plots (default: output)
- `--filter-symbol`: Filter CSV data to only show specified symbol
- `--top-symbols`: Show only top N symbols by volume (0 = all symbols)

### Pairs Analysis Options

- `--pairs-analysis`: Run pairs analysis instead of visualization
- `--file1`: Path to first CSV file for pairs analysis
- `--file2`: Path to second CSV file for pairs analysis
- `--freq`: Aggregation frequency for pairs analysis (1s, 5s, 1min, 1hr)

### Expected CSV Format

The tool specifically supports the IEX TP1 DEEP1.0 format with columns:
- `Exchange Timestamp`: Primary timestamp (preferred)
- `Packet Capture Time`: Alternative timestamp
- `Send Time`: Alternative timestamp
- `Symbol`: Stock symbol
- `Price`: Tick price
- `Size`: Tick volume
- `Tick Type`: Type of tick
- `Trade ID`: Trade identifier
- `Sale Condition`: Sale condition flags

### Visualization Types

1. **Price Time Series**: Interactive line chart of price movements
2. **Volume Bars**: Bar chart of trading volume over time
3. **Datashader Scatter**: High-performance scatter plot for large datasets
4. **OHLC Bars**: Candlestick-style bars showing open/high/low/close prices
5. **Dashboard**: Combined view of all visualizations

## Project Structure

```
IEXScoper/
├── main.py              # Main application script
├── pairs_analysis.py     # Pairs trading analysis module
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── .gitignore          # Git ignore patterns
├── data/               # Directory for CSV files
├── output/             # Generated visualizations
└── old/                # Legacy files
```

## Dependencies

### Core Libraries
- **polars**: High-performance DataFrame library
- **numpy**: Numerical computing
- **pyarrow**: Apache Arrow library for Polars

### IEX Data Parser
- **iex-parser**: IEX data parsing library (from GitHub: https://github.com/hq-4/iex-parser.git)

### Visualization Libraries
- **datashader**: Large dataset visualization
- **holoviews**: Declarative data visualization
- **hvplot**: High-level plotting interface
- **bokeh**: Interactive web-based plotting
- **plotly**: Interactive plotting library
- **panel**: Dashboard creation

### Statistical Libraries
- **scipy**: Scientific computing and statistical tests
- **statsmodels**: Statistical models and tests (cointegration)
- **pandas**: Data manipulation (for compatibility)

## Performance Notes

- **Polars** is used for fast data loading and processing
- **Datashader** enables visualization of millions of data points
- **Memory usage** is optimized for large tick datasets
- **Interactive plots** are saved as HTML files for easy sharing

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure all dependencies are installed with `uv pip install -r requirements.txt`
2. **Memory issues**: Reduce the number of data points or use datashader for large datasets
3. **Date parsing errors**: Check your CSV timestamp format

### Getting Help

- Check the console output for detailed error messages
- Ensure your CSV file follows the expected format
- Try running with sample data first to verify the setup

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is open source. Please check individual dependency licenses for their terms.