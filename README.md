# IEXScoper

IEXScoper is a Python project for ingestion and analytics on IEX tick data. The current active package builds per-second, per-symbol trade aggregates from parsed IEX trade CSV files and writes derived Parquet outputs.

## Features

- **High-performance CSV scanning and aggregation** with Polars
- **Per-second trade aggregates** by symbol
- **VWAP, mean price, share volume, and trade count** outputs
- **Session labels** for pre-market, regular, and after-hours trading
- **Staging Parquet parts** partitioned by symbol
- **Yearly master Parquet compaction**
- **uv-managed dependencies** through `pyproject.toml` and `uv.lock`

## Setup

### Prerequisites

- Python 3.12+
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
   uv sync
   ```

3. Configure input/output roots:
   ```bash
   cp .env.example .env
   # edit .env with IEX_CSV_ROOT, IEX_PARQUET_ROOT, IEX_WORK_ROOT, and IEX_REPORT_ROOT
   ```

4. Run the CLI:
   ```bash
   uv run iexscoper aggregate-per-second --year 2025 --limit-days 1 --dry-run
   ```

To compact staging output into a yearly master Parquet file:

```bash
uv run iexscoper compact-master --year 2025
```

To validate TOPS ingestion before a broad backfill:

```bash
uv run iexscoper tops-spec-audit --report-root reports
uv run iexscoper validate-tops-ingest --dry-run
uv run iexscoper validate-tops-ingest --all-available --start-day 20250101
```

`IEX_WORK_ROOT` controls PCAP scratch space and fallback CSV/Parquet locations. `IEX_REPORT_ROOT`
controls validation metrics and audit reports. Explicit `--work-root` and `--report-root` CLI
arguments override the `.env` values.

### Windows Examples

For Windows users, here are some example commands:

1. **Install dependencies**:
   ```cmd
   uv sync
   ```

2. **Aggregate one dry-run day**:
   ```cmd
   uv run iexscoper aggregate-per-second --year 2025 --limit-days 1 --dry-run
   ```

3. **Using PowerShell** (note the backtick for line continuation):
   ```powershell
   uv run iexscoper aggregate-per-second `
                 --year 2025 `
                 --symbols AAPL,MSFT `
                 --limit-days 10
   ```

### Command Line Options

Aggregate parsed trade CSVs into per-second staging Parquet files:

```bash
uv run iexscoper aggregate-per-second --year 2025 [--symbols AAPL,MSFT] [--rebuild] [--dry-run] [--limit-days 10]
```

Compact staging Parquet files into the yearly master file:

```bash
uv run iexscoper compact-master --year 2025
```

Validate TOPS discovery, download, PCAP parsing, raw Parquet conversion, and per-second aggregation:

```bash
uv run iexscoper validate-tops-ingest [--days 20250102,20250407] [--dry-run] [--all-available]
```

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
├── pyproject.toml       # Package metadata and dependencies
├── uv.lock              # Locked uv dependency graph
├── README.md           # This file
├── .gitignore          # Git ignore patterns
├── src/                # Active application package
├── iex-parser/         # Bundled IEX PCAP-to-CSV parser
└── old/                # Legacy files
```

## Dependencies

### Core Libraries
- **polars**: High-performance DataFrame library
- **numpy**: Numerical computing
- **pyarrow**: Apache Arrow library for Polars

### IEX Data Parser
- **iex-parser**: IEX data parsing library (from GitHub: https://github.com/hq-4/iex-parser.git)

## Performance Notes

- **Polars** is used for fast data loading and aggregation
- **PyArrow** is used for Parquet writing
- **Zstandard** compression is used for derived Parquet outputs

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure all dependencies are installed with `uv sync`
2. **Memory issues**: Use `--symbols` or `--limit-days` for smaller runs while validating configuration
3. **Date parsing errors**: Check your CSV timestamp format

### Getting Help

- Check the console output for detailed error messages
- Ensure your CSV file follows the expected format
- Try running with sample data first to verify the setup

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is open source. Please check individual dependency licenses for their terms.
