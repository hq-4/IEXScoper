import os
import sys
import pandas as pd
from dataclasses import asdict
import config
from IEXTools import Parser, messages
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import urllib.parse

class PCAPProcessor:
    # Schema definitions for the output files.
    QUOTE_SCHEMA_COLUMNS = [
        "type",
        "timestamp",
        "symbol",
        "flags",
        "bid_size",
        "bid_price_int",
        "ask_price_int",
        "ask_size",
        "bid_price",
        "ask_price"
    ]

    OTHER_SCHEMA_COLUMNS = [
        "type",
        "timestamp",
        "symbol",
        "system_event",
        "system_event_str",
        "flags",
        "round_lot_size",
        "adjusted_poc_close",
        "luld_tire",
        "price",
        "status",
        "reason",
        "trading_status_message",
        "retail_liquidity_indicator",
        "halt_status",
        "short_sale_status",
        "detail",
        "size",
        "price_int",
        "trade_id",
        "price_type",
        "sale_flags"
    ]

    # Desired data types for QuoteUpdate messages.
    QUOTE_DTYPE_MAPPING = {
        "type": "string",
        "timestamp": "int64",
        "symbol": "string",
        "flags": "Int64",          # Using pandas nullable integer type.
        "bid_size": "Int64",
        "bid_price_int": "Int64",
        "ask_price_int": "Int64",
        "ask_size": "Int64",
        "bid_price": "float64",
        "ask_price": "float64"
    }

    # Desired data types for non-QuoteUpdate messages.
    OTHER_DTYPE_MAPPING = {
        "type": "string",
        "timestamp": "int64",
        "symbol": "string",
        "system_event": "float64",  # Using float64 so that NaN is allowed.
        "system_event_str": "string",
        "flags": "Int64",
        "round_lot_size": "Int64",
        "adjusted_poc_close": "Int64",
        "luld_tire": "Int64",
        "price": "float64",
        "status": "string",
        "reason": "string",
        "trading_status_message": "string",
        "retail_liquidity_indicator": "string",
        "halt_status": "string",
        "short_sale_status": "float64",
        "detail": "string",
        "size": "Int64",
        "price_int": "Int64",
        "trade_id": "Int64",
        "price_type": "string",
        "sale_flags": "string"
    }

    def __init__(self, source: str, delete_after_processing: bool = False):
        """
        Initialize the processor with a source. The source can be either a URL (with expiring links)
        or a local filename. If a URL is provided, the file will be downloaded into the working folder
        (config.IE_FOLDER) using caching (i.e. it is not re-downloaded if already present).

        The delete_after_processing toggle controls whether the PCAP file should be removed after
        processing into Parquet files.
        """
        self.delete_after_processing = delete_after_processing
        # Set default status to True (expecting well-formed PCAPs)
        self.status = True  
        if source.startswith("http://") or source.startswith("https://"):
            self.pcap_filepath = self.download_from_url(source)
        else:
            self.pcap_filepath = self._get_pcap_filepath(source)
        self._quote_filepath = None
        self._other_filepath = None

    @staticmethod
    def _get_pcap_filepath(filename: str) -> str:
        """
        Returns the absolute path to the PCAP file.
        The file is expected to reside under config.IE_FOLDER.
        """
        if filename.startswith(config.IE_FOLDER):
            path = filename
        else:
            path = os.path.join(config.IE_FOLDER, filename)
        return os.path.abspath(path)

    @staticmethod
    def _get_output_folder(filename: str) -> str:
        """
        Given the PCAP filename (e.g. "20171025_IEXTP1_TOPS1.6.pcap.gz"),
        extract the date from the first eight characters (YYYYMMDD) and return
        the output folder as "pq/YYYY/MM" (based on config.PQ_FOLDER).
        The folder is created if it doesn't exist.
        """
        base_filename = os.path.basename(filename)
        date_str = base_filename[:8]  # Assuming filename starts with YYYYMMDD
        year = date_str[:4]
        month = date_str[4:6]
        out_dir = os.path.abspath(os.path.join(config.PQ_FOLDER, year, month))
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    @staticmethod
    def _prepare_output_file(filepath: str):
        """
        Remove the output file if it already exists so that we always start fresh.
        """
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"Removed existing file: {filepath}")

    @staticmethod
    def _flush_buffer(buffer, writer, filepath, schema_columns, dtype_mapping, chunk_desc):
        """
        Given a buffer (list of dictionaries), create a DataFrame with columns ordered
        as schema_columns, cast the columns to the desired types, and write the chunk
        using a PyArrow ParquetWriter. If no writer exists, one is created (which in turn
        creates the file on disk). Returns a tuple of (writer, number_of_rows_written).
        """
        if not buffer:
            return writer, 0
        df = pd.DataFrame(buffer)
        df = df.reindex(columns=schema_columns)
        try:
            df = df.astype(dtype_mapping)
        except Exception as e:
            print("Error casting DataFrame types:", e)
            raise
        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = pq.ParquetWriter(filepath, table.schema)
            print(f"Created new Parquet file: {filepath}")
        writer.write_table(table)
        n_rows = len(df)
        print(f"Appended {n_rows} rows to {filepath} ({chunk_desc})")
        buffer.clear()
        return writer, n_rows

    @staticmethod
    def download_from_url(url: str) -> str:
        """
        Given a URL (which may have expiring links), resolve the filename as wget would
        (by decoding the URL path) and download the file into config.IE_FOLDER.
        If the file already exists, the cached copy is used.
        """
        parsed = urllib.parse.urlparse(url)
        # Decode the path, e.g. "data%2Ffeeds%2F20171028%2F20171028_IEXTP1_TOPS1.5.pcap.gz" becomes
        # "data/feeds/20171028/20171028_IEXTP1_TOPS1.5.pcap.gz"
        path_decoded = urllib.parse.unquote(parsed.path)
        filename = os.path.basename(path_decoded)
        dest_folder = config.IE_FOLDER
        os.makedirs(dest_folder, exist_ok=True)
        dest_filepath = os.path.join(dest_folder, filename)
        if os.path.exists(dest_filepath):
            print(f"File already exists at {dest_filepath}, using cached version.")
            return os.path.abspath(dest_filepath)
        print(f"Downloading file from {url} to {dest_filepath}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dest_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            print(f"Download complete: {dest_filepath}")
            return os.path.abspath(dest_filepath)
        else:
            raise Exception(f"Failed to download file. Status code: {response.status_code}")

    def process_file(self):
        """
        Process the PCAP file by splitting messages into QuoteUpdate and non-QuoteUpdate
        categories. The output files are written to nested folders based on the date embedded
        in the input filename.

        - QuoteUpdate messages are flushed every 50,000 rows.
        - All other messages are flushed every 10,000 rows.

        After processing, sets self._quote_filepath and self._other_filepath to the filepaths
        of the respective Parquet files, and returns a tuple of these filepaths.

        If the delete_after_processing toggle is enabled, the PCAP file is deleted after processing.

        If a serious error occurs during parsing (indicating a malformed PCAP), self.status is
        set to False.
        """
        base_filename = os.path.basename(self.pcap_filepath)
        if base_filename.endswith('.pcap.gz'):
            base = base_filename[:-len('.pcap.gz')]
        else:
            base = os.path.splitext(base_filename)[0]

        # Determine the output folder based on the date in the filename.
        output_folder = self._get_output_folder(base_filename)

        # Define the full output file paths.
        quote_filepath = os.path.join(output_folder, f"{base}_QuoteUpdate.parquet")
        other_filepath = os.path.join(output_folder, f"{base}.parquet")

        # Store filepaths as instance variables.
        self._quote_filepath = quote_filepath
        self._other_filepath = other_filepath

        # Remove any pre-existing output files.
        self._prepare_output_file(quote_filepath)
        self._prepare_output_file(other_filepath)

        # Set chunk sizes.
        quote_chunk_size = 50000
        other_chunk_size = 10000

        # Initialize buffers, writer handles, and cumulative counters.
        quote_buffer = []
        other_buffer = []
        writer_quote = None
        writer_other = None
        total_quote_rows = 0
        total_other_rows = 0

        parser = Parser(self.pcap_filepath)

        try:
            # Main processing loop.
            while True:
                try:
                    msg = parser.get_next_message()
                except EOFError as e:
                    print(f"EOF reached: {e}")
                    break
                if msg is None:
                    break

                # Convert the message to a dictionary and add its type.
                msg_dict = asdict(msg)
                msg_type = msg.__class__.__name__
                msg_dict["type"] = msg_type

                if msg_type == "QuoteUpdate":
                    # Normalize row to the QuoteUpdate schema.
                    row = {col: msg_dict.get(col, None) for col in self.QUOTE_SCHEMA_COLUMNS}
                    quote_buffer.append(row)
                    if len(quote_buffer) >= quote_chunk_size:
                        writer_quote, n_rows = self._flush_buffer(
                            quote_buffer, writer_quote, quote_filepath,
                            self.QUOTE_SCHEMA_COLUMNS, self.QUOTE_DTYPE_MAPPING, "QuoteUpdate"
                        )
                        total_quote_rows += n_rows
                        print(f"Cumulative QuoteUpdate rows written: {total_quote_rows}")
                else:
                    # Normalize row to the Other Messages schema.
                    row = {col: msg_dict.get(col, None) for col in self.OTHER_SCHEMA_COLUMNS}
                    other_buffer.append(row)
                    if len(other_buffer) >= other_chunk_size:
                        writer_other, n_rows = self._flush_buffer(
                            other_buffer, writer_other, other_filepath,
                            self.OTHER_SCHEMA_COLUMNS, self.OTHER_DTYPE_MAPPING, "Other Messages"
                        )
                        total_other_rows += n_rows
                        print(f"Cumulative Other Message rows written: {total_other_rows}")

        except Exception as e:
            self.status = False
            print("Serious error occurred during processing:", e)
        else:
            self.status = True
        finally:
            # Flush any remaining rows.
            try:
                writer_quote, n_rows = self._flush_buffer(
                    quote_buffer, writer_quote, quote_filepath,
                    self.QUOTE_SCHEMA_COLUMNS, self.QUOTE_DTYPE_MAPPING, "final QuoteUpdate"
                )
                total_quote_rows += n_rows
                print(f"Cumulative QuoteUpdate rows written: {total_quote_rows}")
            except Exception as e:
                print("Error flushing quote buffer:", e)
            try:
                writer_other, n_rows = self._flush_buffer(
                    other_buffer, writer_other, other_filepath,
                    self.OTHER_SCHEMA_COLUMNS, self.OTHER_DTYPE_MAPPING, "final Other Messages"
                )
                total_other_rows += n_rows
                print(f"Cumulative Other Message rows written: {total_other_rows}")
            except Exception as e:
                print("Error flushing other buffer:", e)

            # Finalize the Parquet writers.
            if writer_quote is not None:
                writer_quote.close()
                print(f"Finalized file: {quote_filepath}")
            else:
                print("No QuoteUpdate messages were written.")

            if writer_other is not None:
                writer_other.close()
                print(f"Finalized file: {other_filepath}")
            else:
                print("No other messages were written.")

            # Delete the PCAP file after processing if the toggle is enabled.
            if self.delete_after_processing:
                try:
                    os.remove(self.pcap_filepath)
                    print(f"Deleted source PCAP file: {self.pcap_filepath}")
                except Exception as e:
                    print("Error deleting PCAP file:", e)

        return quote_filepath, other_filepath

    @property
    def QuoteUpdate(self) -> str:
        """
        Returns the filepath of the QuoteUpdate Parquet file.
        """
        return self._quote_filepath

    @property
    def Other(self) -> str:
        """
        Returns the filepath of the Other Messages Parquet file.
        """
        return self._other_filepath


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <pcap_filename_or_url>")
        sys.exit(1)

    # Example usage:
    # Pass a URL or local filename as the first argument.
    # Optionally, set the delete_after_processing toggle (here, True).
    processor = PCAPProcessor(sys.argv[1], delete_after_processing=True)
    print(f"Processing PCAP file: {processor.pcap_filepath}")
    quote_path, other_path = processor.process_file()
    print("Processing complete.")
    print("QuoteUpdate file:", quote_path)
    print("Other Messages file:", other_path)
    print("Processing status:", processor.status)


