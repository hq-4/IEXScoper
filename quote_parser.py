import os
import sys
import pandas as pd
import config
from IEXTools import Parser, messages

def get_pcap_filepath(filename: str) -> str:
    """Returns the full path of a PCAP file by appending the filename to PCAP_FOLDER."""
    return os.path.abspath(os.path.join(config.PCAP_FOLDER, filename))

def convert_message_to_dict(msg):
    """
    Converts a message to a dictionary.
    Since these message classes use __slots__, we need to extract the attributes manually.
    """
    return {slot: getattr(msg, slot) for slot in msg.__slots__}

def process_pcap_file(pcap_filepath: str):
    """
    Process the pcap file by splitting messages into QuoteUpdate and all other types,
    then writing each group to a parquet file.
    """
    # Determine the base filename (e.g. "20171025_IEXTP1_TOPS1.6" from "20171025_IEXTP1_TOPS1.6.pcap.gz")
    base_filename = os.path.basename(pcap_filepath)
    if base_filename.endswith('.pcap.gz'):
        base = base_filename[:-len('.pcap.gz')]
    else:
        base = os.path.splitext(base_filename)[0]

    # Define output file names based on the rules:
    # - QuoteUpdate file: append _QuoteUpdate.parquet to the base
    # - Other messages file: simply use the base with .parquet
    quote_update_filename = f"{base}_QuoteUpdate.parquet"
    other_filename = f"{base}.parquet"

    # Prepare lists to collect messages
    quote_updates = []
    other_messages = []

    # Instantiate the parser for the given pcap file.
    parser = Parser(pcap_filepath)

    # Iterate over all messages
    while True:
        msg = parser.get_next_message()
        if msg is None:
            break
        # Check if the message is a QuoteUpdate message.
        if isinstance(msg, messages.QuoteUpdate):
            quote_updates.append(convert_message_to_dict(msg))
        else:
            other_messages.append(convert_message_to_dict(msg))

    # Convert lists of dictionaries to pandas DataFrames and write them to parquet.
    if quote_updates:
        df_quote = pd.DataFrame(quote_updates)
        df_quote.to_parquet(quote_update_filename, index=False)
        print(f"Written {len(df_quote)} QuoteUpdate messages to {quote_update_filename}")
    else:
        print("No QuoteUpdate messages found.")

    if other_messages:
        df_other = pd.DataFrame(other_messages)
        df_other.to_parquet(other_filename, index=False)
        print(f"Written {len(df_other)} other messages to {other_filename}")
    else:
        print("No other messages found.")

if __name__ == "__main__":
    # Expecting the PCAP filename as a command-line argument.
    if len(sys.argv) < 2:
        print("Usage: python script.py <pcap_filename>")
        sys.exit(1)

    # Build the full path to the pcap file.
    pcap_file = get_pcap_filepath(sys.argv[1])
    print(f"Processing PCAP file: {pcap_file}")

    # Process the file.
    process_pcap_file(pcap_file)
