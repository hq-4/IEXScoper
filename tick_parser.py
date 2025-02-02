import os
import config
from IEXTools import Parser, messages

'''
takes raw TOPS files and dumps to csv in the same folder
'''

def get_pcap_filepath(filename: str) -> str:
    """Returns the full path of a PCAP file by appending the filename to PCAP_FOLDER."""
    return os.path.abspath(os.path.join(config.PCAP_FOLDER, filename))


### iterate over every message when IEX Parser is instantiated
def iter_trade_reports(parser):
    allowed = [messages.TradeReport]
    while True:
        msg = parser.get_next_message(allowed)
        if msg is None:
            break
        yield msg



if __name__ == "__main__":

    pcap_file_name = get_pcap_filepath("20241231_IEXTP1_TOPS1.6.pcap.gz")
    print(pcap_file_name)
    p = Parser(pcap_file_name)

    for trade in iter_trade_reports(p):
        print(trade)
