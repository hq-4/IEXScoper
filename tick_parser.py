import os
from IEXTools import Parser, messages

'''
takes raw TOPS files and dumps to csv in the same folder
'''

def get_pcap_filepath(filename: str) -> str:
    """Returns the full path of a PCAP file by appending the filename to PCAP_FOLDER."""
    return os.path.join(config.PCAP_FOLDER, filename)





if __name__ == "__main__":

    fn = get_pcap_filepath("20241231_IEXTP1_TOPS1.6.pcap.gz")
    print(fn)


	# p = Parser(r'20241231_IEXTP1_TOPS1.6.pcap.gz')