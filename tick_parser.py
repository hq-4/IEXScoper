import os
from IEXTools import Parser, messages

'''
takes raw TOPS files and dumps to csv in the same folder
'''


p = Parser(r'20241231_IEXTP1_TOPS1.6.pcap.gz')