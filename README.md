# IEXScoper
IEX Tick Data viewer and order book visualizer 


# Structure
This is split into 3 sections:

## Parser
This parses the pcap files into its constituent csv files and then ingests into our db.

## Webserver
Webserver that should analyze tick data on a day by day basis with filters for tickers

## Order Book
Order book viewer inside the web server