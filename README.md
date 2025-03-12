# IEXScoper
IEX Tick Data Viewer and Order Book Visualizer  

---

## Install
Run:
```bash
cp example.config.py config.py
```
to configure secrets.

---

## Data Flow Overview
1. **Capture & Download:** IEX PCAP files are stored.  
2. **Parsing & Transformation:** PCAPs → CSV → Parquet (Quotes & Trades).  
3. **Storage & Querying:**  
   - **Xeon**: Order book reconstruction (historical on spinning disks, recent in MergeTree).  
   - **5950X**: Trade & event analysis (fully in MergeTree).  
4. **Webserver:** Displays PCAP processing status.  
5. **Analysis:** ClickHouse distributed queries run across both servers.  

---

## Structure
### **Parser**
Parses PCAP files into CSV and **generates two Parquet files per trading day**:
- **Quote Updates (tick quotes)**
- **Trades & Events (everything else)**

### **Webserver**
Tracks the status of:
- **Downloaded PCAP files**
- **Parsing progress**
- **Query access to parsed data** (optional API)

### **Analytical Servers**
| **Server**  | **Role** | **Data Stored** | **Format** |
|------------|--------|--------------|------------|
| **5950X / 128GB RAM** | Trades & Events | **Trade Data** | **MergeTree** |
| **Dual Xeon / 256GB RAM** | Order Book Reconstruction | **Quote Updates** | **Parquet (historical) / MergeTree (YTD)** |

---

## Distributed ClickHouse Querying
- Queries hit **both servers** to analyze **spoofing, market microstructure, and anomalies.**
- **Query Optimizations:**
  - **Partitioning** (`toYYYYMMDD(trade_time)`)  
  - **Primary ordering** (`symbol, trade_time, order_id`)  
  - **Materialized views** for common aggregations  
  - **Bloom filter indexes** for specific queries  

---

## Storage Strategy
| **Data Type**  | **Server**  | **Format** | **Retention Strategy** |
|---------------|------------|------------|------------------------|
| **Quote Updates** | **Xeon** | **Parquet (Historical) / MergeTree (Recent)** | YTD in MergeTree, older in Parquet (spinning disks) |
| **Trade Data** | **5950X** | **MergeTree** | Full dataset stored in MergeTree |
| **Raw PCAP Files** | **Xeon** | **PCAP** | Retained for 30 days before archival |

---

## Future Enhancements
- [ ] Implement **real-time updates** for new trading days  
- [ ] Add **ClickHouse-based API** for querying parsed data  
- [ ] Improve **order book visualization** using WebSockets  
- [ ] Optimize **distributed query execution**  
