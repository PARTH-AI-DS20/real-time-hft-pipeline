import json
import pandas as pd
import duckdb
from confluent_kafka import Consumer, KafkaError

# --- CONFIGURATION ---
KAFKA_TOPIC = "stock-market"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"
DB_FILE = "stock_market.duckdb"

kafka_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "stock-analyzer-v2",
    "auto.offset.reset": "earliest" 
}

# 1. Initialize Kafka Consumer using list() constructor for stability
kafka_consumer = Consumer(kafka_conf)
target_topics = list()
target_topics.append(KAFKA_TOPIC)
kafka_consumer.subscribe(target_topics)

print(f"[*] Stock Analyzer Active. Processing stream into {DB_FILE}...")

try:
    while True:
        # Micro-batch container
        batch_list = list()
        
        # Pull up to 100 messages for vectorized processing efficiency [1, 2]
        for _ in range(100):
            msg = kafka_consumer.poll(0.5) 
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka Error: {msg.error()}")
                    break
            
            # Parse JSON message into Python dictionary
            batch_list.append(json.loads(msg.value().decode("utf-8")))

        if len(batch_list) > 0:
            # 2. Vectorized Transformation (Silver Layer)
            df = pd.DataFrame(batch_list)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # REAL-TIME METRIC: Calculate 20-period Simple Moving Average (SMA)
            # We group by symbol to ensure technical indicators are node-specific [1, 3]
            df["sma_20"] = df.groupby("symbol")["price"].transform(
                lambda x: x.rolling(window=20, min_periods=1).mean()
            )
            
            # 3. Micro-Transaction Write Pattern
            # We open and close the connection for every batch to release the 
            # OS lock, allowing the Streamlit dashboard to read the file 
            with duckdb.connect(DB_FILE) as db_conn:
                db_conn.execute("""
                    CREATE TABLE IF NOT EXISTS stock_analytics (
                        timestamp TIMESTAMP,
                        symbol VARCHAR,
                        price DOUBLE,
                        volume INTEGER,
                        bid_price DOUBLE,
                        ask_price DOUBLE,
                        sma_20 DOUBLE
                    )
                """)
                
                # Native DuckDB append is optimized for Pandas DataFrames [4, 5]
                db_conn.append('stock_analytics', df)
                
                # Progress logging
                total_count = db_conn.execute('SELECT count(*) FROM stock_analytics').fetchone()
                print(f"[+] Stored {len(df)} ticks. Total DB Records: {total_count}")

except KeyboardInterrupt:
    print("\n[!] Shutting down consumer...")
finally:
    kafka_consumer.close()