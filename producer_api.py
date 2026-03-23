import asyncio
import json
import random
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from confluent_kafka import Producer
from pydantic import BaseModel, Field

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockProducer")

# --- CONFIGURATION (Validated in Image 10) ---
KAFKA_TOPIC = "stock-market"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"

# Production-grade Producer Tuning for High Velocity [1, 2]
kafka_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "hft-stock-producer",
    "acks": 1,                      # Leader ack for optimal throughput/reliability balance
    "linger.ms": 10,                # Buffer messages for 10ms to maximize batch sizes [2]
    "batch.size": 131072,           # 128KB batch size for high-velocity logs [1]
    "compression.type": "lz4",      # Real-time streaming compression
    "queue.buffering.max.messages": 1000000 
}

# --- DATA SCHEMA (Bronze Layer) ---
class StockTick(BaseModel):
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    symbol: str
    price: float
    volume: int
    bid_price: float
    ask_price: float

# Initial market state
market_data = {
    "NVDA": {"price": 130.0, "volatility": 0.5},
    "AAPL": {"price": 220.0, "volatility": 0.3},
    "TSLA": {"price": 250.0, "volatility": 0.8},
    "BTC":  {"price": 65000.0, "volatility": 15.0}
}

producer = Producer(kafka_config)

def delivery_report(err, msg):
    """Callback for message delivery updates"""
    if err is not None:
        logger.error(f"Delivery failed: {err}")

async def simulate_market():
    """Background task simulating Random Walk (Brownian Motion) stock prices"""
    logger.info(f"[*] Starting Market Simulation on topic: {KAFKA_TOPIC}")
    
    while True:
        for symbol, data in market_data.items():
            # Calculate price movement
            change = random.uniform(-data["volatility"], data["volatility"])
            data["price"] = round(max(1.0, data["price"] + change), 2)
            
            # Generate Tick Data
            tick = StockTick(
                symbol=symbol,
                price=data["price"],
                volume=random.randint(1, 500),
                bid_price=round(data["price"] - 0.05, 2),
                ask_price=round(data["price"] + 0.05, 2)
            )
            
            # Produce to Kafka
            try:
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=symbol,
                    value=tick.model_dump_json().encode("utf-8"),
                    callback=delivery_report
                )
            except BufferError:
                producer.poll(1) # Flush buffer if it's full
                
        # poll(0) serves delivery reports without blocking the simulation loop
        producer.poll(0)
        
        # High Velocity: Sleep 0.01s = 100 cycles/sec (400 total ticks/sec)
        await asyncio.sleep(0.01)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP: Initialize background task
    simulation_task = asyncio.create_task(simulate_market())
    yield
    # SHUTDOWN: Cleanup
    simulation_task.cancel()
    producer.flush()
    logger.info("[!] Producer flushed and simulation stopped.")

app = FastAPI(lifespan=lifespan)

@app.get("/market-status")
async def get_status():
    return {"status": "live", "topic": KAFKA_TOPIC, "monitored_assets": list(market_data.keys())}