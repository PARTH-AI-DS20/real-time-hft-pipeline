import asyncio
import httpx
import time
import random

# UPDATE THIS: Ensure this matches your FastAPI ingest route
FASTAPI_URL = "http://localhost:8000/ingest" 

async def send_tick(client, symbol):
    payload = {
        "symbol": symbol,
        "price": round(random.uniform(130.0, 150.0), 2),
        "volume": random.randint(1, 100),
        "timestamp": time.time()
    }
    try:
        # Timeout set short; in HFT, late data is dead data
        await client.post(FASTAPI_URL, json=payload, timeout=2.0)
    except Exception as e:
        pass # Ignore dropped connections during stress testing

async def blast_requests(target_rps: int, test_duration_sec: int):
    print(f"Initiating Stress Test: {target_rps} Ticks/Sec for {test_duration_sec} Seconds.")
    
    async with httpx.AsyncClient() as client:
        for second in range(test_duration_sec):
            start_time = time.time()
            
            # Fire concurrent tasks
            tasks = [send_tick(client, "NVDA") for _ in range(target_rps)]
            await asyncio.gather(*tasks)
            
            # Throttle to exactly 1 second intervals to maintain precise RPS
            elapsed = time.time() - start_time
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
            
            print(f"Second {second + 1}: Deployed {target_rps} ticks. (Latency: {elapsed:.3f}s)")

if __name__ == "__main__":
    # STRATEGIC DIRECTIVE: Change the 100 to 500, then 1000, until failure.
    TARGET_TICKS_PER_SECOND = 1000 
    DURATION_IN_SECONDS = 15
    
    asyncio.run(blast_requests(TARGET_TICKS_PER_SECOND, DURATION_IN_SECONDS))