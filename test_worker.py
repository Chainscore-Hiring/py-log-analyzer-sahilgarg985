import asyncio
from worker import Worker

async def test_worker():
    worker = Worker(worker_id="worker_1", coordinator_url="http://localhost:8000")
    # Test processing a small chunk of logs
    metrics = await worker.process_chunk("sample_logs.txt", start=0, size=100)
    print("Metrics:", metrics)

# Run the test
asyncio.run(test_worker())
