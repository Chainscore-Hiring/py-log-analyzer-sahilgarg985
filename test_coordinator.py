import asyncio
from coordinator import Coordinator

async def test_coordinator():
    coordinator = Coordinator(port=8000)
    
    # Add workers (simulate their registration)
    coordinator.workers = {
        "worker_1": {"url": "http://localhost:8001", "status": "available"},
        "worker_2": {"url": "http://localhost:8002", "status": "available"}
    }

    # Split a sample log file into chunks
    chunks = coordinator.split_file("sample_logs.txt", chunk_size=100)

    print("Chunks:", chunks)

    # Distribute work to workers
    await coordinator.start("sample_logs.txt", chunk_size=100)
    

# Run the test
asyncio.run(test_coordinator())
