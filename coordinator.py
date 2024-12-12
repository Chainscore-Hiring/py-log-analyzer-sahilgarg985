import aiohttp
import asyncio
import logging
import argparse
from pathlib import Path

logging.basicConfig(level=logging.INFO)

class Coordinator:
    def __init__(self, port):
        self.workers = {}  # Worker metadata
        self.port = port
        self.results = []  # To store results from workers

    async def register_worker(self, worker_id, worker_url):
        """Register a new worker."""
        self.workers[worker_id] = worker_url
        logging.info(f"[Coordinator] Registered worker: {worker_id} at {worker_url}")

    async def distribute_work(self, filepath):
        """Split the file into chunks and send work to workers."""
        # Validate file
        file = Path(filepath)
        if not file.exists():
            logging.error(f"[Coordinator] File {filepath} does not exist!")
            return

        file_size = file.stat().st_size
        chunk_size = 100  # Define chunk size in bytes (adjust as needed)
        chunks = [(i, min(i + chunk_size, file_size)) for i in range(0, file_size, chunk_size)]

        logging.info(f"[Coordinator] Splitting {filepath} into {len(chunks)} chunks")

        # Send chunks to workers
        for i, (start, end) in enumerate(chunks):
            worker_id = list(self.workers.keys())[i % len(self.workers)]  # Round-robin allocation
            worker_url = self.workers[worker_id]

            # Send chunk to worker
            async with aiohttp.ClientSession() as session:
                try:
                    logging.info(f"[Coordinator] Sending chunk {start}-{end} to {worker_id}")
                    async with session.post(f"{worker_url}/work", json={
                        "filepath": str(filepath),
                        "start": start,
                        "size": end - start
                    }) as response:
                        result = await response.json()
                        self.results.append(result)
                        logging.info(f"[Coordinator] Received result from {worker_id}: {result}")
                except Exception as e:
                    logging.error(f"[Coordinator] Error sending work to {worker_id}: {e}")

    async def monitor_workers(self):
        """Continuously check worker health."""
        while True:
            async with aiohttp.ClientSession() as session:
                for worker_id, worker_url in self.workers.items():
                    try:
                        async with session.get(f"{worker_url}/health") as response:
                            if response.status == 200:
                                logging.info(f"[Coordinator] {worker_id} is healthy")
                            else:
                                logging.warning(f"[Coordinator] {worker_id} returned status {response.status}")
                    except Exception as e:
                        logging.error(f"[Coordinator] {worker_id} is unreachable: {e}")

            await asyncio.sleep(5)  # Check every 5 seconds


# Main function
async def main():
    parser = argparse.ArgumentParser(description="Coordinator for distributed log processing.")
    parser.add_argument("--port", type=int, required=True, help="Port to run the coordinator")
    parser.add_argument("--logfile", type=str, required=True, help="Log file to process")
    args = parser.parse_args()

    # Initialize Coordinator
    coordinator = Coordinator(port=args.port)

    # Simulate worker registration
    await coordinator.register_worker("worker_1", "http://localhost:8001")
    await coordinator.register_worker("worker_2", "http://localhost:8002")

    # Start monitoring workers
    asyncio.create_task(coordinator.monitor_workers())

    # Start distributing work
    await coordinator.distribute_work(args.logfile)


if __name__ == "__main__":
    asyncio.run(main())
