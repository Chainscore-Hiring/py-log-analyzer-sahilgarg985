import aiohttp
import asyncio
from aiohttp import web
import argparse
import logging
import json

logging.basicConfig(level=logging.INFO)

class Worker:
    def __init__(self, worker_id, coordinator_url):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url

    async def process_chunk(self, filepath, start, size):
        """Processes a chunk of the log file and returns metrics."""
        try:
            with open(filepath, "r") as file:
                # Seek to the starting position of the chunk
                file.seek(start)
                data = file.read(size)

                # Split data into individual log lines
                lines = data.strip().split("\n")
                metrics = {
                    "error_count": 0,
                    "total_response_time": 0,
                    "response_count": 0,
                }

                for line in lines:
                    # Parse log line
                    parts = line.split()
                    if len(parts) < 5:
                        continue  # Skip invalid log lines

                    # Extract fields
                    timestamp = parts[0] + " " + parts[1]
                    level = parts[2]
                    message = " ".join(parts[3:])

                    # Calculate metrics
                    if level == "ERROR":
                        metrics["error_count"] += 1
                    elif "processed in" in message:
                        response_time = int(message.split("in")[1].replace("ms", ""))
                        metrics["total_response_time"] += response_time
                        metrics["response_count"] += 1

                # Calculate average response time
                if metrics["response_count"] > 0:
                    metrics["average_response_time"] = (
                        metrics["total_response_time"] / metrics["response_count"]
                    )
                else:
                    metrics["average_response_time"] = 0

                return metrics

        except Exception as e:
            logging.error(f"[Worker] Error processing chunk: {e}")
            return {"error": str(e)}

    async def report_health(self):
        """Sends a heartbeat to the coordinator."""
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{self.coordinator_url}/heartbeat", json={"worker_id": self.worker_id}) as response:
                    if response.status == 200:
                        logging.info(f"[Worker] Health reported successfully.")
            except Exception as e:
                logging.error(f"[Worker] Failed to report health: {e}")

# Worker HTTP Handlers
async def handle_work(request):
    """Handles the /work endpoint for processing log chunks."""
    worker = request.app["worker"]
    data = await request.json()
    filepath = data["filepath"]
    start = data["start"]
    size = data["size"]

    logging.info(f"[Worker] Received work: {filepath} [{start}:{start + size}]")
    metrics = await worker.process_chunk(filepath, start, size)
    return web.json_response(metrics)

async def health_check(request):
    """Handles the /health endpoint."""
    return web.json_response({"status": "healthy"})

# Main function
async def main():
    parser = argparse.ArgumentParser(description="Distributed Worker Node")
    parser.add_argument("--id", type=str, required=True, help="Worker ID")
    parser.add_argument("--port", type=int, required=True, help="Worker Port")
    parser.add_argument("--coordinator", type=str, required=True, help="Coordinator URL")
    args = parser.parse_args()

    # Create worker instance
    worker = Worker(worker_id=args.id, coordinator_url=args.coordinator)

    # Set up the web server
    app = web.Application()
    app["worker"] = worker
    app.router.add_post("/work", handle_work)
    app.router.add_get("/health", health_check)

    logging.info(f"[Worker] Starting worker {args.id} on port {args.port}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", args.port)
    await site.start()

    # Periodically report health to coordinator
    while True:
        await worker.report_health()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
