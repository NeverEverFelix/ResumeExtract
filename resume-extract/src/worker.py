import asyncio

from src.main import run_worker_forever


if __name__ == "__main__":
    asyncio.run(run_worker_forever())
