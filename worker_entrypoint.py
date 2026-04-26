"""
worker_entrypoint.py — Entry point for TaskIQ worker.
"""
from core.broker import broker
import core.tasks  # Import tasks so they are registered

if __name__ == "__main__":
    import asyncio
    from taskiq.cli.worker.run import run_worker
    # In production, you run: taskiq worker worker_entrypoint:broker
    pass
