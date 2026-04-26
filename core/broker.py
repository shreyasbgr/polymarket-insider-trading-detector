"""
core/broker.py — TaskIQ broker initialization.
"""
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend
import config

# RabbitMQ Broker for robust, distributed task management
broker = AioPikaBroker(config.RABBITMQ_URL)

# Redis Backend for storing results
result_backend = RedisAsyncResultBackend(config.REDIS_URL)
broker.with_result_backend(result_backend)

@broker.on_event("startup")
async def setup_db(state):
    """Initialize ClickHouse on worker startup."""
    from db.clickhouse import init_clickhouse
    try:
        init_clickhouse()
        print("ClickHouse initialized on worker startup.")
    except Exception as e:
        print(f"Failed to initialize ClickHouse on worker startup: {e}")
