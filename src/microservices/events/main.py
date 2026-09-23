import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("events-service")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")

TOPIC_MOVIE = "movie-events"
TOPIC_USER = "user-events"
TOPIC_PAYMENT = "payment-events"

GROUP_ID = "events-service"


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    action: str
    timestamp: str
    username: Optional[str] = None
    email: Optional[str] = None


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: Optional[str] = None


class Event(BaseModel):
    id: str
    type: str
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    payload: Dict[str, Any]


class EventResponse(BaseModel):
    status: str
    partition: int
    offset: int
    event: Event


producer: Optional[AIOKafkaProducer] = None
consumer_tasks: List[asyncio.Task] = []


def _produce_event(event_type: str, payload: Dict[str, Any]) -> Event:
    return Event(
        id=f"{event_type}-{uuid4().hex[:12]}",
        type=event_type,
        payload=payload,
    )


async def _create_producer() -> None:
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info("Kafka producer started (brokers=%s)", KAFKA_BROKERS)


async def _stop_producer() -> None:
    global producer
    if producer is not None:
        await producer.stop()
        producer = None
        logger.info("Kafka producer stopped")


async def _consume_topic(topic: str) -> None:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=f"{GROUP_ID}-{topic}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()
    logger.info("Kafka consumer started for topic %s", topic)
    try:
        async for msg in consumer:
            logger.info(
                "[events] consumed topic=%s partition=%s offset=%s key=%s event=%s",
                msg.topic,
                msg.partition,
                msg.offset,
                msg.key,
                msg.value,
            )
    except Exception as exc:
        logger.error("Kafka consumer for topic %s failed: %s", topic, exc)
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped for topic %s", topic)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _create_producer()
    consumer_tasks.extend(
        asyncio.create_task(_consume_topic(topic))
        for topic in (TOPIC_MOVIE, TOPIC_USER, TOPIC_PAYMENT)
    )
    yield
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    await _stop_producer()


app = FastAPI(title="CinemaAbyss Events Service", lifespan=lifespan)


async def _publish(topic: str, event: Event) -> EventResponse:
    if producer is None:
        raise RuntimeError("kafka producer is not initialized")
    metadata = await producer.send_and_wait(
        topic=topic,
        key=event.id.encode("utf-8"),
        value=event.model_dump(),
    )
    logger.info(
        "Published event id=%s type=%s topic=%s partition=%s offset=%s",
        event.id,
        event.type,
        topic,
        metadata.partition,
        metadata.offset,
    )
    return EventResponse(
        status="success",
        partition=metadata.partition,
        offset=metadata.offset,
        event=event,
    )


@app.get("/api/events/health")
async def health():
    return {"status": True}


@app.post("/api/events/movie", response_model=EventResponse, status_code=201)
async def create_movie_event(payload: MovieEvent):
    try:
        return await _publish(TOPIC_MOVIE, _produce_event("movie", payload.model_dump()))
    except Exception as exc:
        logger.error("Failed to publish movie event: %s", exc)
        return JSONResponse({"error": "movie event publish failed"}, status_code=500)


@app.post("/api/events/user", response_model=EventResponse, status_code=201)
async def create_user_event(payload: UserEvent):
    try:
        return await _publish(TOPIC_USER, _produce_event("user", payload.model_dump()))
    except Exception as exc:
        logger.error("Failed to publish user event: %s", exc)
        return JSONResponse({"error": "user event publish failed"}, status_code=500)


@app.post("/api/events/payment", response_model=EventResponse, status_code=201)
async def create_payment_event(payload: PaymentEvent):
    try:
        return await _publish(
            TOPIC_PAYMENT, _produce_event("payment", payload.model_dump())
        )
    except Exception as exc:
        logger.error("Failed to publish payment event: %s", exc)
        return JSONResponse({"error": "payment event publish failed"}, status_code=500)