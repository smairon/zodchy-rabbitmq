import typing
import dataclasses
import json
import uuid
import logging
import collections.abc
import asyncio
from aio_pika import ExchangeType, IncomingMessage, connect_robust
from zodchy.codex.transport import CommunicationMessage


@dataclasses.dataclass
class ConsumerSettings:
    durable: bool = True
    prefetch_count: int | None = None
    ttl: int | None = None


class TopicConsumer:
    def __init__(
        self,
        dsn: str,
        exchange_name: str,
        handler: collections.abc.Callable[
            [CommunicationMessage], collections.abc.Awaitable[None]
        ],
        settings: ConsumerSettings = ConsumerSettings(),
        logger: logging.Logger | None = None,
    ):
        self._dsn = dsn
        self._exchange_name = exchange_name
        self._handler = handler
        self._settings = settings
        self._logger = logger
        self._connection = None
        self._channel = None
        self._exchange = None
        self._queue = None

    async def run(self, routing_patterns: list[str], queue_name: str | None = None):
        """Main method to run the consumer"""
        try:
            await self.connect()
            await self.setup_queue(routing_patterns, queue_name)
            await self.start_consuming()

            # Keep the consumer running
            await self._log("Consumer is running. Press Ctrl+C to stop.", "info")
            await asyncio.Future()  # Run forever

        except asyncio.CancelledError:
            await self._log("Consumer stopped by user", "info")
        except Exception as e:
            await self._log(f"Consumer error: {e}", "error")
        finally:
            await self.disconnect()

    async def connect(self):
        """Establish connection to RabbitMQ"""
        self._connection = await connect_robust(self._dsn)
        self._channel = await self._connection.channel()

        # Set prefetch count to limit unacknowledged messages
        if self._settings.prefetch_count is not None:
            await self._channel.set_qos(prefetch_count=self._settings.prefetch_count)

        # Declare topic exchange
        await self._log(f"Declaring exchange: {self._exchange_name}", "info")
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name, ExchangeType.TOPIC, durable=self._settings.durable
        )

    async def disconnect(self):
        """Disconnect from RabbitMQ"""
        if self._connection:
            await self._connection.close()

    async def setup_queue(
        self, routing_patterns: list[str], queue_name: str | None = None
    ):
        """Setup queue and bindings"""
        # Declare queue
        queue_name = queue_name or f"q_{self._exchange_name.replace('x_', '')}"
        arguments = {}
        if self._settings.ttl is not None:
            arguments["x-message-ttl"] = self._settings.ttl

        self._queue = await self._channel.declare_queue(
            queue_name, durable=True, arguments=arguments or None
        )

        # Bind queue to exchange with routing patterns
        for pattern in routing_patterns:
            await self._queue.bind(self._exchange, routing_key=pattern)
            await self._log(f"Bound queue {queue_name} to pattern '{pattern}'", "info")

    async def start_consuming(self):
        """Start consuming messages"""
        await self._log(f"Started consuming from queue: {self._queue.name}", "info")
        await self._queue.consume(self._message_handler)

    async def _message_handler(self, message: IncomingMessage):
        """Process incoming messages"""
        try:
            async with message.process():
                await self._handler(
                    CommunicationMessage(
                        id=uuid.uuid4(),
                        routing_key=message.routing_key,
                        body=json.loads(message.body),
                    )
                )

        except Exception as e:
            await self._log(f"Error processing message: {e}", "error")
            # You might want to implement dead letter queue here
            await message.nack(requeue=False)  # Don't requeue on error

    async def _log(self, message: str, level: typing.Literal["info", "error", "debug"]):
        if self._logger:
            _methods = {
                "info": self._logger.info,
                "error": self._logger.error,
                "debug": self._logger.debug,
            }
            try:
                _methods[level](message)
            except KeyError as e:
                self._logger.error(f"Error logging message: {e}")
