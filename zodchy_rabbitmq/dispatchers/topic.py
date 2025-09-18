import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import dataclasses
from aio_pika import connect, Message, ExchangeType
from aio_pika.pool import Pool
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractExchange
from zodchy.codex.transport import CommunicationMessage
from .general import DispatcherSettings, Dispatcher
from ..internals import encoders


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class TopicDispatcherSettings(DispatcherSettings):
    max_connections: int = 10
    max_channels: int = 100
    timeout: float = 5.0


class TopicDispatcher(Dispatcher):
    def __init__(
        self,
        dsn: str,
        exchange_name: str,
        settings: DispatcherSettings = TopicDispatcherSettings(),
        json_encoder: json.JSONEncoder = encoders.DefaultJSONEncoder,
        logger: logging.Logger | None = None,
    ):
        super().__init__(dsn, exchange_name, settings, json_encoder, logger)
        self._exchange_cache = {}
        self._is_initialized = False

    async def initialize(self) -> None:
        """Initialize connection and channel pools (must be called before use)"""
        if self._is_initialized:
            return

        # Create pools with proper event loop handling
        loop = asyncio.get_event_loop()

        self._connection_pool = Pool(
            self._get_connection, max_size=self._settings.max_connections, loop=loop
        )

        self._channel_pool = Pool(
            self._get_channel, max_size=self._settings.max_channels, loop=loop
        )

        self._is_initialized = True
        self._logger.info("RabbitMQ dispatcher initialized")

    @asynccontextmanager
    async def get_exchange_context(self) -> AsyncGenerator[AbstractExchange, None]:
        if not self._is_initialized:
            await self.initialize()
        """Context manager for acquiring exchange"""
        async with self._channel_pool.acquire() as channel:
            exchange = await self._get_exchange(channel)
            yield exchange

    async def dispatch(
        self,
        message: CommunicationMessage,
    ) -> bool:
        """Publish message using connection pooling"""
        try:
            if not self._is_initialized:
                await self.initialize()

            async with self.get_exchange_context() as exchange:
                rabbitmq_message = self._build_rabbitmq_message(message)
                await exchange.publish(
                    rabbitmq_message, routing_key=message.routing_key
                )
                self._log_debug(f"Published to {message.routing_key}")
                return True

        except Exception as e:
            self._log_error(f"Publish failed for {message.routing_key}: {e}")
            return False

    async def dispatch_with_confirmation(self, message: CommunicationMessage) -> bool:
        """Publish with delivery confirmation"""
        try:
            if not self._is_initialized:
                await self.initialize()

            async with self._channel_pool.acquire() as channel:
                # Enable publisher confirms
                await channel.set_publisher_confirms(True)

                exchange = await self._get_exchange(channel)

                rabbitmq_message = self._build_rabbitmq_message(message)
                # Wait for confirmation
                confirmation = await exchange.publish(
                    rabbitmq_message,
                    routing_key=message.routing_key,
                    timeout=self._settings.timeout,
                )

                if confirmation:
                    self._log_debug(f"Message confirmed for {message.routing_key}")
                    return True
                else:
                    self._log_error(f"Message not confirmed for {message.routing_key}")
                    return False

        except asyncio.TimeoutError:
            self._log_error(f"Publish confirmation timeout for {message.routing_key}")
            return False
        except Exception as e:
            self._log_error(f"Publish with confirmation failed: {e}")
            return False

    async def shutdown(self) -> None:
        """Cleanup resources"""
        if self._channel_pool:
            await self._channel_pool.close()
        if self._connection_pool:
            await self._connection_pool.close()

        self._is_initialized = False
        self._logger.info("RabbitMQ dispatcher closed")

    def _build_rabbitmq_message(self, message: CommunicationMessage) -> Message:
        message_body = json.dumps(message.body, cls=self._json_encoder).encode()
        return Message(
            body=message_body,
            delivery_mode=2,
            content_type="application/json",
            headers=message.headers or {},
        )

    async def _get_connection(self) -> AbstractConnection:
        """Create new connection for pool"""
        return await connect(self._dsn)

    async def _get_channel(self) -> AbstractChannel:
        """Create new channel for pool"""
        async with self._connection_pool.acquire() as connection:
            return await connection.channel()

    async def _get_exchange(self, channel: AbstractChannel) -> AbstractExchange:
        """Get or create exchange"""
        if channel not in self._exchange_cache:
            exchange = await channel.declare_exchange(
                self._exchange_name, ExchangeType.TOPIC, durable=self._settings.durable
            )
            self._exchange_cache[channel] = exchange
        return self._exchange_cache[channel]

    def _log_info(self, message: str):
        if self._logger:
            self._logger.info(message)

    def _log_error(self, message: str):
        if self._logger:
            self._logger.error(message)

    def _log_debug(self, message: str):
        if self._logger:
            self._logger.debug(message)

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()
