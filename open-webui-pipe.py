"""
title: Hayhooks HITL Pipe
author: mpangrazzi
date: 2025-11-24
version: 2.0.1
license: MIT
description: Proxy requests from Open WebUI to Hayhooks HITL pipeline with Redis-based approval tracking
requirements: redis, aiohttp
environment_variables: HAYHOOKS_URL, REDIS_HOST, REDIS_PORT
"""

from __future__ import annotations

import json
import logging
from typing import AsyncGenerator, Callable

import aiohttp
import redis.asyncio as redis
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default configuration
HAYHOOKS_URL = "http://hayhooks:1416"
REDIS_HOST = "redis"
REDIS_PORT = 6379


class SSEParser:
    """Parse Server-Sent Events (SSE) stream."""

    def __init__(self):
        self.buffer = ""

    def add_chunk(self, chunk: bytes) -> list[dict]:
        """
        Add a chunk to the buffer and return complete events.

        :param chunk: Raw bytes from SSE stream
        :returns: List of parsed event data dictionaries
        """
        self.buffer += chunk.decode("utf-8", errors="ignore")
        events = []

        # Process complete SSE messages (delimited by \n\n)
        while "\n\n" in self.buffer:
            message, self.buffer = self.buffer.split("\n\n", 1)
            event_data = self._parse_sse_message(message)
            if event_data:
                events.append(event_data)

        return events

    def _parse_sse_message(self, message: str) -> dict | None:
        for line in message.split("\n"):
            if line.startswith("data: "):
                json_str = line[6:]  # Remove "data: " prefix
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse SSE JSON: {json_str}, error: {e}")
                    return None
        return None


class HITLApprovalHandler:
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client

    async def request_approval(
        self,
        tool_name: str,
        arguments: dict,
        tool_call_id: str,
        __event_call__: Callable,
    ) -> bool:
        """
        Request user approval for tool execution via Open WebUI dialog.

        :param tool_name: Name of the tool to execute
        :param arguments: Tool arguments
        :param tool_call_id: Unique ID for this tool call
        :param __event_call__: Open WebUI event callback for blocking dialogs
        :returns: True if approved, False if rejected
        """
        # Show confirmation dialog to user
        result = await __event_call__(
            {
                "type": "confirmation",
                "data": {
                    "title": f"🔧 Tool Call: {tool_name}",
                    "message": f"Approve calling {tool_name}?\n\nArguments:\n{json.dumps(arguments, indent=2)}",
                },
            }
        )

        approved = bool(result)
        logger.info(
            f"User {'approved' if approved else 'rejected'} tool call: {tool_name}"
        )

        return approved

    async def send_approval_to_redis(self, tool_call_id: str, approved: bool) -> None:
        """
        Send approval decision to Redis for pipeline to consume.

        :param tool_call_id: Unique ID for this tool call
        :param approved: Whether the tool was approved
        """
        approval_list_key = f"tool_approval:{tool_call_id}"
        approval_value = "approved" if approved else "rejected"

        try:
            # Push approval to Redis list (pipeline is blocking on BLPOP)
            await self.redis_client.lpush(approval_list_key, approval_value)  # type: ignore
            # Set expiry in case approval is not consumed
            await self.redis_client.expire(approval_list_key, 300)  # type: ignore
            logger.info(
                f"Sent approval to Redis: {approval_list_key} = {approval_value}"
            )
        except Exception as e:
            logger.error(f"Failed to write approval to Redis: {e}", exc_info=True)

    async def emit_approval_status(
        self,
        tool_name: str,
        approved: bool,
        __event_emitter__: Callable,
    ) -> None:
        """
        Emit status event to Open WebUI about approval decision.

        :param tool_name: Name of the tool
        :param approved: Whether the tool was approved
        :param __event_emitter__: Open WebUI event emitter
        """
        if approved:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "description": f"✅ Tool {tool_name} approved, executing...",
                        "done": False,
                        "hidden": True,  # Don't clutter the chat
                    },
                }
            )
        else:
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "description": f"❌ Tool {tool_name} rejected by user",
                        "done": True,
                        "hidden": False,
                    },
                }
            )


class Pipe:
    """Open WebUI Pipe for Hayhooks HITL integration."""

    class Valves(BaseModel):
        """Configuration valves for the pipe."""

        BASE_URL: str = Field(
            default=HAYHOOKS_URL,
            description="Hayhooks server base URL",
        )
        PIPELINE_NAME: str = Field(
            default="hitl",
            description="Pipeline to call via /<name>/run",
        )
        REDIS_HOST: str = Field(
            default=REDIS_HOST,
            description="Redis host for approval tracking",
        )
        REDIS_PORT: int = Field(
            default=REDIS_PORT,
            description="Redis port",
        )

    def __init__(self) -> None:
        self.valves = self.Valves()
        self.redis_client: redis.Redis | None = None
        logger.info(f"Initialized pipe with valves: {self.valves}")

    async def _get_redis_client(self) -> redis.Redis:
        """Get or create Redis client (lazy initialization)."""
        if self.redis_client is None:
            self.redis_client = redis.Redis(
                host=self.valves.REDIS_HOST,
                port=self.valves.REDIS_PORT,
                db=0,
                decode_responses=False,
            )
        return self.redis_client

    async def _handle_tool_call_start(
        self,
        event_data: dict,
        approval_handler: HITLApprovalHandler,
        __event_emitter__: Callable,
        __event_call__: Callable,
    ) -> bool:
        """
        Handle tool_call_start event by requesting user approval.

        :param event_data: Event data containing tool call information
        :param approval_handler: HITL approval handler
        :param __event_emitter__: Open WebUI event emitter
        :param __event_call__: Open WebUI event caller (for blocking dialogs)
        :returns: True if approved, False if rejected
        """
        tool_name = event_data.get("tool_name", "unknown")
        arguments = event_data.get("arguments", {})
        tool_call_id = event_data.get("id")

        if not tool_call_id:
            logger.warning("Tool call event missing ID, skipping approval")
            return True

        # Request approval from user
        approved = await approval_handler.request_approval(
            tool_name, arguments, tool_call_id, __event_call__
        )

        # Send approval to Redis for pipeline to consume
        await approval_handler.send_approval_to_redis(tool_call_id, approved)

        # Emit status to Open WebUI
        await approval_handler.emit_approval_status(
            tool_name, approved, __event_emitter__
        )

        return approved

    async def _stream_from_hayhooks(
        self,
        endpoint: str,
        payload: dict,
        __event_emitter__: Callable,
        __event_call__: Callable,
    ) -> AsyncGenerator[str, None]:
        """
        Stream responses from Hayhooks and handle HITL approval workflow.

        :param endpoint: Hayhooks endpoint URL
        :param payload: Request payload
        :param __event_emitter__: Open WebUI event emitter
        :param __event_call__: Open WebUI event caller
        :yields: Text content from the stream
        """
        redis_client = await self._get_redis_client()
        approval_handler = HITLApprovalHandler(redis_client)
        sse_parser = SSEParser()

        async with aiohttp.ClientSession() as session:
            async with session.post(
                endpoint,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream",
                },
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    yield error_text or f"Hayhooks error (HTTP {resp.status})"
                    return

                # Stream and parse SSE events
                async for chunk in resp.content.iter_any():
                    if not chunk:
                        continue

                    events = sse_parser.add_chunk(chunk)

                    for event_data in events:
                        event_type = event_data.get("type")

                        if event_type == "text":
                            # Yield text content
                            content = event_data.get("content", "")
                            if content:
                                yield content

                        elif event_type == "tool_call_start":
                            # Handle HITL approval
                            approved = await self._handle_tool_call_start(
                                event_data,
                                approval_handler,
                                __event_emitter__,
                                __event_call__,
                            )

                            if not approved:
                                # User rejected - stop streaming
                                tool_name = event_data.get("tool_name", "unknown")
                                yield f"\n\n❌ Tool execution of '{tool_name}' was rejected by user."
                                return

                        # Ignore other event types (tool_call_approved, etc.)
                        # They're just status updates from the pipeline

    async def pipe(
        self,
        body: dict,
        __user__: dict,  # noqa: ARG002 - required by Open WebUI
        __event_emitter__: Callable,
        __event_call__: Callable,
    ) -> AsyncGenerator[str, None]:
        """
        Main pipe function - proxy requests to Hayhooks with HITL approval.

        :param body: Request body from Open WebUI
        :param __user__: User information (unused)
        :param __event_emitter__: Open WebUI event emitter
        :param __event_call__: Open WebUI event caller
        :yields: Text content from the pipeline
        """
        messages = body.get("messages", [])
        if not messages:
            yield "⚠️ Please provide a message to start the conversation."
            return

        endpoint = f"{self.valves.BASE_URL.rstrip('/')}/{self.valves.PIPELINE_NAME}/run"
        payload = {"messages": messages}

        # Stream from Hayhooks - errors are handled in _stream_from_hayhooks
        async for content in self._stream_from_hayhooks(
            endpoint, payload, __event_emitter__, __event_call__
        ):
            yield content
