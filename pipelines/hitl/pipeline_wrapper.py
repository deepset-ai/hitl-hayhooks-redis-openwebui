# pyright: reportMissingImports=false, reportMissingTypeStubs=false
import asyncio
import os
import uuid
from typing import TYPE_CHECKING, Any, AsyncGenerator, Union

import redis.asyncio as redis
from haystack.components.generators.chat import OpenAIChatGenerator
from haystack.dataclasses import ChatMessage, StreamingChunk
from haystack.tools import create_tool_from_function
from haystack_experimental.components.agents.agent import Agent
from haystack_experimental.components.agents.human_in_the_loop import (
    ConfirmationStrategy,
)
from haystack_experimental.components.agents.human_in_the_loop.dataclasses import (
    ToolExecutionDecision,
)
from hayhooks import BasePipelineWrapper, async_streaming_generator, log
from hayhooks.server.pipelines.sse import SSEStream
from datetime import datetime
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from haystack_experimental.components.agents.agent import _ExecutionContext


# Tool functions
def weather_function(location: str) -> str:
    """
    Provides weather information for a given location.

    :param location: The location to get weather for.
    :returns: Weather information string.
    """
    return f"The weather in {location} is cloudy."


def get_time(timezone: str = "UTC") -> str:
    """
    Get the current time in a specific timezone.

    :param timezone: The timezone to get time for (e.g., 'UTC', 'Europe/Rome', 'America/New_York').
    :returns: Current time string in the specified timezone.
    """

    try:
        tz = ZoneInfo(timezone)
        now = datetime.now(tz)
        return f"Current time in {timezone}: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}"
    except Exception as e:
        return f"Error getting time for timezone '{timezone}': {e}"


# Create tools
weather_tool = create_tool_from_function(
    function=weather_function,
    name="weather_tool",
    description="Provides weather information for a given location.",
)

time_tool = create_tool_from_function(
    function=get_time,
    name="get_time",
    description="Get the current time in a specific timezone.",
)


class RedisConfirmationStrategy(ConfirmationStrategy):
    """
    Stateless async confirmation strategy using Redis for approval tracking.

    Per-request state (event_queue, redis_client) is obtained from execution_context.run_context,
    allowing this strategy instance to be reused across requests.
    """

    def run(
        self,
        tool_name: str,
        tool_description: str,
        tool_params: dict[str, Any],
        tool_call_id: str | None = None,
        execution_context: "_ExecutionContext | None" = None,
    ) -> ToolExecutionDecision:
        """
        Sync version - not supported, use run_async instead.

        :raises RuntimeError: Always raises since this strategy requires async.
        """
        raise RuntimeError(
            "RedisConfirmationStrategy requires async execution. "
            "Use Agent.run_async() or async_streaming_generator instead."
        )

    async def run_async(
        self,
        tool_name: str,
        tool_description: str,
        tool_params: dict[str, Any],
        tool_call_id: str | None = None,
        execution_context: "_ExecutionContext | None" = None,
    ) -> ToolExecutionDecision:
        """
        Async confirmation strategy using Redis BLPOP for non-blocking approval wait.

        :param tool_name: Name of the tool.
        :param tool_description: Description of the tool.
        :param tool_params: Tool parameters.
        :param tool_call_id: Unique ID for this tool call.
        :param execution_context: Execution context containing run_context with per-request state.
        :returns: ToolExecutionDecision object with approval status.
        """
        # Get per-request state from execution context
        if execution_context is None or execution_context.run_context is None:
            raise RuntimeError(
                "execution_context.run_context is required for RedisConfirmationStrategy"
            )

        run_ctx = execution_context.run_context
        event_queue: asyncio.Queue[dict[str, Any]] = run_ctx["event_queue"]
        redis_client: redis.Redis = run_ctx["redis_client"]

        # Generate tool_call_id if not provided
        if not tool_call_id:
            tool_call_id = f"{tool_name}_{uuid.uuid4()}"

        # Emit tool call start event to queue
        await event_queue.put(
            {
                "type": "tool_call_start",
                "tool_name": tool_name,
                "arguments": tool_params,
                "id": tool_call_id,
            }
        )
        log.info(f"Tool call started: {tool_name} (id={tool_call_id})")

        # Use async BLPOP to wait for approval (non-blocking)
        approval_list_key = f"tool_approval:{tool_call_id}"
        timeout = 300  # 5 minutes

        try:
            # Async BLPOP - doesn't block the event loop
            result = await redis_client.blpop([approval_list_key], timeout=timeout)  # type: ignore

            if result:
                _, approval_value = result  # type: ignore
                approved = approval_value.decode("utf-8") == "approved"  # type: ignore
                log.info(
                    f"Tool call {'approved' if approved else 'rejected'}: {tool_name} (id={tool_call_id})"
                )

                # Emit approval status
                await event_queue.put(
                    {
                        "type": "tool_call_approved"
                        if approved
                        else "tool_call_rejected",
                        "tool_name": tool_name,
                        "id": tool_call_id,
                    }
                )

                return ToolExecutionDecision(
                    tool_name=tool_name,
                    execute=approved,
                    tool_call_id=tool_call_id,
                    feedback="Approved by user" if approved else "Rejected by user",
                    final_tool_params=tool_params if approved else None,
                )
            else:
                # Timeout - reject by default
                log.warning(f"Tool call timeout: {tool_name} (id={tool_call_id})")
                await event_queue.put(
                    {
                        "type": "tool_call_timeout",
                        "tool_name": tool_name,
                        "id": tool_call_id,
                    }
                )

                return ToolExecutionDecision(
                    tool_name=tool_name,
                    execute=False,
                    tool_call_id=tool_call_id,
                    feedback="Timeout: No approval received within 5 minutes",
                    final_tool_params=None,
                )

        except Exception as e:
            # Handle any Redis errors
            log.error(
                f"Redis error for tool call {tool_name} (id={tool_call_id}): {e}",
                exc_info=True,
            )
            await event_queue.put(
                {
                    "type": "tool_call_error",
                    "tool_name": tool_name,
                    "id": tool_call_id,
                    "error": str(e),
                }
            )

            return ToolExecutionDecision(
                tool_name=tool_name,
                execute=False,
                tool_call_id=tool_call_id,
                feedback=f"Error waiting for approval: {str(e)}",
                final_tool_params=None,
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the strategy to a dictionary."""
        return {
            "type": "RedisConfirmationStrategy",
            "init_parameters": {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RedisConfirmationStrategy":
        """Deserialize the strategy from a dictionary."""
        raise NotImplementedError(
            "Deserialization not supported for RedisConfirmationStrategy"
        )


class PipelineWrapper(BasePipelineWrapper):
    def setup(self) -> None:
        # Redis configuration from environment variables
        redis_host = os.environ.get("REDIS_HOST", "localhost")
        redis_port = int(os.environ.get("REDIS_PORT", "6379"))

        # Create async Redis connection pool (can be created synchronously)
        self.redis_pool = redis.ConnectionPool(
            host=redis_host,
            port=redis_port,
        )

        # Create async Redis client with connection pool
        self.redis_client = redis.Redis(connection_pool=self.redis_pool)
        log.info(
            f"Async Redis client initialized with connection pool: {redis_host}:{redis_port}"
        )

        # Create reusable confirmation strategy (stateless - uses run_context for per-request state)
        self.confirmation_strategy = RedisConfirmationStrategy()

        # Create reusable Agent instance (thread-safe for concurrent requests)
        # Per-request state is passed via run_context parameter
        self.agent = Agent(
            chat_generator=OpenAIChatGenerator(model="gpt-4o-mini"),
            system_prompt="You're a helpful agent with access to tools. Use them when needed.",
            tools=[weather_tool, time_tool],
            confirmation_strategies={
                weather_tool.name: self.confirmation_strategy,
                time_tool.name: self.confirmation_strategy,
            },
        )
        log.info("Agent initialized with reusable async HITL confirmation strategy")

    async def run_api_async(self, messages: list[dict]) -> Any:
        """
        Run agent with HITL using async streaming with shared event queue.

        :param messages: List of OpenAI-format messages.
        :returns: SSEStream wrapping the async generator.
        """

        async def main_generator() -> AsyncGenerator[
            Union[dict[str, Any], StreamingChunk], None
        ]:
            # Per-request async event queue for HITL events
            event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

            # Convert messages to ChatMessage format
            chat_messages = [
                ChatMessage.from_openai_dict_format(msg) for msg in messages
            ]

            # Use hayhooks async_streaming_generator with external_event_queue
            # Pass per-request state via run_context (redis_client is shared and reused)
            async for item in async_streaming_generator(
                pipeline=self.agent,  # Reused across requests!
                pipeline_run_args={
                    "messages": chat_messages,
                    "run_context": {  # Per-request state for confirmation strategy
                        "event_queue": event_queue,
                        "redis_client": self.redis_client,  # Shared async client
                    },
                },
                external_event_queue=event_queue,
            ):
                # Transform StreamingChunk to dict format for SSE
                if isinstance(item, StreamingChunk):
                    yield {"type": "text", "content": item.content}
                else:
                    # Already a dict (HITL event)
                    yield item

        return SSEStream(main_generator())
