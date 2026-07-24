# pyright: reportMissingImports=false, reportMissingTypeStubs=false
import asyncio
import json
import os
import urllib.error
import urllib.request
import uuid
from datetime import date, datetime
from typing import Any, AsyncGenerator, Union
from zoneinfo import ZoneInfo

import redis.asyncio as redis
from haystack.components.generators.chat import OpenAIChatGenerator
from haystack.dataclasses import ChatMessage, StreamingChunk
from haystack.tools import create_tool_from_function
from haystack.components.agents.agent import Agent
from haystack.hooks.human_in_the_loop import (
    ConfirmationHook,
    ToolExecutionDecision
)
from haystack.hooks.human_in_the_loop.types import ConfirmationStrategy
from haystack_integrations.tools.mcp import MCPTool, StreamableHttpServerInfo
from hayhooks import BasePipelineWrapper, async_streaming_generator, log
from hayhooks.server.pipelines.sse import SSEStream


# Haystack 3.0 Launch Week ran Monday July 20 - Friday July 24, 2026, timed to CET.
LAUNCH_WEEK_START = date(2026, 7, 20)

LAUNCH_WEEK_DAYS: dict[int, dict[str, str]] = {
    1: {
        "date": "Monday, July 20",
        "title": "Haystack 3.0 Release",
        "summary": (
            "Agents move to the center of the framework. New Agent hooks (before_run, before_llm, "
            "before_tool, after_tool, on_exit, after_run), skills as first-class citizens, and a much "
            "lighter core (30+ components spun out into separate integration packages)."
        ),
        "url": "https://haystack.deepset.ai/blog/haystack-3-release",
    },
    2: {
        "date": "Tuesday, July 21",
        "title": "Agent Budget & Cost Control",
        "summary": (
            "Turn agent metadata (step_count, token_usage, tool_call_counts) into an enforceable budget "
            "policy via hooks, with soft and hard token limits."
        ),
        "url": "https://haystack.deepset.ai/cookbook/cost_aware_agent",
    },
    3: {
        "date": "Wednesday, July 22",
        "title": "Agent Pack",
        "summary": (
            "Pre-built, complex agents ready to deploy: a Deep Research Agent and a metadata-aware "
            "Advanced RAG Agent, usable in a single line or fully customized."
        ),
        "url": "https://haystack.deepset.ai/tutorials/50_using_pre_built_agents_from_agent_pack",
    },
    4: {
        "date": "Thursday, July 23",
        "title": "Computer-Use Agent with Skills",
        "summary": (
            "A local agent with real bash/shell access, SkillToolset for progressive disclosure of "
            "instructions, and human-in-the-loop via hooks before it touches the machine."
        ),
        "url": "https://haystack.deepset.ai/cookbook/computer_use_agent_with_skills",
    },
    5: {
        "date": "Friday, July 24",
        "title": "Grand Finale (this repo!)",
        "summary": (
            "A deployed, distributed take on the same before_tool confirmation hooks from Day 4: "
            "Hayhooks + Redis + Open WebUI, so a human can approve or reject a tool call from a real "
            "chat UI across separate services, not just a blocking console prompt."
        ),
        "url": "https://github.com/deepset-ai/hitl-hayhooks-redis-openwebui",
    },
}

# Verified against the deepset-ai GitHub org - github.com/deepset-ai
HELPFUL_REPOS: dict[str, list[dict[str, str]]] = {
    "learn": [
        {
            "name": "haystack-tutorials",
            "description": "All the official Haystack tutorials.",
            "url": "https://github.com/deepset-ai/haystack-tutorials",
        },
        {
            "name": "haystack-cookbook",
            "description": "Example notebooks covering advanced Haystack use cases.",
            "url": "https://github.com/deepset-ai/haystack-cookbook",
        },
    ],
    "deploy": [
        {
            "name": "hayhooks",
            "description": "Deploy Haystack pipelines and agents as REST APIs and MCP tools.",
            "url": "https://github.com/deepset-ai/hayhooks",
        },
    ],
    "demos": [
        {
            "name": "haystack-demos",
            "description": "Fully working applications built with Haystack.",
            "url": "https://github.com/deepset-ai/haystack-demos",
        }
    ],
    "hitl": [
        {
            "name": "hitl-hayhooks-redis-openwebui",
            "description": "This repo! A deployed, Redis-based human-in-the-loop pattern for Haystack Agents.",
            "url": "https://github.com/deepset-ai/hitl-hayhooks-redis-openwebui",
        },
    ],
    "extend": [
        {
            "name": "haystack-core-integrations",
            "description": "Official integration packages (components, document stores, and the like).",
            "url": "https://github.com/deepset-ai/haystack-core-integrations",
        },
        {
            "name": "custom-component",
            "description": "A template repo for building and publishing your own Haystack component.",
            "url": "https://github.com/deepset-ai/custom-component",
        },
        {
            "name": "haystack-integrations",
            "description": "List of Haystack integrations.",
            "url": "https://github.com/deepset-ai/haystack-integrations",
        },
    ],
}

FEATURE_EXPLANATIONS: dict[str, str] = {
    "hooks": (
        "Agent hooks are extension points in the Agent's run loop - before_run, before_llm, before_tool, "
        "after_tool, on_exit, after_run - that let you validate inputs, enforce guardrails, or ask a human "
        "before a sensitive action, without modifying the Agent's core logic."
    ),
    "skills": (
        "Skills are first-class citizens in Haystack 3.0. A SkillToolset exposes skill names and short "
        "descriptions to the model up front, and only loads full instructions when the model actually "
        "picks one - keeping context lean while still supporting a large toolbox."
    ),
    "agent pack": (
        "Agent Pack ships pre-built, complex agents ready to deploy - like a Deep Research Agent and a "
        "metadata-aware Advanced RAG Agent - usable in a single line or fully customized."
    ),
    "budget": (
        "Agent budget control turns runtime metadata (step_count, token_usage, tool_call_counts) into an "
        "enforceable policy via hooks, so an over-budget run can be stopped before its next LLM call."
    ),
    "computer-use agent": (
        "The Computer-Use Agent gives a local agent real bash/shell access (e.g. via Ollama), gated by the "
        "same before_tool confirmation hooks this repo uses - a human approves each command before it "
        "touches the machine."
    ),
    "lighter core": (
        "Haystack 3.0's core is lighter: 30+ components moved out into independent integration packages "
        "and the experimental package is no longer a core dependency, shrinking install footprint and "
        "supply-chain surface."
    ),
}

# Slack configuration for the HITL-gated feedback tool.
DEMO_MODE = os.environ.get("DEMO_MODE", "false").strip().lower() not in ("false", "0", "no")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


# Tool functions
def get_launch_week_day(day: int) -> str:
    """
    Get the theme, summary, and link for a specific day of Haystack 3.0 Launch Week. Read-only,
    so it runs without approval.

    :param day: The launch week day number, from 1 to 5 (Monday-Friday).
    :returns: The day's date, title, summary, and link, or an error message if out of range.
    """
    info = LAUNCH_WEEK_DAYS.get(day)
    if not info:
        return f"Launch week only runs Monday-Friday (days 1-5). '{day}' is out of range."
    return f"Day {day} - {info['date']}: {info['title']}\n{info['summary']}\nMore: {info['url']}"


def whats_new_today() -> str:
    """
    Get today's Haystack 3.0 Launch Week drop, based on the current date. Read-only, so it runs
    without approval.

    :returns: Today's launch week day info, or a message if launch week isn't currently running.
    """
    today = datetime.now(ZoneInfo("Europe/Berlin")).date()
    offset = (today - LAUNCH_WEEK_START).days
    if 0 <= offset <= 4:
        return get_launch_week_day(offset + 1)
    return (
        "Haystack 3.0 Launch Week ran Monday July 20 - Friday July 24, 2026. Check "
        "https://haystack.deepset.ai/launch-week for the full recap."
    )


def recommend_repo(interest: str) -> str:
    """
    Recommend relevant deepset-ai GitHub repos based on what someone is trying to do. Read-only,
    so it runs without approval.

    :param interest: A free-text description of what the person is interested in
        (e.g. 'learning Haystack', 'deploying pipelines', 'human in the loop', 'building demos').
    :returns: A list of matching repos with short descriptions and links.
    """
    interest_lower = interest.lower()
    keyword_map = {
        "learn": ["learn", "tutorial", "cookbook", "beginner", "getting started"],
        "deploy": ["deploy", "api", "rest", "production", "serve"],
        "demos": ["demo", "app", "example", "ui"],
        "hitl": ["human in the loop", "hitl", "approval", "confirm", "confirmation"],
        "extend": ["integration", "extend", "custom component", "plugin"],
    }
    matched_categories = [
        category for category, keywords in keyword_map.items() if any(kw in interest_lower for kw in keywords)
    ]
    if not matched_categories:
        matched_categories = list(HELPFUL_REPOS.keys())

    lines = [
        f"- {repo['name']}: {repo['description']} ({repo['url']})"
        for category in matched_categories
        for repo in HELPFUL_REPOS[category]
    ]
    return "\n".join(lines)


def explain_feature(feature: str) -> str:
    """
    Explain a specific Haystack 3.0 Launch Week feature. Read-only, so it runs without approval.

    :param feature: The feature to explain (e.g. 'hooks', 'skills', 'agent pack', 'budget',
        'computer-use agent', 'lighter core').
    :returns: A short explanation, or a list of known features if the given one isn't recognized.
    """
    feature_lower = feature.lower().strip()
    for key, explanation in FEATURE_EXPLANATIONS.items():
        if key in feature_lower or feature_lower in key:
            return explanation
    return f"I don't have an explanation for '{feature}'. Known features: " + ", ".join(FEATURE_EXPLANATIONS.keys())


def submit_feedback_to_deepset(message: str) -> str:
    """
    Submit anonymous feedback - a question, comment, or feature request - to deepset's Slack.
    No email or other personal details are collected. This is the sensitive action itself - it
    posts to a real Slack channel the moment it's approved, with nothing left for a human to do
    afterward, which is exactly why it requires approval first.

    :param message: The feedback, question, or feature request to submit.
    :returns: Confirmation that the feedback was submitted (or simulated, in demo mode).
    """
    if DEMO_MODE:
        log.info(f"[DEMO_MODE] Would post anonymous Slack feedback: '{message}'")
        return f"[Demo mode] Feedback was simulated, not actually submitted.\nMessage: {message}"

    if not SLACK_WEBHOOK_URL:
        log.error("Slack feedback webhook isn't configured (missing SLACK_WEBHOOK_URL)")
        return "Failed to submit feedback: the Slack webhook isn't configured yet."

    payload = json.dumps({"text": f"📨 Anonymous Launch Week feedback:\n>{message}"}).encode("utf-8")
    request = urllib.request.Request(
        SLACK_WEBHOOK_URL, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
        log.info("Posted anonymous feedback to Slack")
        return "Feedback submitted to deepset anonymously - no personal details were sent."
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        log.error(f"Failed to post Slack feedback: {e}", exc_info=True)
        return f"Failed to submit feedback: {e}"


# Create tools
get_launch_week_day_tool = create_tool_from_function(
    function=get_launch_week_day,
    name="get_launch_week_day",
    description="Get the theme, summary, and link for a specific day (1-5) of Haystack 3.0 Launch Week.",
)

whats_new_today_tool = create_tool_from_function(
    function=whats_new_today,
    name="whats_new_today",
    description="Get today's Haystack 3.0 Launch Week drop, based on the current date.",
)

recommend_repo_tool = create_tool_from_function(
    function=recommend_repo,
    name="recommend_repo",
    description="Recommend relevant deepset-ai GitHub repos based on what someone is trying to do.",
)

explain_feature_tool = create_tool_from_function(
    function=explain_feature,
    name="explain_feature",
    description="Explain a specific Haystack 3.0 Launch Week feature.",
)

search_haystack_docs_tool = MCPTool(
    name="search_haystack_docs",
    server_info=StreamableHttpServerInfo(url="https://docs.haystack.deepset.ai/api/mcp"),
)

submit_feedback_tool = create_tool_from_function(
    function=submit_feedback_to_deepset,
    name="submit_feedback_to_deepset",
    description="Submit anonymous feedback (a question, comment, or feature request) to deepset's Slack.",
)


class RedisConfirmationStrategy(ConfirmationStrategy):
    """
    Stateless async confirmation strategy using Redis for approval tracking.

    Per-request state (event_queue, redis_client) is obtained from confirmation_strategy_context,
    allowing this strategy instance to be reused across requests. The dict is passed to
    Agent.run_async() as the `hook_context` argument; the ConfirmationHook forwards it to this
    strategy as `confirmation_strategy_context`.
    """

    def run(
        self,
        *,
        tool_name: str,
        tool_description: str,
        tool_params: dict[str, Any],
        tool_call_id: str | None = None,
        confirmation_strategy_context: dict[str, Any] | None = None,
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
        *,
        tool_name: str,
        tool_description: str,
        tool_params: dict[str, Any],
        tool_call_id: str | None = None,
        confirmation_strategy_context: dict[str, Any] | None = None,
    ) -> ToolExecutionDecision:
        """
        Async confirmation strategy using Redis BLPOP for non-blocking approval wait.

        :param tool_name: Name of the tool.
        :param tool_description: Description of the tool.
        :param tool_params: Tool parameters.
        :param tool_call_id: Unique ID for this tool call.
        :param confirmation_strategy_context: Dictionary containing per-request state (event_queue, redis_client).
        :returns: ToolExecutionDecision object with approval status.
        """
        # Get per-request state from confirmation_strategy_context
        if confirmation_strategy_context is None:
            raise RuntimeError(
                "confirmation_strategy_context is required for RedisConfirmationStrategy"
            )

        event_queue: asyncio.Queue[dict[str, Any]] = confirmation_strategy_context["event_queue"]
        redis_client: redis.Redis = confirmation_strategy_context["redis_client"]

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

        # Create reusable confirmation strategy (stateless - uses confirmation_strategy_context for per-request state)
        self.confirmation_strategy = RedisConfirmationStrategy()

        # Create reusable Agent instance (thread-safe for concurrent requests)
        # HITL is a "before_tool" hook; per-request state is passed via the hook_context run argument
        self.agent = Agent(
            chat_generator=OpenAIChatGenerator(model="gpt-4o-mini"),
            system_prompt=(
                "You're the Haystack 3.0 Launch Week Concierge. Help visitors learn about the daily "
                "launch week drops, explain new features, and recommend helpful deepset-ai repos. For "
                "general Haystack questions beyond launch week (e.g. how a component or API works), "
                "use search_haystack_docs to search the real documentation instead of guessing. If "
                "someone wants to share a question, comment, or feature request with the deepset team, "
                "use submit_feedback_to_deepset to submit it anonymously - no need to ask for their "
                "name or email."
            ),
            tools=[
                get_launch_week_day_tool,
                whats_new_today_tool,
                recommend_repo_tool,
                explain_feature_tool,
                search_haystack_docs_tool,
                submit_feedback_tool,
            ],
            hooks={
                "before_tool": [
                    ConfirmationHook(
                        # Only the feedback submission reaches deepset's Slack and requires
                        # approval - the informational tools are read-only and execute immediately
                        # since they have no entry here.
                        confirmation_strategies={
                            submit_feedback_tool.name: self.confirmation_strategy,
                        }
                    )
                ]
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
            # Pass per-request state via hook_context; the ConfirmationHook hands it to the
            # confirmation strategy as confirmation_strategy_context (redis_client is shared and reused)
            async for item in async_streaming_generator(
                pipeline=self.agent,  # Reused across requests!
                pipeline_run_args={
                    "messages": chat_messages,
                    "hook_context": {  # Per-request state for confirmation strategy
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
