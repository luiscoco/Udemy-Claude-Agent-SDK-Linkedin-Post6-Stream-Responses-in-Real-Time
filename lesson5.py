"""
Lesson 5 - Enable and Handle Stream Events (compact teaching script)
====================================================================

The complete lesson5.py from the slides:

  1. include_partial_messages=True   -> the SDK also yields StreamEvent objects
  2. content_block_delta + text_delta -> print each fragment as it arrives
  3. ResultMessage                   -> report the outcome once, without reprinting the answer

Run:  python lesson5.py
"""

import asyncio
import os
import sys

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, StreamEvent, query

# Optional: hide the "claude.ai connectors are disabled..." startup warning.
os.environ["ENABLE_CLAUDEAI_MCP_SERVERS"] = "false"

# Windows consoles default to cp1252 and can crash on emoji in Claude's replies.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


async def main() -> None:
    options = ClaudeAgentOptions(
        include_partial_messages=True,   # the switch: add StreamEvent objects to the stream
        tools=[],                        # this lesson needs no tools: a plain streamed answer
        setting_sources=[],              # ignore local Claude settings for a reproducible run
    )

    async for message in query(prompt="Explain AI agents in three sentences.", options=options):
        # 1) Partial updates: filter twice (event type, then nested delta type)
        if isinstance(message, StreamEvent):
            event = message.event
            if event.get("type") == "content_block_delta":
                delta = event.get("delta", {})
                if delta.get("type") == "text_delta":
                    # end="" -> no added newline; flush=True -> show it now
                    print(delta.get("text", ""), end="", flush=True)

        # 2) The run outcome: report it once, separately from the live text
        elif isinstance(message, ResultMessage):
            print()
            print("\n--- Execution complete ---")
            print("[OUTCOME]", message.subtype)
            print("Error:", message.is_error)
            print("Turns:", message.num_turns)
            cost = message.total_cost_usd
            print("Cost (USD):", f"{cost:.4f}" if cost is not None else "unavailable")

        # 3) AssistantMessage still arrives too, but its text was already streamed:
        #    printing it here would duplicate the answer, so it is deliberately ignored.


if __name__ == "__main__":
    asyncio.run(main())
