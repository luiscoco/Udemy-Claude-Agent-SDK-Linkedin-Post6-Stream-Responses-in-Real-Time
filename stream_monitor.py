"""
Live Stream Monitor - Practical Application for Lesson 5
========================================================

Lesson 5 turns on partial message streaming. With include_partial_messages=True,
the SDK yields StreamEvent objects (raw API events) *alongside* the normal messages:

    message_start -> content_block_start -> content_block_delta x N -> content_block_stop
                  -> message_delta -> message_stop          (one model message)

The monitor runs an agent with that switch on and handles every part of the stream:

  * LIVE ANSWER   text_delta fragments are printed the moment they arrive (end="", flush=True),
                  with their spacing preserved exactly
  * TOOL INPUT    input_json_delta fragments are BUFFERED per content-block index and parsed
                  only at content_block_stop - they are never shown as answer text
  * OTHER BLOCKS  thinking / signature deltas are counted, not displayed as the answer
  * LIFECYCLE     block state is reset at every message_start; message_stop is NOT the end of the run
  * NO DUPLICATES the complete AssistantMessage and ResultMessage.result are checked against what
                  was already streamed, instead of being printed a second time
  * OUTCOME       the run report reads ResultMessage (subtype, is_error, turns, cost) and can save
                  ONE canonical final artifact (--save), only when the run succeeded

Usage examples:
    python stream_monitor.py                         # live: Glob on ./project, then a streamed answer
    python stream_monitor.py --events                # live, one line per stream event (lifecycle view)
    python stream_monitor.py "Explain AI agents in three sentences." --no-tools
    python stream_monitor.py --no-partial            # compare: the same run WITHOUT partial updates
    python stream_monitor.py --save output/answer.md # save the final answer once the run succeeds
    python stream_monitor.py --demo text             # offline, no API key needed
    python stream_monitor.py --demo tool --events    # input_json_delta buffering, event by event
    python stream_monitor.py --demo thinking         # thinking + signature deltas are not answer text
    python stream_monitor.py --demo fail             # partial text shown, then the run fails
"""

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    CLINotFoundError,
    ProcessError,
    ResultError,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    query,
)

# Optional: hide the "claude.ai connectors are disabled..." startup warning.
os.environ["ENABLE_CLAUDEAI_MCP_SERVERS"] = "false"

# Windows consoles default to cp1252 and crash on emoji in Claude's replies.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = Path(__file__).resolve().parent
DEFAULT_PROMPT = ("Use the Glob tool to find the Python files in this project. "
                  "Then explain in three short sentences what each file is probably for.")


# ----------------------------------------------------------------------------
# Presentation helpers
# ----------------------------------------------------------------------------
USE_COLOR = sys.stdout.isatty() and "NO_COLOR" not in os.environ
WIDTH = min(shutil.get_terminal_size((100, 20)).columns, 100)


def color(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def dim(t: str) -> str: return color(t, "2")
def ok(t: str) -> str: return color(t, "92")
def bad(t: str) -> str: return color(t, "91")
def warn(t: str) -> str: return color(t, "93")
def bold(t: str) -> str: return color(t, "1")


# Same colors as the "Common stream event types" cards on the lesson slides.
EVENT_COLORS = {
    "message_start": "94", "content_block_start": "94", "content_block_delta": "92",
    "content_block_stop": "95", "message_delta": "33", "message_stop": "33",
}


def short(value: str, n: int = 14) -> str:
    return value if len(value) <= n else value[:n] + "…"


def heading(title: str) -> None:
    print("\n" + "═" * WIDTH + "\n  " + bold(title) + "\n" + "═" * WIDTH)


# ----------------------------------------------------------------------------
# State: one model message and its content blocks
# ----------------------------------------------------------------------------
@dataclass
class Block:
    """One content-block slot (event["index"]) inside the current model message."""
    index: int
    type: str                                  # text | tool_use | thinking | ...
    name: str = ""                             # tool name for tool_use blocks
    tool_id: str = ""
    fragments: list[str] = field(default_factory=list)   # text or partial_json pieces
    deltas: int = 0
    closed: bool = False

    @property
    def joined(self) -> str:
        return "".join(self.fragments)         # join exactly: never strip or add spaces


@dataclass
class ModelMessage:
    number: int
    msg_id: str
    blocks: dict[int, Block] = field(default_factory=dict)
    stop_reason: str | None = None
    stopped: bool = False
    complete_text: str | None = None           # text of the complete AssistantMessage, when it arrives

    @property
    def streamed_text(self) -> str:
        return "".join(b.joined for _, b in sorted(self.blocks.items()) if b.type == "text")


@dataclass
class ToolInput:
    name: str
    tool_id: str
    fragments: int
    raw: str
    parsed: dict | None
    error: str | None


# ----------------------------------------------------------------------------
# The monitor
# ----------------------------------------------------------------------------
class StreamMonitor:
    def __init__(self, events_view: bool, partial: bool):
        self.events_view = events_view
        self.partial = partial
        self.t0 = time.perf_counter()
        self.n = 0                                       # SDK objects received
        self.messages: list[ModelMessage] = []
        self.current: ModelMessage | None = None
        self.delta_counts: dict[str, int] = {}
        self.block_counts: dict[str, int] = {}
        self.tool_inputs: list[ToolInput] = []
        self.tool_results = 0
        self.subagent_events = 0
        self.other_events: dict[str, int] = {}
        self.first_text_at: float | None = None          # first VISIBLE answer text
        self.last_text_at: float | None = None           # last text fragment
        self.complete_at: float | None = None            # last complete AssistantMessage
        self.fallback_prints = 0                         # text printed from a complete message
        self.last_fallback_text = ""
        self.duplicates_avoided = 0
        self.result: ResultMessage | None = None
        self.exception: BaseException | None = None
        self.at_line_start = True

    # ---- output helpers -------------------------------------------------------
    def elapsed(self) -> float:
        return time.perf_counter() - self.t0

    def line(self, text: str = "") -> None:
        """Print a status line, ending any live text that is still on the current line."""
        if not self.at_line_start:
            print()
        print(text)
        self.at_line_start = True

    def mark_text(self) -> None:
        now = self.elapsed()
        if self.first_text_at is None:
            self.first_text_at = now
        self.last_text_at = now

    def live(self, fragment: str) -> None:
        """Show answer text immediately. The fragment is printed as-is (only indented after newlines)."""
        self.mark_text()
        if self.at_line_start:
            print("   " + color("│ ", "94"), end="")
        print(fragment.replace("\n", "\n   " + color("│ ", "94")), end="", flush=True)
        self.at_line_start = fragment.endswith("\n")

    def ev(self, kind: str, detail: str = "") -> None:
        """--events view: one line per stream event."""
        label = color(f"{kind:<20}", EVENT_COLORS.get(kind, "37"))
        self.line(f" {dim(f'#{self.n:<3}')} {label} {detail}")

    # ---- routing --------------------------------------------------------------
    def on_message(self, message) -> None:
        self.n += 1
        if isinstance(message, StreamEvent):
            self.on_stream_event(message)
        elif isinstance(message, AssistantMessage):
            self.on_assistant(message)
        elif isinstance(message, UserMessage):
            if isinstance(message.content, list):        # check the shape before iterating
                for block in message.content:
                    if isinstance(block, ToolResultBlock):
                        self.on_tool_result(block)
        elif isinstance(message, ResultMessage):
            self.result = message
            if self.events_view:
                self.ev("ResultMessage", f"subtype={message.subtype!r}  (the RUN is complete)")
        elif isinstance(message, SystemMessage):
            if self.events_view and message.subtype == "init":
                tools = message.data.get("tools", [])
                self.ev("SystemMessage", dim(f"'init': run started, tools={tools}"))
        # RateLimitEvent and other informational objects are ignored on purpose.

    # ---- StreamEvent: the raw API events ---------------------------------------
    def on_stream_event(self, se: StreamEvent) -> None:
        if se.parent_tool_use_id:                        # events from a subagent's own messages
            self.subagent_events += 1
            return
        event = se.event
        kind = event.get("type", "?")

        if kind == "message_start":
            msg = event.get("message", {})
            # Reset per-message block tracking: indices restart at 0 in every message.
            self.current = ModelMessage(len(self.messages) + 1, msg.get("id", ""))
            self.messages.append(self.current)
            if self.events_view:
                self.ev(kind, f"model message {self.current.number}  {dim(short(self.current.msg_id, 18))}"
                              f"  {dim('(block tracking reset)')}")
            else:
                self.line(dim(f"── model message {self.current.number} " + "─" * 30))
            return

        cur = self.current
        if cur is None:                                  # defensive: an event before message_start
            self.other_events[kind] = self.other_events.get(kind, 0) + 1
            return

        if kind == "content_block_start":
            index = event.get("index", 0)
            cb = event.get("content_block", {})
            block = Block(index, cb.get("type", "?"), cb.get("name", ""), cb.get("id", ""))
            cur.blocks[index] = block
            self.block_counts[block.type] = self.block_counts.get(block.type, 0) + 1
            if self.events_view:
                extra = f" {block.name} {dim(short(block.tool_id, 16))}" if block.type == "tool_use" else ""
                self.ev(kind, f"[{index}] {block.type}{extra}")
            elif block.type == "tool_use":
                if not self.at_line_start:
                    print()
                # No newline: one progress dot per input fragment follows on this line.
                print(f"   {color('⚙', '95')} tool_use [{index}] {bold(block.name)}  "
                      f"{dim('buffering input ')}", end="", flush=True)
                self.at_line_start = False
            elif block.type == "thinking":
                self.line(dim(f"   💭 thinking block [{index}]: not answer text, not displayed"))
            elif block.type != "text":
                self.line(dim(f"   ({block.type} block [{index}]: not answer text)"))

        elif kind == "content_block_delta":
            index = event.get("index", 0)
            delta = event.get("delta", {})
            dtype = delta.get("type", "?")
            self.delta_counts[dtype] = self.delta_counts.get(dtype, 0) + 1
            block = cur.blocks.get(index)
            if block is None:                            # a delta for an unknown slot: track it anyway
                block = cur.blocks[index] = Block(index, "unknown")
            block.deltas += 1

            if dtype == "text_delta":                    # 1) answer text: show it now
                text = delta.get("text", "")
                block.fragments.append(text)
                if self.events_view:
                    self.ev(kind, f"[{index}] text_delta        {color(repr(text), '92')}")
                    self.mark_text()
                else:
                    self.live(text)
            elif dtype == "input_json_delta":            # 2) tool arguments: buffer, never print as text
                piece = delta.get("partial_json", "")
                block.fragments.append(piece)
                if self.events_view:
                    self.ev(kind, f"[{index}] input_json_delta  {color(repr(piece), '95')}"
                                  f"  {dim(f'→ buffer ({len(block.fragments)})')}")
                else:
                    print(color("·", "95"), end="", flush=True)
            else:                                        # 3) thinking_delta, signature_delta, ...
                if self.events_view:
                    self.ev(kind, f"[{index}] {dtype:<17} {dim('(counted, not answer text)')}")

        elif kind == "content_block_stop":
            index = event.get("index", 0)
            block = cur.blocks.get(index)
            if block is None:
                return
            block.closed = True
            detail = f"[{index}] {block.type} closed after {block.deltas} delta(s)"
            if block.type == "tool_use":
                detail += self.finish_tool_input(block)
            if self.events_view:
                self.ev(kind, detail)

        elif kind == "message_delta":
            cur.stop_reason = event.get("delta", {}).get("stop_reason")
            if self.events_view:
                self.ev(kind, f"stop_reason={cur.stop_reason!r}")

        elif kind == "message_stop":
            cur.stopped = True
            if self.events_view:
                self.ev(kind, dim("model message ended (the RUN may continue with tools)"))
            elif cur.stop_reason == "tool_use":
                self.line(dim("   message_stop: this model message ended, but the run continues with a tool"))

        else:                                            # ping, error, or future event types
            self.other_events[kind] = self.other_events.get(kind, 0) + 1
            if self.events_view:
                self.ev(kind, dim("(other event: counted)"))

    def finish_tool_input(self, block: Block) -> str:
        """Parse the buffered tool input only now that its block is complete."""
        raw = block.joined
        parsed, error = None, None
        try:
            parsed = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            error = str(exc)
        self.tool_inputs.append(ToolInput(block.name, block.tool_id, len(block.fragments), raw, parsed, error))
        if error:
            msg = f"invalid JSON after {len(block.fragments)} fragment(s): {error}"
            if not self.events_view:
                self.line(f"   {bad('✘')} {block.name} input {bad(msg)}")
            return "  " + bad(msg)
        shown = json.dumps(parsed, ensure_ascii=False)
        if not self.events_view:
            self.line(f"   {ok('✔')} {block.name} input complete → {color(shown, '95')}  "
                      f"{dim(f'({len(block.fragments)} fragments joined, buffered, not shown as answer)')}")
        return f"  → joined input {color(shown, '95')}"

    # ---- complete messages ---------------------------------------------------
    def on_assistant(self, message: AssistantMessage) -> None:
        if message.parent_tool_use_id:
            return
        texts = [b.text for b in message.content if isinstance(b, TextBlock)]
        # Find the streamed model message this complete message belongs to. The CLI can split one
        # model message into several AssistantMessages (one per block), so texts are accumulated.
        streamed = None
        if message.message_id:
            streamed = next((m for m in self.messages if m.msg_id == message.message_id), None)
        elif self.partial and self.current is not None:
            streamed = self.current
        if texts:
            self.complete_at = self.elapsed()
        if streamed is not None:
            streamed.complete_text = (streamed.complete_text or "") + "".join(texts)
            if texts:
                self.duplicates_avoided += 1
            if self.events_view:
                kinds = ", ".join(type(b).__name__ for b in message.content)
                self.ev("AssistantMessage", dim(f"complete message [{kinds}]: already streamed, not printed again"))
            return
        # Not streamed (for example --no-partial): the complete message is the only source of text.
        if not self.partial and not self.events_view:
            self.line(dim("── complete AssistantMessage " + "─" * 22))
        for block in message.content:
            if isinstance(block, TextBlock):
                self.fallback_prints += 1
                self.last_fallback_text = block.text
                self.live(block.text)
            elif isinstance(block, ToolUseBlock):
                self.tool_inputs.append(ToolInput(block.name, block.id, 0, json.dumps(block.input), block.input, None))
                self.line(f"   {color('⚙', '95')} {bold(block.name)} input (complete, no fragments) → "
                          f"{color(json.dumps(block.input), '95')}")
            elif isinstance(block, ThinkingBlock):
                self.line(dim("   💭 thinking block: not answer text, not displayed"))

    def on_tool_result(self, block: ToolResultBlock) -> None:
        self.tool_results += 1
        content = block.content
        if isinstance(content, list):
            content = "\n".join(str(i.get("text", "")) for i in content if isinstance(i, dict))
        paths = [p for p in (content or "").splitlines() if p.strip() and p.strip() != "No files found"]
        status = bad("tool error") if block.is_error else f"{len(paths)} line(s) returned"
        if self.events_view:
            self.ev("UserMessage", f"ToolResultBlock: {status}  {dim('(the SDK ran the tool)')}")
        else:
            self.line(f"   {ok('✔')} tool result: {status}  {dim('(the SDK ran the tool; Claude continues)')}")


# ----------------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------------
def print_report(m: StreamMonitor, save: Path | None) -> None:
    m.line()
    heading("RUN REPORT")
    r = m.result

    print(f"\n  {bold('Stream summary')}  {dim('(include_partial_messages=' + str(m.partial) + ')')}")
    print(f"    model messages     {len(m.messages)}  {dim('(message_start … message_stop)')}")
    blocks = ", ".join(f"{k}: {v}" for k, v in m.block_counts.items()) or "none streamed"
    print(f"    content blocks     {blocks}")
    deltas = ", ".join(f"{k} {v}" for k, v in m.delta_counts.items()) or "none"
    print(f"    deltas             {deltas}")
    if m.other_events:
        print(f"    other events       {m.other_events}")
    if m.subagent_events:
        print(f"    subagent events    {m.subagent_events} (parent_tool_use_id set: skipped)")
    if m.first_text_at is not None:
        print(f"    first answer text  {m.first_text_at:.2f}s  {dim('(when the user first saw something)')}")
        print(f"    last answer text   {m.last_text_at:.2f}s  "
              f"{dim(f'(text streamed for {m.last_text_at - m.first_text_at:.2f}s)')}")
    if m.complete_at is not None:
        print(f"    complete message   {m.complete_at:.2f}s  {dim('(when a non-streaming app could print it)')}")

    if m.tool_inputs:
        print(f"\n  {bold('Tool inputs')}  {dim('(buffered per block index, parsed at content_block_stop)')}")
        for t in m.tool_inputs:
            state = bad(f"invalid JSON: {t.error}") if t.error else ok("valid JSON")
            shown = json.dumps(t.parsed) if t.parsed is not None else repr(t.raw)
            print(f"    {t.name:<6} {shown}  {t.fragments} fragment(s) → {state}")

    print(f"\n  {bold('No duplicate output')}")
    # The last answer text the user already saw: streamed, or printed from a complete message.
    final_streamed = next((x.streamed_text for x in reversed(m.messages) if x.streamed_text),
                          m.last_fallback_text)
    for x in m.messages:
        if x.complete_text is None or not x.streamed_text:
            continue
        same = x.streamed_text == x.complete_text
        mark = ok("✔") if same else warn("⚠")
        note = "identical: the complete message was not printed again" if same else \
            "differs from the complete message (inspect your delta handling)"
        print(f"    {mark} message {x.number}: streamed text {note}")
    if m.fallback_prints:
        print(f"    {warn('•')} {m.fallback_prints} text block(s) printed from complete AssistantMessages "
              f"{dim('(no partial updates for them)')}")
    if r is not None and r.result:
        if r.result.strip() == final_streamed.strip() and final_streamed:
            print(f"    {ok('✔')} ResultMessage.result repeats the answer already shown: not printed again")
        else:
            print(f"    {warn('•')} ResultMessage.result: {short(r.result.replace(chr(10), ' '), 70)!r}")
    elif r is not None:
        print(f"    {warn('•')} ResultMessage.result is absent: never rely on it being there")

    print(f"\n  {bold('Run outcome')}  {dim('(ResultMessage: the run, not one model message)')}")
    if r is None:
        print("    " + bad("no ResultMessage received"))
    else:
        print(f"    subtype            {r.subtype!r}  ({ok('✔ success') if not r.is_error else bad('✘ error')})")
        print(f"    is_error           {r.is_error}")
        print(f"    num_turns          {r.num_turns}")
        cost = f"${r.total_cost_usd:.4f}" if r.total_cost_usd is not None else "unavailable (not invented)"
        print(f"    total_cost_usd     {cost}")
        if r.terminal_reason:
            print(f"    terminal_reason    {r.terminal_reason!r}")
    if m.exception is not None:
        print(f"    exception          {bad(type(m.exception).__name__)}: {m.exception}")

    succeeded = r is not None and not r.is_error and m.exception is None
    print(f"\n  {bold('Verdict')}")
    if succeeded:
        print("    " + ok("The run succeeded. Incremental text was shown once; the outcome is reported separately."))
    elif m.first_text_at is not None:
        print("    " + bad("Partial text was shown, but the run did NOT succeed."))
        print("    " + dim("Partial text is presentation, not proof of success: show the error status in the UI."))
    else:
        print("    " + bad("The run did not succeed and produced no answer text."))

    if save is not None:
        answer = (r.result if r is not None and r.result else final_streamed)
        if succeeded and answer:
            save.parent.mkdir(parents=True, exist_ok=True)
            save.write_text(answer + "\n", encoding="utf-8")
            print(f"\n  {ok('✔')} Final artifact saved once: {save}")
        else:
            print(f"\n  {warn('•')} Nothing saved to {save}: only a successful run's final answer is saved.")


# ----------------------------------------------------------------------------
# Offline demo streams (real SDK classes, same order as a live run)
# ----------------------------------------------------------------------------
SESSION = "demo-session"
MODEL = "claude-demo"


def se(event: dict) -> StreamEvent:
    return StreamEvent(uuid=str(uuid.uuid4()), session_id=SESSION, event=event)


def result(subtype: str, turns: int, text: str | None, is_error: bool = False) -> ResultMessage:
    return ResultMessage(subtype=subtype, duration_ms=3900, duration_api_ms=3600, is_error=is_error,
                         num_turns=turns, session_id=SESSION, total_cost_usd=0.0114, result=text,
                         terminal_reason="max_turns" if subtype == "error_max_turns" else "completed")


async def text_message(msg_id: str, fragments: list[str], pause: float, index: int = 0, stop="end_turn",
                       before: list | None = None):
    """Yield one streamed model message with a single text block, like a live run."""
    yield se({"type": "message_start", "message": {"id": msg_id, "role": "assistant", "content": []}})
    for item in before or []:
        yield item
    yield se({"type": "content_block_start", "index": index, "content_block": {"type": "text", "text": ""}})
    for f in fragments:
        await asyncio.sleep(pause)
        yield se({"type": "content_block_delta", "index": index, "delta": {"type": "text_delta", "text": f}})
    # Live runs deliver the complete AssistantMessage right after the last delta, before content_block_stop.
    yield AssistantMessage(content=[TextBlock(text="".join(fragments))], model=MODEL, message_id=msg_id)
    yield se({"type": "content_block_stop", "index": index})
    yield se({"type": "message_delta", "delta": {"stop_reason": stop}})
    yield se({"type": "message_stop"})


async def tool_message(msg_id: str, tool_id: str, json_parts: list[str], pause: float, lead_text: str = ""):
    """Yield a model message that (optionally) says something, then streams a Glob tool request."""
    yield se({"type": "message_start", "message": {"id": msg_id, "role": "assistant", "content": []}})
    content, index = [], 0
    if lead_text:
        yield se({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}})
        for f in lead_text.split("|"):
            await asyncio.sleep(pause)
            yield se({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": f}})
        yield se({"type": "content_block_stop", "index": 0})
        content.append(TextBlock(text=lead_text.replace("|", "")))
        index = 1
    yield se({"type": "content_block_start", "index": index,
              "content_block": {"type": "tool_use", "id": tool_id, "name": "Glob", "input": {}}})
    for p in json_parts:
        await asyncio.sleep(pause)
        yield se({"type": "content_block_delta", "index": index,
                  "delta": {"type": "input_json_delta", "partial_json": p}})
    content.append(ToolUseBlock(id=tool_id, name="Glob", input=json.loads("".join(json_parts))))
    yield AssistantMessage(content=content, model=MODEL, message_id=msg_id)
    yield se({"type": "content_block_stop", "index": index})
    yield se({"type": "message_delta", "delta": {"stop_reason": "tool_use"}})
    yield se({"type": "message_stop"})


async def demo_stream(scenario: str, partial: bool, pause: float):
    yield SystemMessage(subtype="init", data={"model": MODEL, "tools": ["Glob"]})
    answer_tool = ["There are", " 3 Python files", ": `agent.py` is", " the entry point,",
                   " `src/helpers.py` holds", " shared helpers, and", " `tests/test_agent.py`",
                   " tests the agent."]

    if scenario == "text":
        # Leading spaces and punctuation: printing the fragments as-is keeps the sentence intact.
        frags = ["AI agents", " use a language", " model to decide", " what to do next.", " They can",
                 " call tools", ", read the results", ", and keep going", " until the task is done.",
                 "\nThat loop", " is what makes", " them useful", " for real work."]
        async for x in text_message("msg_demo_text", frags, pause):
            yield x
        yield result("success", 1, "".join(frags))

    elif scenario == "tool":
        async for x in tool_message("msg_demo_01", "toolu_demo_01",
                                    ["", '{"pattern": "**/*', '.py', '"}'], pause,
                                    lead_text="I'll look| for the Python files."):
            yield x
        yield UserMessage(content=[ToolResultBlock(tool_use_id="toolu_demo_01",
                                                   content="agent.py\nsrc/helpers.py\ntests/test_agent.py")])
        async for x in text_message("msg_demo_02", answer_tool, pause):
            yield x
        yield result("success", 2, "".join(answer_tool))

    elif scenario == "thinking":
        thinking = [se({"type": "content_block_start", "index": 0,
                        "content_block": {"type": "thinking", "thinking": ""}}),
                    se({"type": "content_block_delta", "index": 0,
                        "delta": {"type": "thinking_delta", "thinking": "The user wants a short answer…"}}),
                    se({"type": "content_block_delta", "index": 0,
                        "delta": {"type": "thinking_delta", "thinking": " three sentences."}}),
                    se({"type": "content_block_delta", "index": 0,
                        "delta": {"type": "signature_delta", "signature": "EqQBCkYIBxgCKkB…"}}),
                    se({"type": "content_block_stop", "index": 0})]
        frags = ["Streaming", " shows the answer", " while it is", " being generated."]
        async for x in text_message("msg_demo_think", frags, pause, index=1, before=thinking):
            yield x
        yield result("success", 1, "".join(frags))

    elif scenario == "fail":
        # Claude streams some text and a tool request, then the turn limit ends the run.
        async for x in tool_message("msg_demo_f1", "toolu_demo_f1", ['{"pattern":', ' "src/*.py"}'], pause,
                                    lead_text="Let me| start by| listing the source files."):
            yield x
        yield UserMessage(content=[ToolResultBlock(tool_use_id="toolu_demo_f1", content="src/helpers.py")])
        yield result("error_max_turns", 2, None, is_error=True)
        raise ResultError("Claude Code returned an error result: Reached maximum number of turns (1)",
                          {"subtype": "error_max_turns", "terminal_reason": "max_turns", "errors": []}, 1)


async def without_partials(stream):
    """--no-partial in demo mode: drop StreamEvents, exactly what the SDK does when the switch is off."""
    async for x in stream:
        if not isinstance(x, StreamEvent):
            yield x


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
async def run(args) -> int:
    partial = not args.no_partial
    cwd = Path(args.cwd).resolve() if args.cwd else HERE / "project"
    tools = [] if args.no_tools else ["Glob"]
    prompt = args.prompt or ("Explain AI agents in three sentences." if args.no_tools else DEFAULT_PROMPT)

    print(bold("📡  LIVE STREAM MONITOR") + dim("  ·  Claude Agent SDK, Lesson 5"))
    print(f"    include_partial_messages  {partial}   {dim('(the switch: adds StreamEvent objects)')}")
    print(f"    tools / allowed_tools     {tools}")
    print(f"    cwd                       {cwd}")
    print(f"    max_turns                 {args.max_turns}")
    print(f"    view                      {'one line per event (--events)' if args.events else 'live answer'}")
    if args.demo:
        print(f"    mode                      {warn('offline demo: ' + args.demo)}  {dim('(no API call, no cost)')}")
    else:
        print(f"    prompt                    {prompt!r}")

    monitor = StreamMonitor(events_view=args.events, partial=partial)
    if args.demo:
        stream = demo_stream(args.demo, partial, pause=0.0 if args.events else args.pause)
        if not partial:
            stream = without_partials(stream)
    else:
        options = ClaudeAgentOptions(
            include_partial_messages=partial,   # the switch this lesson is about
            tools=tools,                        # the only tools the agent can see
            allowed_tools=tools,                # ...and may run without asking
            cwd=str(cwd),
            max_turns=args.max_turns,
            setting_sources=[],                 # ignore local Claude settings for a reproducible run
            strict_mcp_config=True,             # don't load MCP servers configured elsewhere
        )
        stream = query(prompt=prompt, options=options)

    print()
    try:
        async for message in stream:
            monitor.on_message(message)
    except ResultError as exc:                  # must come before ProcessError (its parent class)
        monitor.exception = exc
        monitor.line(bad(f"\n✘ ResultError: subtype={exc.subtype!r}, terminal_reason={exc.terminal_reason!r}"))
    except CLINotFoundError as exc:
        monitor.exception = exc
        monitor.line(bad(f"\n✘ Claude Code CLI not found: {exc}"))
    except ProcessError as exc:
        monitor.exception = exc
        monitor.line(bad(f"\n✘ The agent process failed (exit code {exc.exit_code}): {exc}"))
    except Exception as exc:                    # missing API key, network error, ...
        monitor.exception = exc
        monitor.line(bad(f"\n✘ {type(exc).__name__}: {exc}"))

    print_report(monitor, Path(args.save) if args.save else None)
    ok_run = monitor.result is not None and not monitor.result.is_error and monitor.exception is None
    return 0 if ok_run else 1


def main() -> None:
    p = argparse.ArgumentParser(description="Lesson 5: stream an agent's answer and handle every StreamEvent.")
    p.add_argument("prompt", nargs="?", help="what to ask Claude (default: Glob the ./project fixture, then explain)")
    p.add_argument("--events", action="store_true", help="print one line per stream event (lifecycle view)")
    p.add_argument("--no-partial", action="store_true", help="turn include_partial_messages OFF to compare")
    p.add_argument("--no-tools", action="store_true", help="run without the Glob tool (a plain streamed answer)")
    p.add_argument("--cwd", help="working folder for the run (default: ./project)")
    p.add_argument("--max-turns", type=int, default=5, help="ceiling on agent turns (default: 5)")
    p.add_argument("--save", help="save the final answer to this file, only if the run succeeds")
    p.add_argument("--demo", choices=["text", "tool", "thinking", "fail"], help="offline scenario, no API key")
    p.add_argument("--pause", type=float, default=0.08, help="seconds between demo fragments (default: 0.08)")
    args = p.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
