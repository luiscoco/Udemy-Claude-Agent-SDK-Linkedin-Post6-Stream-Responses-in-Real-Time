# Lesson 5 – Practical Application: Live Stream Monitor

> **Claude Agent SDK · Lesson 5 – Streaming Agent Responses in Real Time**
> Turn on partial message streaming, print Claude's answer while it is being written, keep tool-input
> fragments out of the answer, and finish the run without printing anything twice.

---

## 1. Purpose of the application

Until now, every program in this course waited for **complete** messages. Claude finished a whole answer, and
only then did your loop print it. For a long answer, the user stares at an empty screen for several seconds.

In Lesson 5 you switch on **partial message streaming**:

```python
options = ClaudeAgentOptions(include_partial_messages=True)
```

The SDK keeps sending the normal messages (`AssistantMessage`, `UserMessage`, `ResultMessage`) and **adds**
`StreamEvent` objects: the raw, incremental events of the API stream. Every model message follows the same
lifecycle:

```
message_start ─► content_block_start ─► content_block_delta × N ─► content_block_stop ─► message_delta ─► message_stop
                 └──────────── repeated for each content block, identified by event["index"] ────────────┘
```

This folder contains two programs and a small fixture folder. Run the programs **in this order**:

| File | What it does |
|---|---|
| `lesson5.py` | The compact teaching script, which is *the complete lesson5.py* from the slides. It enables streaming, prints every `text_delta` the moment it arrives, and reports the `ResultMessage` once at the end. |
| `stream_monitor.py` | The practical application. It streams the answer **live**, **buffers** tool-input fragments and parses them only when their block is complete, skips thinking deltas, and **checks for duplicates** instead of printing complete messages again. It ends with a **run report**. It can show the stream **event by event** (`--events`), compare against a run **without** streaming (`--no-partial`), save **one** final artifact (`--save`), and has an **offline demo mode** with four scenarios. |
| `project/` | A small fixture folder (`agent.py`, `README.md`, `src/helpers.py`, `tests/test_agent.py`). The monitor's default prompt asks Claude to search it with `Glob`, so you can see a tool request stream in. |

### Concepts this application explains

| Concept (slide) | Where you see it |
|---|---|
| **`include_partial_messages=True` is the switch** (slide 1) | Printed at the top of every run. `--no-partial` turns it off, and the stream summary then shows `0` model messages streamed. |
| **Normal SDK messages still arrive** (slide 1) | In `--events`, `AssistantMessage`, `UserMessage` and `ResultMessage` appear **between** the stream events. |
| **`StreamEvent` carries raw event data in `message.event`** (slide 1) | `on_stream_event()` reads `se.event["type"]`, `["index"]`, `["delta"]` (section 4.2 c). |
| **This is output streaming, not streaming user input** (slide 1) | The prompt is still one plain string. Only Claude's **output** streams. |
| **The lifecycle: start, block start/delta/stop, message delta, message stop** (slide 2) | `--events` prints one numbered line per event, colored like the slide's event cards. |
| **`event["index"]` identifies the content-block slot** (slide 2) | Every event line shows `[0]`, `[1]`…, and fragments are stored **per index**. |
| **Reset per-message block tracking at `message_start`** (slide 2) | `(block tracking reset)` on every `message_start` line (section 4.2 c). |
| **`message_stop` is not the same as `ResultMessage`** (slide 2) | `message_stop: this model message ended, but the run continues with a tool`. Only `ResultMessage` says `(the RUN is complete)`. |
| **Filter twice: event type, then nested delta type** (slide 3) | The `content_block_delta` branch switches on `delta["type"]` (section 4.2 d). |
| **Preserve each fragment; print immediately with `end=""`, `flush=True`** (slide 3) | `--events` shows each fragment with `repr()`, so you can see the leading spaces. The live view prints them untouched. |
| **`text_delta` → visible answer; `input_json_delta` → buffer** (slide 4) | Text goes to the `│` answer area. Tool input shows only as progress dots `····`, then the joined JSON. |
| **A single JSON fragment may be invalid; parse at completion** (slide 4) | The fragment `'{"pattern": "**/*'` is invalid on its own. `finish_tool_input()` parses only at `content_block_stop` (section 4.2 e). |
| **Observe, do not replay** (slide 4) | The monitor never runs `Glob`. It only reports `the SDK ran the tool`. |
| **Keep room for other events (thinking, signatures)** (slide 4) | `💭 thinking block: not answer text`. `thinking_delta` and `signature_delta` are counted, never printed. |
| **Avoid double printing** (slide 5) | The *No duplicate output* report section compares the streamed text with the complete `AssistantMessage` and with `ResultMessage.result`. |
| **Do not equate partial text with success** (slide 5) | `--demo fail`: text was shown, then the verdict says *"Partial text was shown, but the run did NOT succeed"*. |
| **Keep optional metadata optional** (slide 5) | Missing cost is printed as `unavailable (not invented)`. A missing result prints *"never rely on it being there"*. |
| **Save one canonical final artifact** (slide 5) | `--save output/answer.md` writes the final answer **once**, and only when the run succeeded. |

---

## 2. How to configure the application

### Prerequisites

- **Python 3.10 or newer**: check with `python --version`
- **One way to authenticate with Claude**, either:
  - an **Anthropic API key** from [platform.claude.com](https://platform.claude.com) (it starts with `sk-ant-`), or
  - **Claude Code** installed and logged in on this computer (the SDK runs the Claude Code CLI under the hood, so
    it can use that login).
- *Neither is needed for `--demo` mode.*

### Step 1 – Create and activate a virtual environment (Windows PowerShell)

Open a terminal **in this folder** and run:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

> macOS / Linux: `python3 -m venv .venv` then `source .venv/bin/activate`

### Step 2 – Install the Claude Agent SDK

```powershell
python -m pip install -r requirements.txt
```

### Step 3 – Set your API key for this terminal session

```powershell
$env:ANTHROPIC_API_KEY = "your-api-key"
```

> macOS / Linux: `export ANTHROPIC_API_KEY="your-api-key"`

🔐 **Treat your key like a password.** Keep it in your environment. Never hard-code it in a `.py` file and never
commit it to Git. (Skip this step if you are logged in to Claude Code.)

### Step 4 – Choose what to configure

Both programs are configured through `ClaudeAgentOptions`:

| Option | `lesson5.py` | `stream_monitor.py` | Meaning |
|---|---|---|---|
| `include_partial_messages` | `True` | `True` (turn off with `--no-partial`) | **The switch.** Adds `StreamEvent` objects to the stream. |
| `tools` / `allowed_tools` | `[]` (no tools) | `["Glob"]` (none with `--no-tools`) | What the agent can see / may run without asking. |
| `cwd` | (current folder) | `./project` (change with `--cwd`) | The folder the run searches in. |
| `max_turns` | (SDK default) | `5` (change with `--max-turns`) | Ceiling on agent turns. |
| `setting_sources` | `[]` | `[]` | Ignore local Claude settings, so every student gets the same behavior. |

The monitor also has display options: `--events` (one line per event), `--save FILE` (final artifact) and
`--pause SECONDS` (speed of the demo streams). Both programs switch the Windows console to UTF-8 themselves, so
emoji in Claude's answers can't crash them.

---

## 3. How to run the application

```powershell
# 1) The teaching script: watch the answer appear word by word
python lesson5.py

# 2) The monitor, offline: no API key and no cost
python stream_monitor.py --demo text
python stream_monitor.py --demo tool
python stream_monitor.py --demo tool --events
python stream_monitor.py --demo thinking --events
python stream_monitor.py --demo fail

# 3) The monitor, live: Glob on ./project, then a streamed explanation
python stream_monitor.py
python stream_monitor.py --events

# 4) Live comparisons and options
python stream_monitor.py "Write about 250 words on why streaming matters in AI apps." --no-tools
python stream_monitor.py "Write about 250 words on why streaming matters in AI apps." --no-tools --no-partial
python stream_monitor.py --save output/answer.md
python stream_monitor.py --max-turns 1 --save output/answer.md
```

| Demo scenario | What it shows |
|---|---|
| `text` | One text block in 13 fragments, with leading spaces, punctuation and a newline. Joined exactly as received, the sentence stays intact. |
| `tool` | Model message 1 says a sentence, then streams a `Glob` request as 4 `input_json_delta` fragments (the first is empty). After the tool result, model message 2 streams the answer. |
| `thinking` | A thinking block (`thinking_delta` ×2, `signature_delta` ×1) at index 0, then the answer at index 1. Only index 1 is shown. |
| `fail` | Claude streams a sentence and a tool request, then the turn limit ends the run: `error_max_turns`, no result text, a caught `ResultError`. |

### Expected output (real live run of `python stream_monitor.py`, colors removed)

```text
📡  LIVE STREAM MONITOR  ·  Claude Agent SDK, Lesson 5
    include_partial_messages  True   (the switch: adds StreamEvent objects)
    tools / allowed_tools     ['Glob']
    cwd                       …\Lesson 5\Practical Application Example\project
    max_turns                 5
    view                      live answer
    prompt                    'Use the Glob tool to find the Python files in this project. Then explain in three short sentences what each file is probably for.'

── model message 1 ──────────────────────────────
   ⚙ tool_use [0] Glob  buffering input ····
   ✔ Glob input complete → {"pattern": "**/*.py"}  (4 fragments joined, buffered, not shown as answer)
   message_stop: this model message ended, but the run continues with a tool
   ✔ tool result: 3 line(s) returned  (the SDK ran the tool; Claude continues)
── model message 2 ──────────────────────────────
   💭 thinking block [0]: not answer text, not displayed
   │ I found three Python files. I only read the file names, not their contents, so these are guesses:
   │
   │ - **`agent.py`** is probably the main script that sets up and runs the Claude agent with the Claude Agent SDK.
   │ - **`src/helpers.py`** probably holds small shared functions that `agent.py` uses, such as formatting or file handling.
   │ - **`tests/test_agent.py`** probably contains tests that check the agent works as expected, likely run with pytest.

════════════════════════════════════════════════════════════════
  RUN REPORT
════════════════════════════════════════════════════════════════

  Stream summary  (include_partial_messages=True)
    model messages     2  (message_start … message_stop)
    content blocks     tool_use: 1, thinking: 1, text: 1
    deltas             input_json_delta 4, thinking_delta 2, signature_delta 1, text_delta 28
    first answer text  16.56s  (when the user first saw something)
    …

  Tool inputs  (buffered per block index, parsed at content_block_stop)
    Glob   {"pattern": "**/*.py"}  4 fragment(s) → valid JSON

  No duplicate output
    ✔ message 2: streamed text identical: the complete message was not printed again
    ✔ ResultMessage.result repeats the answer already shown: not printed again

  Run outcome  (ResultMessage: the run, not one model message)
    subtype            'success'  (✔ success)
    is_error           False
    num_turns          2
    total_cost_usd     $0.0124
    terminal_reason    'completed'

  Verdict
    The run succeeded. Incremental text was shown once; the outcome is reported separately.
```

Your IDs, timings, cost, fragment boundaries and wording will differ. Claude decides how to phrase the answer, and
the API decides where each fragment ends.

### The same run, event by event (`--events`, excerpt from a real run)

```text
 #3   message_start        model message 1  msg_011CgjSFCtvaYe…  (block tracking reset)
 #4   content_block_start  [0] tool_use Glob toolu_01Dm6VQvab…
 #5   content_block_delta  [0] input_json_delta  ''  → buffer (1)
 #7   content_block_delta  [0] input_json_delta  '{"pattern": "**/*'  → buffer (2)
 #8   content_block_delta  [0] input_json_delta  '.py'  → buffer (3)
 #9   content_block_delta  [0] input_json_delta  '"}'  → buffer (4)
 #10  AssistantMessage     complete message [ToolUseBlock]: already streamed, not printed again
 #11  content_block_stop   [0] tool_use closed after 4 delta(s)  → joined input {"pattern": "**/*.py"}
 #12  message_delta        stop_reason='tool_use'
 #13  message_stop         model message ended (the RUN may continue with tools)
 #14  UserMessage          ToolResultBlock: 3 line(s) returned  (the SDK ran the tool)
 #16  message_start        model message 2  msg_011CgjSFJvZdYD…  (block tracking reset)
 #17  content_block_start  [0] thinking
 #19  content_block_delta  [0] thinking_delta    (counted, not answer text)
 #22  content_block_delta  [0] signature_delta   (counted, not answer text)
 #23  AssistantMessage     complete message [ThinkingBlock]: already streamed, not printed again
 #24  content_block_stop   [0] thinking closed after 3 delta(s)
 #25  content_block_start  [1] text
 #26  content_block_delta  [1] text_delta        'I found three Python'
 #27  content_block_delta  [1] text_delta        ' files. I'
 #28  content_block_delta  [1] text_delta        ' only'
 …
 #55  AssistantMessage     complete message [TextBlock]: already streamed, not printed again
 #56  content_block_stop   [1] text closed after 29 delta(s)
 #57  message_delta        stop_reason='end_turn'
 #58  message_stop         model message ended (the RUN may continue with tools)
 #59  ResultMessage        subtype='success'  (the RUN is complete)
```

Message numbers skip (`#5 → #7`) because the monitor stays quiet about informational objects this lesson doesn't
need, such as `SystemMessage 'status'` and `RateLimitEvent`.

**Five things the live run teaches that the slides only hint at:**

1. **The complete `AssistantMessage` arrives *before* `content_block_stop`.** It comes right after the last delta
   (`#10` before `#11`). Don't use the complete message as a signal that the block has closed, and don't print it:
   its text has already been streamed.
2. **The first `input_json_delta` is an empty string**, and `'{"pattern": "**/*'` is invalid JSON on its own.
   This is exactly why slide 4 says *"parse assembled tool input only when the block is complete"*.
3. **One model message can produce several `AssistantMessage` objects**, one per block (the `ThinkingBlock` in
   `#23`, then the `TextBlock` in `#55`). The monitor matches them to the stream by `message_id`.
4. **A thinking block can appear even though nobody asked for one.** Its index is `0`, so the answer text is at
   index `1`. Never assume the answer is always block `0`.
5. **Short answers can arrive in a burst.** In the run above, the 28 text fragments arrived within a few
   milliseconds, because the answer is short. With the 250-word prompt from command 4, a real run received 114
   fragments spread over 5.5 seconds: that's when streaming really makes a difference. Compare it with `--no-partial`.

### Exercises: "Try it"

1. Run `--demo text --events`. Find the fragments with a **leading space**. What would the sentence look like if
   you called `.strip()` on each fragment? *(Slide 3's "Preserve the fragment".)*
2. Run `--demo tool --events`. Which event first lets you parse the tool input? What would `json.loads()` say
   about fragment 2 on its own? *(Slide 4's "Wait for completeness".)*
3. Run the 250-word prompt from command 4 with and without `--no-partial`. Compare *first answer text* in the two
   reports. *(Slide 1: "With partial updates" vs. "Without partial updates".)*
4. Run `--demo thinking --events`. At which index is the answer text? *(Slide 4's "Keep room for other events".)*
5. Run `python stream_monitor.py --max-turns 1 --save output/answer.md`. Does a `message_stop` appear? Did the run
   succeed? Was a file saved? *(Slide 2: "message_stop is not the same as ResultMessage"; slide 5: "Save one
   canonical final artifact".)*
6. In `lesson5.py`, move the `print(...)` of the complete answer into an `AssistantMessage` branch, and run it.
   What happens to the output? Undo the change afterwards. *(Slide 5's "Avoid double printing".)*

### Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: No module named 'claude_agent_sdk'` | Activate `.venv` and install the SDK **in that same environment**. |
| `ImportError: cannot import name 'StreamEvent'` or `'ResultError'` | Your SDK is too old. Run `python -m pip install -U claude-agent-sdk`. |
| Authentication error | Set `ANTHROPIC_API_KEY` in the **same terminal** you run the script from, log in to Claude Code, or use `--demo`. |
| `✘ Claude Code CLI not found` | Reinstall the SDK (it bundles the CLI) or install Claude Code. |
| The whole answer appears at once | Either `include_partial_messages` is off, or the answer is short (see point 5 above). Try a longer prompt. |
| The answer appears twice | You print both the deltas **and** the complete `AssistantMessage` or `ResultMessage.result`. Print one source only. |
| `Activate.ps1 cannot be loaded because running scripts is disabled` | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once. |
| Strange characters like `←[94m` instead of colors | Your console doesn't support colors. Run `$env:NO_COLOR = "1"` first. |

---

## 4. How the application was built

### 4.1 `lesson5.py`: the complete lesson script

```python
options = ClaudeAgentOptions(
    include_partial_messages=True,   # the switch: add StreamEvent objects to the stream
    tools=[],                        # this lesson needs no tools: a plain streamed answer
    setting_sources=[],              # ignore local Claude settings for a reproducible run
)

async for message in query(prompt="Explain AI agents in three sentences.", options=options):
    if isinstance(message, StreamEvent):
        event = message.event
        if event.get("type") == "content_block_delta":
            delta = event.get("delta", {})
            if delta.get("type") == "text_delta":
                print(delta.get("text", ""), end="", flush=True)
    elif isinstance(message, ResultMessage):
        print()
        print("\n--- Execution complete ---")
        print("[OUTCOME]", message.subtype)
        ...
```

Line by line:

- **`include_partial_messages=True`** adds `StreamEvent` objects. Nothing else changes: `query()` and the
  `async for` loop are the same as in earlier lessons.
- **`isinstance(message, StreamEvent)`**: the stream now mixes two kinds of objects, so classify each one first.
- **Filter twice.** `event["type"] == "content_block_delta"` selects the delta events. `delta["type"] ==
  "text_delta"` selects the *answer text* among them, and excludes tool-input and thinking deltas.
- **`.get()` with defaults** (`event.get("delta", {})`, `delta.get("text", "")`): `message.event` is a raw
  dictionary, so a missing key returns a default instead of raising `KeyError`.
- **`end=""`** prints the fragment without adding a newline, and **`flush=True`** pushes it to the terminal
  right away instead of waiting in Python's output buffer.
- **`ResultMessage`** is handled separately. It prints the outcome, not the answer. The `AssistantMessage` that
  also arrives is deliberately **not** handled, because its text has already been streamed.
- The cost line prints `unavailable` when `total_cost_usd` is `None`, instead of inventing a value.

### 4.2 `stream_monitor.py`: from a loop to a monitor

#### a) Configuration

```python
options = ClaudeAgentOptions(
    include_partial_messages=partial,   # the switch this lesson is about
    tools=tools,                        # the only tools the agent can see
    allowed_tools=tools,                # ...and may run without asking
    cwd=str(cwd),
    max_turns=args.max_turns,
    setting_sources=[],                 # ignore local Claude settings for a reproducible run
    strict_mcp_config=True,             # don't load MCP servers configured elsewhere
)
```

`partial` is `False` only with `--no-partial`, so you can compare the same run with and without the switch. The
`Glob` tool is included on purpose. A tool request gives the stream an `input_json_delta` block, which is what
slide 4 is about.

#### b) State: one model message and its block slots

```python
@dataclass
class Block:
    index: int
    type: str                    # text | tool_use | thinking | ...
    name: str = ""
    tool_id: str = ""
    fragments: list[str] = field(default_factory=list)
    deltas: int = 0
    closed: bool = False

    @property
    def joined(self) -> str:
        return "".join(self.fragments)   # join exactly: never strip or add spaces

@dataclass
class ModelMessage:
    number: int
    msg_id: str
    blocks: dict[int, Block] = field(default_factory=dict)   # keyed by event["index"]
    ...
```

A `ModelMessage` represents one `message_start … message_stop` cycle. Its blocks are stored in a dictionary
keyed by `event["index"]`: slide 4's *"Use message identity and content-block index to keep fragments from
different blocks apart"*. `"".join(...)` reassembles fragments **exactly**, as slide 3 requires.

#### c) Routing, and the lifecycle at `message_start`

```python
def on_message(self, message) -> None:
    self.n += 1
    if isinstance(message, StreamEvent):
        self.on_stream_event(message)
    elif isinstance(message, AssistantMessage):
        self.on_assistant(message)
    elif isinstance(message, UserMessage):
        ...                                   # ToolResultBlock: the SDK ran the tool
    elif isinstance(message, ResultMessage):
        self.result = message
```

```python
def on_stream_event(self, se: StreamEvent) -> None:
    if se.parent_tool_use_id:                 # events from a subagent's own messages
        self.subagent_events += 1
        return
    event = se.event
    kind = event.get("type", "?")

    if kind == "message_start":
        msg = event.get("message", {})
        # Reset per-message block tracking: indices restart at 0 in every message.
        self.current = ModelMessage(len(self.messages) + 1, msg.get("id", ""))
        self.messages.append(self.current)
```

This is slide 1's *"classify each incoming SDK object"*. Each `message_start` creates a **new** `ModelMessage`.
That's slide 2's *"reset per-message block tracking at a new message_start"*. Without the reset, index `0` of the
second message would be appended to index `0` of the first.

Events with a `parent_tool_use_id` belong to a subagent's messages. They are counted, not mixed into the answer.

#### d) `content_block_delta`: filter twice, then route by delta type

```python
elif kind == "content_block_delta":
    index = event.get("index", 0)
    delta = event.get("delta", {})
    dtype = delta.get("type", "?")
    block = cur.blocks.get(index)
    ...
    if dtype == "text_delta":                    # 1) answer text: show it now
        text = delta.get("text", "")
        block.fragments.append(text)
        self.live(text)
    elif dtype == "input_json_delta":            # 2) tool arguments: buffer, never print as text
        block.fragments.append(delta.get("partial_json", ""))
        print(color("·", "95"), end="", flush=True)
    else:                                        # 3) thinking_delta, signature_delta, ...
        ...                                      #    counted, not answer text
```

The nested `delta["type"]` decides what to do: slide 4's key takeaway. Only `text_delta` reaches the screen.
`input_json_delta` is stored in its block's buffer, with one progress dot per fragment, so the user sees that
something is happening without seeing raw JSON. Every other delta type falls into the `else` branch. That's
*"keep room for other events"*: a new delta type in a future SDK can't crash the program or leak into the answer.

`live()` prints a fragment as-is:

```python
def live(self, fragment: str) -> None:
    self.mark_text()                             # timing for the report
    if self.at_line_start:
        print("   " + color("│ ", "94"), end="")
    print(fragment.replace("\n", "\n   " + color("│ ", "94")), end="", flush=True)
    self.at_line_start = fragment.endswith("\n")
```

It changes only the **display** (an indented answer bar after each newline). The stored fragment stays untouched.
`at_line_start` lets status lines (tool progress, results) start on a fresh line, without breaking the answer
text in the middle of a word.

#### e) `content_block_stop`: parse the buffered tool input only now

```python
def finish_tool_input(self, block: Block) -> str:
    raw = block.joined
    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as exc:
        error = str(exc)
    ...
```

The tool input is assembled from **all** its fragments, then parsed once. Parsing any single fragment would fail:
`'{"pattern": "**/*'` isn't valid JSON. An empty buffer means a tool with no arguments (`{}`). If the joined input
is still invalid, the monitor reports it instead of crashing.

The monitor **observes** the tool request. It never executes it: the SDK runs `Glob` and returns a
`ToolResultBlock`. Slide 4: *"A tool-input delta is not an instruction to call the tool again"*.

#### f) `message_delta` and `message_stop`: the end of a message, not of the run

```python
elif kind == "message_delta":
    cur.stop_reason = event.get("delta", {}).get("stop_reason")
elif kind == "message_stop":
    cur.stopped = True
    if cur.stop_reason == "tool_use":
        self.line(dim("   message_stop: this model message ended, but the run continues with a tool"))
```

`message_delta` carries the `stop_reason`. When it is `tool_use`, the run continues: the SDK executes the tool,
and a new `message_start` follows. That's why slide 2 says *"message_stop is not the same as ResultMessage"*. Only
the `ResultMessage` ends the run.

#### g) Complete messages: match them to the stream instead of printing them again

```python
def on_assistant(self, message: AssistantMessage) -> None:
    texts = [b.text for b in message.content if isinstance(b, TextBlock)]
    streamed = None
    if message.message_id:
        streamed = next((m for m in self.messages if m.msg_id == message.message_id), None)
    elif self.partial and self.current is not None:
        streamed = self.current
    if streamed is not None:
        streamed.complete_text = (streamed.complete_text or "") + "".join(texts)
        return                                   # already shown: do not print again
    # Not streamed (for example --no-partial): the complete message is the only source of text.
    for block in message.content:
        if isinstance(block, TextBlock):
            self.live(block.text)
```

`AssistantMessage.message_id` equals the `id` from `message_start`, so the monitor knows exactly which streamed
message a complete message belongs to. If it was streamed, its text is only **stored** for the duplicate check. If
it wasn't (`--no-partial`), the complete message is the only source of text, so it is printed. Either way, each
answer appears **exactly once**.

Texts are **accumulated** (`+=`), because the CLI can split one model message into several `AssistantMessage`
objects (one per block, as the live run shows).

#### h) The run report: duplicates, outcome and the final artifact

```python
for x in m.messages:
    same = x.streamed_text == x.complete_text
    ...   # ✔ identical: the complete message was not printed again
if r is not None and r.result:
    if r.result.strip() == final_streamed.strip() and final_streamed:
        print("✔ ResultMessage.result repeats the answer already shown: not printed again")
```

The report **proves** the no-duplicates rule. The streamed text equals the complete message's text, and
`ResultMessage.result` repeats the final answer. Both are reported but **not printed again**: slide 5's *"Avoid
double printing"*.

The outcome comes **only** from `ResultMessage` (`subtype`, `is_error`, `num_turns`, `total_cost_usd`,
`terminal_reason`). If text was streamed but the run failed, the verdict says so: *partial text is presentation,
not proof of success*.

```python
if save is not None:
    answer = (r.result if r is not None and r.result else final_streamed)
    if succeeded and answer:
        save.write_text(answer + "\n", encoding="utf-8")
```

`--save` writes **one canonical final artifact**, and only when the run succeeded. It is kept separate from the
live display, as slide 5 asks.

#### i) The application boundary: catch SDK exceptions

```python
try:
    async for message in stream:
        monitor.on_message(message)
except ResultError as exc:                  # must come before ProcessError (its parent class)
    monitor.exception = exc
except CLINotFoundError as exc:
    ...
except ProcessError as exc:
    ...
except Exception as exc:                    # missing API key, network error, ...
    ...
print_report(monitor, Path(args.save) if args.save else None)
```

When a run fails, the SDK first **yields** a `ResultMessage` with `is_error=True`, then **raises** `ResultError`.
Catching it means the report still prints, including any partial text that had already streamed. Slide 5: *"A run
can fail after showing some output. Check the outcome and handle SDK exceptions."*

#### j) Offline demo streams built from real SDK classes

```python
def se(event: dict) -> StreamEvent:
    return StreamEvent(uuid=str(uuid.uuid4()), session_id=SESSION, event=event)

async def text_message(msg_id, fragments, pause, index=0, stop="end_turn", before=None):
    yield se({"type": "message_start", "message": {"id": msg_id, "role": "assistant", "content": []}})
    yield se({"type": "content_block_start", "index": index, "content_block": {"type": "text", "text": ""}})
    for f in fragments:
        await asyncio.sleep(pause)
        yield se({"type": "content_block_delta", "index": index, "delta": {"type": "text_delta", "text": f}})
    # Live runs deliver the complete AssistantMessage right after the last delta, before content_block_stop.
    yield AssistantMessage(content=[TextBlock(text="".join(fragments))], model=MODEL, message_id=msg_id)
    yield se({"type": "content_block_stop", "index": index})
    yield se({"type": "message_delta", "delta": {"stop_reason": stop}})
    yield se({"type": "message_stop"})
```

The demos are async generators that yield **real `StreamEvent` and message objects** in the **same order as a
live run**, including the detail that the complete `AssistantMessage` arrives before `content_block_stop`. The
monitor can't tell them apart from `query()`. `--no-partial` in demo mode wraps the stream in `without_partials()`,
which drops every `StreamEvent`: exactly what the SDK does when the switch is off. This is a good pattern for
**unit-testing your own stream handlers** without network access or cost.

---

## Key takeaway

> **Partial text is presentation; the final result reports the outcome.**
> Enable `include_partial_messages`, classify every incoming object, print only `text_delta` fragments, exactly
> as they arrive, buffer tool-input fragments until their block is complete, and report the run from its
> `ResultMessage`, without printing the answer a second time.
