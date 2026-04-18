from __future__ import annotations

import asyncio
import enum
import inspect
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional


logger = logging.getLogger(__name__)


def default_progress_bar(pct: float, width: int = 20) -> str:
    try:
        pct = float(pct)
    except (TypeError, ValueError):
        pct = 0.0
    pct = max(0.0, min(100.0, pct))
    filled = int(width * pct / 100.0)
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct:.1f}%"


class TaskState(enum.Enum):
    pending = "pending"
    running = "running"
    blocked = "blocked"
    complete = "complete"
    failed = "failed"
    cancelled = "cancelled"


class EventType(enum.Enum):
    status_change = "status_change"
    progress_update = "progress_update"
    checklist_update = "checklist_update"
    report = "report"
    child_task_start = "child_task_start"
    child_task_complete = "child_task_complete"
    comment = "comment"
    eta_update = "eta_update"


@dataclass
class ChecklistItem:
    id: str
    label: str
    done: bool = False
    state: TaskState = TaskState.pending

    @property
    def icon(self) -> str:
        if self.done or self.state == TaskState.complete:
            return "✅"
        if self.state == TaskState.running:
            return "🔄"
        if self.state == TaskState.failed:
            return "❌"
        if self.state == TaskState.blocked:
            return "⏸️"
        if self.state == TaskState.cancelled:
            return "⛔"
        return "⬜"

    def mark_done(self):
        self.done = True
        self.state = TaskState.complete


@dataclass
class TaskEvent:
    type: EventType
    timestamp: float = field(default_factory=time.time)
    state: Optional[TaskState] = None
    progress_pct: Optional[float] = None
    message: Optional[str] = None
    checklist: Optional[list[ChecklistItem]] = None
    eta: Optional[str] = None
    child_task_id: Optional[str] = None
    reply_to_event: Optional[str] = None


@dataclass
class TrackerPollResult:
    content: str
    done: bool = False
    min_edit_interval: float | None = None


def _render_checklist(items: list[ChecklistItem]) -> str:
    if not items:
        return ""
    lines = []
    for item in items:
        lines.append(f"{item.icon} {item.label}")
    return "\n".join(lines)


def _format_tracker_content(
    title: str,
    state: TaskState = TaskState.pending,
    progress_pct: Optional[float] = None,
    checklist: Optional[list[ChecklistItem]] = None,
    message: Optional[str] = None,
    eta: Optional[str] = None,
) -> str:
    parts = []
    state_icon = {
        TaskState.pending: "⏳",
        TaskState.running: "🔄",
        TaskState.blocked: "⏸️",
        TaskState.complete: "✅",
        TaskState.failed: "❌",
        TaskState.cancelled: "⛔",
    }.get(state, "❓")
    parts.append(f"**{state_icon} {title}**")
    parts.append(f"State: {state.value}")
    if progress_pct is not None:
        parts.append(default_progress_bar(progress_pct))
    if eta:
        parts.append(f"ETA: {eta}")
    checklist_text = _render_checklist(checklist or [])
    if checklist_text:
        parts.append("")
        parts.append(checklist_text)
    if message:
        parts.append("")
        parts.append(message)
    return "\n".join(parts)


class TaskEventBuffer:
    """Thread-safe event history for a tracked task.

    Maintains a list of TaskEvents and computes the current aggregate state
    (progress, checklist, etc.) from the event stream.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._events: list[TaskEvent] = []
        self._state = TaskState.pending
        self._progress: Optional[float] = None
        self._checklist: list[ChecklistItem] = []
        self._eta: Optional[str] = None
        self._message: Optional[str] = None
        self._title: str = "Task"

    def set_title(self, title: str):
        with self._lock:
            self._title = title

    def push(self, event: TaskEvent):
        with self._lock:
            self._events.append(event)
            if event.state is not None:
                self._state = event.state
            if event.progress_pct is not None:
                self._progress = event.progress_pct
            if event.checklist is not None:
                existing = {item.id: item for item in self._checklist}
                new_items = []
                for item in event.checklist:
                    old = existing.get(item.id)
                    if old and old.done:
                        item.done = True
                        item.state = TaskState.complete
                    new_items.append(item)
                self._checklist = new_items
            if event.eta is not None:
                self._eta = event.eta
            if event.message is not None:
                self._message = event.message

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "title": self._title,
                "state": self._state,
                "progress": self._progress,
                "checklist": list(self._checklist),
                "eta": self._eta,
                "message": self._message,
                "event_count": len(self._events),
            }

    def render(self) -> str:
        with self._lock:
            return _format_tracker_content(
                title=self._title,
                state=self._state,
                progress_pct=self._progress,
                checklist=list(self._checklist),
                message=self._message,
                eta=self._eta,
            )


class GatewayTaskTracker:
    """Gateway-owned tracked message manager.

    A tracker is identified by an arbitrary key (string/tuple/etc.) and owns one
    editable platform message. Callers provide the full message content for each
    update; this class does not impose formatting.
    """

    def __init__(self, owner, loop, adapter, chat_id, metadata=None):
        self.owner = owner
        self.loop = loop
        self.adapter = adapter
        self.chat_id = chat_id
        self.metadata = metadata
        self._lock = threading.Lock()
        self._messages = getattr(owner, "_task_tracker_messages", None)
        if self._messages is None:
            self._messages = {}
            owner._task_tracker_messages = self._messages
        self._pollers = getattr(owner, "_task_tracker_pollers", None)
        if self._pollers is None:
            self._pollers = {}
            owner._task_tracker_pollers = self._pollers
        self._buffers: dict = getattr(owner, "_task_tracker_buffers", None)
        if self._buffers is None:
            self._buffers = {}
            owner._task_tracker_buffers = self._buffers

    async def _send_or_edit(self, key, content: str, min_edit_interval: float = 5.0):
        if not self.adapter or not self.chat_id:
            return
        content = str(content or "").strip()
        if not content:
            return
        current = self._messages.get(key, {})
        if current.get("last_content") == content:
            return
        now = time.monotonic()
        msg_id = current.get("message_id")
        last_edit_ts = float(current.get("last_edit_ts", 0.0))
        if (
            msg_id
            and min_edit_interval > 0
            and (now - last_edit_ts) < min_edit_interval
        ):
            return

        if msg_id:
            result = await self.adapter.edit_message(
                chat_id=self.chat_id,
                message_id=msg_id,
                content=content,
            )
            if result.success:
                self._messages[key] = {
                    "message_id": msg_id,
                    "last_edit_ts": time.monotonic(),
                    "last_content": content,
                }
                return

        result = await self.adapter.send(
            self.chat_id,
            content,
            metadata=self.metadata,
        )
        if result.success and result.message_id:
            self._messages[key] = {
                "message_id": result.message_id,
                "last_edit_ts": time.monotonic(),
                "last_content": content,
            }

    def update_threadsafe(self, key, content: str, min_edit_interval: float = 5.0):
        asyncio.run_coroutine_threadsafe(
            self._send_or_edit(key, content, min_edit_interval=min_edit_interval),
            self.loop,
        )

    async def _run_poller(self, key, poll_fn, interval: float, initial_delay: float):
        try:
            await asyncio.sleep(max(0.0, initial_delay))
            while True:
                result = poll_fn()
                if inspect.isawaitable(result):
                    result = await result
                if result is None:
                    await asyncio.sleep(interval)
                    continue
                if isinstance(result, str):
                    result = TrackerPollResult(content=result)
                if result.content:
                    await self._send_or_edit(
                        key,
                        result.content,
                        min_edit_interval=result.min_edit_interval or 0.0,
                    )
                if result.done:
                    break
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("task tracker poller error for %r: %s", key, exc)
        finally:
            self._pollers.pop(key, None)

    def ensure_poller_threadsafe(
        self,
        key,
        poll_fn: Callable[
            [],
            TrackerPollResult | str | Awaitable[TrackerPollResult | str | None] | None,
        ],
        interval: float = 5.0,
        initial_delay: float = 5.0,
    ):
        with self._lock:
            existing = self._pollers.get(key)
            if existing and not existing.done():
                return
            task = asyncio.run_coroutine_threadsafe(
                self._run_poller(
                    key, poll_fn, interval=interval, initial_delay=initial_delay
                ),
                self.loop,
            )
            self._pollers[key] = task

    def _get_buffer(self, key) -> TaskEventBuffer:
        with self._lock:
            if key not in self._buffers:
                self._buffers[key] = TaskEventBuffer()
            return self._buffers[key]

    def push_event_threadsafe(self, key, event: TaskEvent):
        buf = self._get_buffer(key)
        buf.push(event)
        content = buf.render()
        self.update_threadsafe(key, content)

    def set_task_title_threadsafe(self, key, title: str):
        buf = self._get_buffer(key)
        buf.set_title(title)

    def push_status_threadsafe(self, key, state: TaskState, message: Optional[str] = None):
        self.push_event_threadsafe(
            key,
            TaskEvent(
                type=EventType.status_change,
                state=state,
                message=message,
            ),
        )

    def push_progress_threadsafe(self, key, pct: float, message: Optional[str] = None):
        self.push_event_threadsafe(
            key,
            TaskEvent(
                type=EventType.progress_update,
                progress_pct=pct,
                message=message,
            ),
        )

    def push_checklist_threadsafe(self, key, items: list[ChecklistItem]):
        self.push_event_threadsafe(
            key,
            TaskEvent(
                type=EventType.checklist_update,
                checklist=items,
            ),
        )

    def push_eta_threadsafe(self, key, eta: str):
        self.push_event_threadsafe(
            key,
            TaskEvent(
                type=EventType.eta_update,
                eta=eta,
            ),
        )

    def push_report_threadsafe(self, key, message: str):
        self.push_event_threadsafe(
            key,
            TaskEvent(
                type=EventType.report,
                message=message,
            ),
        )
