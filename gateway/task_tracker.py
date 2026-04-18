from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from dataclasses import dataclass
from typing import Awaitable, Callable


logger = logging.getLogger(__name__)


def default_progress_bar(pct: float, width: int = 20) -> str:
    try:
        pct = float(pct)
    except (TypeError, ValueError):
        pct = 0.0
    pct = max(0.0, min(100.0, pct))
    filled = int(width * pct / 100.0)
    return f"[{'█' * filled}{'░' * (width - filled)}] {pct:.1f}%"


@dataclass
class TrackerPollResult:
    content: str
    done: bool = False
    min_edit_interval: float | None = None


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
