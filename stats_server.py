"""Tiny HTTP + WebSocket server for live walker stats.

Serves a minimal HTML page at `/` and a push-only WebSocket endpoint at `/ws`.
Designed to run in a background thread so the crawler can stay synchronous.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any

from aiohttp import web


def _load_stats_html() -> str:
    path = Path(__file__).with_name("stats.html")
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        # If the file is missing (e.g. mis-packaged container), fall back to a
        # tiny message rather than hard-crashing the crawler.
        return "<html><body><p>Missing stats.html</p></body></html>"


STATS_HTML = _load_stats_html()


class StatsWebServer:
    def __init__(self, *, port: int) -> None:
        self._port = int(port)
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: web.AppRunner | None = None
        self._ws_clients: set[web.WebSocketResponse] = set()
        self._lock = threading.Lock()
        self._last_rows: list[list[str]] = [["Status", "Starting…"]]
        self._status: str = "starting"

    @property
    def port(self) -> int:
        return self._port

    def __enter__(self) -> "StatsWebServer":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.stop()

    def start(self) -> None:
        if self._thread is not None:
            return

        ready = threading.Event()
        exc_holder: list[BaseException] = []

        def _run() -> None:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                loop.run_until_complete(self._start_async())
                ready.set()
                loop.run_forever()
            except BaseException as exc:  # noqa: BLE001
                exc_holder.append(exc)
                ready.set()
            finally:
                try:
                    if self._loop is not None:
                        pending = asyncio.all_tasks(loop=self._loop)
                        for task in pending:
                            task.cancel()
                        if pending:
                            self._loop.run_until_complete(
                                asyncio.gather(*pending, return_exceptions=True)
                            )
                        self._loop.close()
                except BaseException:
                    pass

        self._thread = threading.Thread(target=_run, name="stats-web", daemon=True)
        self._thread.start()
        ready.wait(timeout=10)

        if exc_holder:
            raise SystemExit(f"Stats webserver failed to start: {exc_holder[0]}")
        if not ready.is_set() or self._loop is None:
            raise SystemExit("Stats webserver failed to start (timeout)")

        # Publish initial payload so new WS clients see something immediately.
        self._publish_locked(self._last_rows)

    async def _start_async(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/ws", self._handle_ws)

        runner = web.AppRunner(app)
        await runner.setup()
        self._runner = runner

        site = web.TCPSite(runner, host="0.0.0.0", port=self._port)
        await site.start()

    async def _handle_index(self, request: web.Request) -> web.Response:
        return web.Response(text=STATS_HTML, content_type="text/html")

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)

        with self._lock:
            self._ws_clients.add(ws)
            payload = self._make_payload(self._last_rows)

        await ws.send_str(json.dumps(payload, ensure_ascii=True))

        try:
            async for _ in ws:
                pass
        finally:
            with self._lock:
                self._ws_clients.discard(ws)
        return ws

    def set_status(self, status: str) -> None:
        """Update only the status field shown in the UI."""

        with self._lock:
            self._status = str(status)
            self._publish_locked(self._last_rows)

    def publish(self, rows: list[list[str]]) -> None:
        """Publish new table rows. `updated_at` and `status` are filled in automatically."""

        with self._lock:
            self._last_rows = rows
            self._publish_locked(rows)

    def _publish_locked(self, rows: list[list[str]]) -> None:
        payload = self._make_payload(rows)
        loop = self._loop
        if loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._broadcast(payload), loop)

    def _make_payload(self, rows: list[list[str]]) -> dict[str, Any]:
        return {
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": self._status,
            "rows": rows,
        }

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        message = json.dumps(payload, ensure_ascii=True)
        with self._lock:
            clients = list(self._ws_clients)

        if not clients:
            return

        dead: list[web.WebSocketResponse] = []
        for ws in clients:
            try:
                await ws.send_str(message)
            except BaseException:
                dead.append(ws)

        if dead:
            with self._lock:
                for ws in dead:
                    self._ws_clients.discard(ws)

    def stop(self) -> None:
        loop = self._loop
        thread = getattr(self, "_thread", None)
        if loop is None or thread is None:
            return
        future = asyncio.run_coroutine_threadsafe(self._stop_async(), loop)
        try:
            # Wait for graceful async shutdown (closing websockets, cleanup runner).
            future.result(timeout=5.0)
        except BaseException:
            # Best-effort shutdown; ignore cleanup errors/timeouts.
            pass
        loop.call_soon_threadsafe(loop.stop)
        try:
            thread.join(timeout=5.0)
        except BaseException:
            pass
        with self._lock:
            self._loop = None
            self._runner = None
            # Only reset _thread if it exists.
            if hasattr(self, "_thread"):
                self._thread = None

    async def _stop_async(self) -> None:
        with self._lock:
            clients = list(self._ws_clients)
            self._ws_clients.clear()

        for ws in clients:
            try:
                await ws.close()
            except BaseException:
                pass

        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except BaseException:
                pass
