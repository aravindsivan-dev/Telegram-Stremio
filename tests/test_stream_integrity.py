import os
import time
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.middleware.sessions import SessionMiddleware

with patch.dict(os.environ, {
    "DATABASE": "mongodb://127.0.0.1:1/tracking,mongodb://127.0.0.1:1/storage",
    "API_ID": "1234",
    "API_HASH": "00000000000000000000000000000000",
    "BOT_TOKEN": "1234:test-owner",
}):
    from Backend import db
    from Backend.fastapi.main import auth_exception_handler
    from Backend.fastapi.routes import stream_routes
    from Backend.helper import custom_dl, virtual_dl


class StreamIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def test_split_stops_after_partial_download(self):
        first = SimpleNamespace(file_size=8)
        second = SimpleNamespace(file_size=4)
        parts = [
            {"index": 0, "chat_id": 123, "msg_id": 1, "file_id": first, "size": 8, "cum_start": 0},
            {"index": 1, "chat_id": 123, "msg_id": 2, "file_id": second, "size": 4, "cum_start": 8},
        ]

        async def incomplete_body():
            yield b"abcd"

        streamer = SimpleNamespace(prefetch_stream=AsyncMock(return_value=incomplete_body()))
        stream = virtual_dl.virtual_stream_generator(
            parts=parts, start=0, end=11, chunk_size=4, streamer=streamer,
            client_index=0, request=None, meta={}, stream_id="test-split",
            parallelism=1, prefetch_count=1,
        )
        self.assertEqual([chunk async for chunk in stream], [b"abcd"])
        streamer.prefetch_stream.assert_awaited_once()

    async def test_reference_refresh_rejects_a_replacement_file(self):
        reader = object.__new__(custom_dl.ByteStreamer)
        reader.client = object()
        reader._file_id_cache = {}
        original = SimpleNamespace(dc_id=1, media_id=1, file_size=8)
        replacement = SimpleNamespace(dc_id=1, media_id=2, file_size=8)
        location = SimpleNamespace(id=1)
        session = SimpleNamespace(send=AsyncMock(side_effect=[
            SimpleNamespace(bytes=b"abcd"), RuntimeError("FILE_REFERENCE_EXPIRED"),
        ]))
        reader._get_media_session = AsyncMock(return_value=session)
        reader._get_location = AsyncMock(return_value=location)

        with (
            patch.object(custom_dl, "get_file_ids", new=AsyncMock(return_value=replacement)),
            patch.object(custom_dl, "ACTIVE_STREAMS", {}),
            patch.object(custom_dl, "work_loads", {0: 0}),
            patch.object(custom_dl, "client_avg_mbps", {0: 0}),
            patch.object(db, "log_stream_stats", new_callable=AsyncMock),
        ):
            stream = await reader.prefetch_stream(
                original, client_index=0, offset=0, first_part_cut=0, last_part_cut=4,
                part_count=2, chunk_size=4, prefetch=1, parallelism=1, stream_id="test-refresh",
                chat_id=123, message_id=1,
            )
            self.assertEqual([chunk async for chunk in stream], [b"abcd"])
            self.assertEqual(custom_dl.ACTIVE_STREAMS["test-refresh"]["status"], "error")
            self.assertEqual(custom_dl.work_loads[0], 0)
        self.assertIs(reader._file_id_cache[123, 1], replacement)
        self.assertEqual(session.send.await_count, 2)


class StreamDashboardTests(unittest.TestCase):
    def test_stats_require_dashboard_session_and_filter_details(self):
        app = FastAPI()
        app.add_middleware(SessionMiddleware, secret_key="only-for-local-tests")
        app.add_exception_handler(401, auth_exception_handler)
        app.include_router(stream_routes.router)

        @app.post("/test-admin-session")
        async def sign_in(request: Request):
            request.session.update({"authenticated": True, "sid": "test-admin"})

        stream = {
            "stream_id": "example", "start_ts": time.time() - 20, "status": "active",
            "total_bytes": 100, "last_bytes": 100, "last_activity_ts": time.time() - 20,
            "meta": {"title": "Example", "token": "test-token", "client_host": "192.0.2.10"},
        }
        with (
            patch.object(stream_routes, "ACTIVE_STREAMS", {"example": stream}),
            patch.object(stream_routes, "RECENT_STREAMS", deque()),
            TestClient(app, follow_redirects=False) as client,
        ):
            for path in ("/stream/stats", "/stream/stats/example"):
                self.assertEqual(client.get(path).status_code, 401)

            client.post("/test-admin-session")
            summary = client.get("/stream/stats")
            self.assertEqual(summary.status_code, 200)
            self.assertEqual(summary.json()["active_streams"][0]["status"], "active")
            self.assertEqual(stream["status"], "active")
            detail = client.get("/stream/stats/example")
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.headers["cache-control"], "no-store")
            self.assertEqual(detail.json()["meta"], {"title": "Example"})
