import asyncio
import io
import os
import time
import unittest
import zipfile
from collections import deque
from datetime import datetime, timezone
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
    from Backend.helper import cf_live, custom_dl, virtual_dl
    from Backend.helper.stream_container import split_video_content_type


class SplitVideoTypeTests(unittest.IsolatedAsyncioTestCase):
    def test_ebml_doc_type_must_be_a_complete_header_element(self):
        matroska = bytes.fromhex("1a45dfa38b428288") + b"matroska"
        webm = bytes.fromhex("1a45dfa34007428284") + b"webm"
        self.assertEqual(split_video_content_type(matroska), "video/x-matroska")
        self.assertEqual(split_video_content_type(webm), "video/webm")
        self.assertIsNone(split_video_content_type(matroska[:-1]))
        self.assertIsNone(split_video_content_type(b"PK\x03\x04" + matroska))
        self.assertIsNone(split_video_content_type(bytes.fromhex("1a45dfa38dec8b") + matroska[5:]))

    async def test_split_head_and_seek_return_matroska_with_only_one_probe(self):
        header = bytes.fromhex("1a45dfa38b428288") + b"matroska"
        first = header + b"A" * (8192 - len(header))
        second = b"B" * 8192
        data = {1: first, 2: second}
        calls = []

        async def send(query):
            calls.append((query.location.id, query.offset, query.limit))
            return SimpleNamespace(bytes=data[query.location.id][query.offset:query.offset + query.limit])

        reader = object.__new__(custom_dl.ByteStreamer)
        reader._pending_page_reads = {}
        reader._get_media_session = AsyncMock(return_value=SimpleNamespace(send=send))
        reader._get_location = AsyncMock(side_effect=lambda file_id: SimpleNamespace(id=file_id.media_id))
        parts = [
            {"index": i - 1, "chat_id": 123, "msg_id": i, "size": len(item), "cum_start": (i - 1) * 8192,
             "file_id": SimpleNamespace(dc_id=1, media_id=i, file_size=len(item), file_name="clip.zip.001",
                                        mime_type="application/zip")}
            for i, item in data.items()
        ]
        with (
            patch.object(stream_routes, "_db_client", new=AsyncMock(return_value=(0, reader, (parts, 16384)))),
            patch.object(stream_routes, "_prefetch_for", return_value=(1, 1)),
            patch.object(stream_routes, "track_usage", new_callable=AsyncMock),
            patch.object(custom_dl, "_SPLIT_TYPES", {}),
            patch.object(custom_dl, "_RECENT_CHUNKS", {}),
            patch.object(custom_dl, "ACTIVE_STREAMS", {}),
            patch.object(custom_dl, "work_loads", {0: 0}),
            patch.object(custom_dl, "client_avg_mbps", {0: 0}),
            patch.object(db, "log_stream_stats", new_callable=AsyncMock),
        ):
            head = await stream_routes.virtual_media_streamer(Request({"type": "http", "method": "HEAD", "headers": []}), [], "viewer")
            self.assertEqual(head.headers["content-type"], "video/x-matroska")
            self.assertEqual(head.headers["content-length"], "16384")
            self.assertEqual(head.body, b"")
            self.assertEqual(calls, [(1, 0, 4096)])

            request = Request({"type": "http", "method": "GET", "headers": [(b"range", b"bytes=8190-8193")]})
            response = await stream_routes.virtual_media_streamer(request, [], "viewer")
            self.assertEqual(response.status_code, 206)
            self.assertEqual(response.headers["content-type"], "video/x-matroska")
            self.assertEqual(response.headers["content-range"], "bytes 8190-8193/16384")
            self.assertEqual(b"".join([chunk async for chunk in response.body_iterator]), b"AABB")
            await asyncio.sleep(0)
        self.assertEqual(calls, [(1, 0, 4096), (1, 0, 1048576), (2, 0, 1048576)])

    async def test_failed_probe_is_retryable_and_genuine_zip_keeps_its_mime(self):
        send = AsyncMock(side_effect=[
            TimeoutError(),
            SimpleNamespace(bytes=b"PK\x03\x04" + b"\0" * 4092),
        ])
        reader = object.__new__(custom_dl.ByteStreamer)
        reader._pending_page_reads = {}
        reader._get_media_session = AsyncMock(return_value=SimpleNamespace(send=send))
        reader._get_location = AsyncMock(return_value=SimpleNamespace(id=1))
        file_id = SimpleNamespace(dc_id=1, media_id=1, file_size=8192, file_name="archive.zip.001",
                                  mime_type="application/zip")
        with patch.object(custom_dl, "_SPLIT_TYPES", {}):
            for _ in range(3):
                self.assertEqual(await stream_routes._resolve_split_filename_mime(reader, file_id),
                                 ("archive.zip.001", "application/zip"))
            self.assertEqual(send.await_count, 2)


class StreamIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def test_split_zip_reuses_header_chunk_and_logs_only_playback(self):
        clip = b"A" * 80 + b"B" * 80
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as writer:
            writer.writestr("clip.mkv", clip)
        data = {1: archive.getvalue()[:100], 2: archive.getvalue()[100:]}
        file_ids = {
            i: SimpleNamespace(dc_id=1, media_id=i, file_size=len(part))
            for i, part in data.items()
        }
        reader = object.__new__(custom_dl.ByteStreamer)
        reader._pending_page_reads = {}
        reader.get_file_properties = AsyncMock(side_effect=lambda chat_id, message_id: file_ids[message_id])
        calls = []

        async def send(query):
            calls.append(query.location.id)
            return SimpleNamespace(bytes=data[query.location.id][query.offset:query.offset + query.limit])

        reader._get_media_session = AsyncMock(return_value=SimpleNamespace(send=send))
        reader._get_location = AsyncMock(side_effect=lambda file_id: SimpleNamespace(id=file_id.media_id))

        async def db_client(fetch):
            return 0, reader, await fetch(reader)

        def request(method, byte_range):
            return Request({"type": "http", "method": method, "headers": [(b"range", byte_range)]})

        parts = [{"chat_id": 123, "msg_id": i} for i in data]
        active, loads = {}, {0: 0}
        with (
            patch.object(stream_routes, "_db_client", side_effect=db_client),
            patch.object(stream_routes, "_prefetch_for", return_value=(1, 1)),
            patch.object(stream_routes, "_zip_entry_cache", {}),
            patch.object(stream_routes, "track_usage", new_callable=AsyncMock) as track_usage,
            patch.object(custom_dl, "ACTIVE_STREAMS", active),
            patch.object(custom_dl, "_RECENT_CHUNKS", {}),
            patch.object(custom_dl, "work_loads", loads),
            patch.object(custom_dl, "client_avg_mbps", {}),
            patch.object(db, "log_stream_stats", new_callable=AsyncMock) as history,
        ):
            head = await stream_routes.db_zip_media_streamer(request("HEAD", b"bytes=0-31"), parts, "viewer")
            self.assertEqual(head.status_code, 206)
            self.assertEqual(head.headers["Content-Range"], "bytes 0-31/160")
            self.assertEqual(head.body, b"")
            self.assertFalse(active)
            self.assertEqual(loads[0], 0)
            history.assert_not_called()
            track_usage.assert_not_called()
            calls.clear()

            response = await stream_routes.db_zip_media_streamer(request("GET", b"bytes=60-83"), parts, "viewer")
            self.assertEqual(response.headers["Content-Range"], "bytes 60-83/160")
            self.assertEqual(b"".join([chunk async for chunk in response.body_iterator]), clip[60:84])
            self.assertEqual(calls, [])

            response = await stream_routes.db_zip_media_streamer(request("GET", b"bytes=0-31"), parts, "other")
            self.assertEqual(b"".join([chunk async for chunk in response.body_iterator]), clip[:32])
            self.assertEqual(calls, [1, 2])
            await asyncio.sleep(0)
            self.assertEqual(loads[0], 0)
            self.assertEqual(track_usage.await_count, 2)
            self.assertEqual([c.args[0]["meta"]["title"] for c in history.await_args_list], ["clip.mkv"] * 3)

    async def test_split_resolves_two_parts_at_a_time_and_preserves_offsets(self):
        started = []
        completed = []
        release_first = asyncio.Event()

        async def get_file_properties(chat_id, message_id):
            started.append((chat_id, message_id))
            if message_id == 1:
                await release_first.wait()
            if message_id == 2:
                release_first.set()
            completed.append(message_id)
            return SimpleNamespace(file_size=message_id * 10)

        streamer = SimpleNamespace(get_file_properties=get_file_properties)
        parts, size = await asyncio.wait_for(virtual_dl.resolve_virtual_parts(
            [{"chat_id": 123, "msg_id": msg_id} for msg_id in (1, 2, 3, 1)],
            streamer,
        ), timeout=2)
        self.assertEqual(started, [(-100123, 1), (-100123, 2), (-100123, 3)])
        self.assertEqual(completed, [2, 1, 3])
        self.assertEqual([part["cum_start"] for part in parts], [0, 10, 30, 60])
        self.assertEqual([part["msg_id"] for part in parts], [1, 2, 3, 1])
        self.assertEqual(size, 70)

    async def test_split_cancels_peer_lookup_after_a_part_fails(self):
        peer_started = asyncio.Event()
        peer_cancelled = asyncio.Event()

        async def get_file_properties(chat_id, message_id):
            self.assertEqual(chat_id, 123)
            if message_id == 1:
                await peer_started.wait()
                raise FileNotFoundError("Part unavailable")
            peer_started.set()
            try:
                await asyncio.Future()
            finally:
                peer_cancelled.set()

        streamer = SimpleNamespace(get_file_properties=get_file_properties)
        with self.assertRaisesRegex(FileNotFoundError, "Part unavailable"):
            await asyncio.wait_for(virtual_dl.resolve_virtual_parts(
                [{"chat_id": 123, "msg_id": 1}, {"chat_id": 123, "msg_id": 2}],
                streamer, prefix_100=False,
            ), timeout=2)
        self.assertTrue(peer_cancelled.is_set())

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
            await asyncio.sleep(0)
        self.assertIs(reader._file_id_cache[123, 1], replacement)
        self.assertEqual(session.send.await_count, 2)

    async def test_playback_finishes_before_title_lookup_and_preserves_analytics_title(self):
        with (
            patch.object(custom_dl.ByteStreamer, "_clean_cache", new_callable=AsyncMock),
            patch.object(custom_dl.ByteStreamer, "_prewarm_sessions", new_callable=AsyncMock),
            patch.object(custom_dl, "_ensure_stale_cleaner"),
        ):
            reader = custom_dl.ByteStreamer(object())
        reader._get_media_session = AsyncMock(return_value=SimpleNamespace(
            send=AsyncMock(return_value=SimpleNamespace(bytes=b"data")),
        ))
        reader._get_location = AsyncMock(return_value=SimpleNamespace(id=1))
        file_id = SimpleNamespace(dc_id=1, media_id=1, file_size=4, file_name="clip.mkv", mime_type=None)
        release_title = asyncio.Event()
        title_recorded = asyncio.Event()
        saved_titles = []

        async def get_title(_):
            await release_title.wait()
            return "Library title"

        async def persist(entry):
            saved_titles.append(entry["meta"]["title"])
            title_recorded.set()

        request = Request({
            "type": "http", "method": "GET",
            "headers": [(b"range", b"bytes=0-3")],
            "path_params": {"name": "clip.mkv"},
        })
        with (
            patch.object(stream_routes, "_db_client", new=AsyncMock(return_value=(0, reader, file_id))),
            patch.object(stream_routes, "_prefetch_for", return_value=(1, 1)),
            patch.object(stream_routes, "track_usage", new_callable=AsyncMock),
            patch.object(stream_routes, "_title_cache", {}),
            patch.object(custom_dl, "_RECENT_CHUNKS", {}),
            patch.object(custom_dl, "ACTIVE_STREAMS", {}),
            patch.object(custom_dl, "work_loads", {0: 0}),
            patch.object(custom_dl, "client_avg_mbps", {0: 0}),
            patch.object(db, "get_title_by_stream_id", side_effect=get_title),
            patch.object(db, "log_stream_stats", side_effect=persist),
        ):
            async def read():
                response = await stream_routes.media_streamer(request, 123, 1, "fixture", stream_id_hash="id")
                self.assertEqual(response.status_code, 206)
                return b"".join([piece async for piece in response.body_iterator])

            try:
                self.assertEqual(await asyncio.wait_for(read(), timeout=2), b"data")
                stream = next(iter(custom_dl.ACTIVE_STREAMS.values()))
                self.assertEqual(stream["meta"]["title"], "clip.mkv")
                self.assertEqual(stream["meta"]["source"], "direct")
                self.assertFalse(title_recorded.is_set())
            finally:
                release_title.set()
            await asyncio.wait_for(title_recorded.wait(), timeout=2)
            self.assertEqual(saved_titles, ["Library title"])
            self.assertEqual(stream["meta"]["title"], "Library title")

    async def test_title_lookup_error_keeps_the_playback_filename(self):
        meta = {"title": "clip.mkv"}
        with (
            patch.object(stream_routes, "_title_cache", {}),
            patch.object(db, "get_title_by_stream_id", side_effect=RuntimeError("Database unavailable")),
        ):
            await stream_routes._stream_title_task("id", meta)
            self.assertEqual(meta["title"], "clip.mkv")
            self.assertNotIn("id", stream_routes._title_cache)

    async def test_global_split_head_skips_playback_bookkeeping(self):
        file_id = SimpleNamespace(file_size=4, file_name="clip.mkv", mime_type=None)
        request = Request({"type": "http", "method": "HEAD", "headers": [(b"range", b"bytes=0-1")]})
        with (
            patch.object(stream_routes, "_get_userbot_streamer", return_value=object()),
            patch.object(stream_routes, "resolve_virtual_parts", new=AsyncMock(return_value=([{"file_id": file_id}], 4))),
            patch.object(stream_routes, "track_usage", new_callable=AsyncMock) as track_usage,
            patch.object(db, "get_title_by_stream_id", new_callable=AsyncMock) as get_title,
        ):
            response = await stream_routes.global_virtual_media_streamer(request, [], "fixture", stream_id_hash="id")
            self.assertEqual(response.status_code, 206)
            self.assertEqual(response.headers["Content-Range"], "bytes 0-1/4")
            self.assertEqual(response.headers["Content-Length"], "2")
            self.assertEqual(response.body, b"")
            track_usage.assert_not_called()
            get_title.assert_not_called()


class StreamAnalyticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_late_title_is_ordered_by_stream_end_and_preserves_activity_count(self):
        allow_lookup = asyncio.Event()
        async def lookup(_):
            await allow_lookup.wait()
            return "$Older"

        analytics = SimpleNamespace(insert_one=AsyncMock())
        activity = SimpleNamespace(update_one=AsyncMock())
        newer = {"stream_id": "newer", "end_ts": 2.5, "meta": {"token": "viewer", "title": "$Newer"}}
        older = {"stream_id": "older", "end_ts": 1.5, "meta": {"token": "viewer", "title": "older.mkv"}}
        with (
            patch.object(stream_routes, "_title_cache", {}),
            patch.object(db, "get_title_by_stream_id", side_effect=lookup),
            patch.object(db, "dbs", {"tracking": {"stream_analytics": analytics, "user_activity": activity}}),
        ):
            title_task = stream_routes._stream_title_task("older-id", older["meta"])
            pending = asyncio.create_task(custom_dl._persist_stream_stats(older, title_task))
            try:
                await custom_dl._persist_stream_stats(newer, None)
            finally:
                allow_lookup.set()
                await pending

        self.assertEqual([call.args[0]["title"] for call in analytics.insert_one.await_args_list], ["$Newer", "$Older"])
        self.assertEqual(activity.update_one.await_count, 2)
        for call, entry in zip(activity.update_one.await_args_list, (newer, older)):
            expected_end = entry["end_ts"]
            last_title_end = {"$ifNull": ["$last_title_end_ts", 0]}
            self.assertEqual(call.args[0], {"_id": "viewer"})
            self.assertEqual(call.kwargs, {"upsert": True})
            self.assertEqual(call.args[1], [{"$set": {
                "last_active": {"$max": ["$last_active", datetime.fromtimestamp(expected_end, timezone.utc)]},
                "streams": {"$add": [{"$ifNull": ["$streams", 0]}, 1]},
                "last_title": {
                    "$cond": [{"$gte": [expected_end, last_title_end]}, {"$literal": entry["meta"]["title"]}, "$last_title"],
                },
                "last_title_end_ts": {"$max": [expected_end, last_title_end]},
            }}])

    async def test_cloudflare_reports_resolve_titles_and_finish_streams(self):
        active = {}
        recent = deque()
        with (
            patch.object(cf_live, "ACTIVE_STREAMS", active),
            patch.object(cf_live, "RECENT_STREAMS", recent),
            patch.object(stream_routes, "_title_cache", {}),
            patch.object(db, "get_title_by_stream_id", new=AsyncMock(return_value="Cloudflare title")),
            patch.object(db, "get_api_token", new=AsyncMock(return_value={"name": "Viewer"})),
            patch.object(db, "log_stream_stats", new_callable=AsyncMock) as log_stats,
        ):
            await cf_live.apply_report("worker", None, 1, [{
                "id": "clip", "token": "viewer", "file": "file-id", "name": "clip.mkv",
                "bytes": 1048576, "since": 1000, "last": 3500,
            }])
            self.assertEqual(active["cf-clip"]["meta"]["title"], "Cloudflare title")
            await cf_live.apply_report("worker", None, 1, [])
            await asyncio.sleep(0)
        self.assertFalse(active)
        self.assertEqual(recent[0]["end_ts"], 3.5)
        log_stats.assert_awaited_once_with(recent[0])


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
            "last_ts": 123.5,
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
            self.assertEqual(summary.json()["active_streams"][0]["speed_sampled_at"], 123.5)
            self.assertEqual(stream["status"], "active")
            detail = client.get("/stream/stats/example")
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.headers["cache-control"], "no-store")
            self.assertEqual(detail.json()["meta"], {"title": "Example"})
