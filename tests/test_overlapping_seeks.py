import asyncio
import os
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

with patch.dict(os.environ, {
    "DATABASE": "mongodb://127.0.0.1:1/tracking,mongodb://127.0.0.1:1/storage",
    "API_ID": "1234",
    "API_HASH": "00000000000000000000000000000000",
    "BOT_TOKEN": "1234:test-owner",
}):
    from Backend import db
    from Backend.helper import custom_dl


PAGE = 1024 * 1024


class OverlappingSeekTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        mocks = ExitStack()
        self.addCleanup(mocks.close)
        mocks.enter_context(patch.object(custom_dl, "_RECENT_CHUNKS", {}))
        mocks.enter_context(patch.object(custom_dl, "ACTIVE_STREAMS", {}))
        mocks.enter_context(patch.object(custom_dl, "work_loads", {0: 0}))
        mocks.enter_context(patch.object(custom_dl, "client_avg_mbps", {0: 0}))
        mocks.enter_context(patch.object(db, "log_stream_stats", new_callable=AsyncMock))
        self.reader = object.__new__(custom_dl.ByteStreamer)
        self.reader._pending_page_reads = {}
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.origin_cancelled = asyncio.Event()
        self.page = (bytes(range(256)) * (PAGE // 256))
        self.file_id = SimpleNamespace(dc_id=1, media_id=1, file_size=PAGE)
        self.send = AsyncMock(side_effect=self.read_origin)
        self.reader._get_media_session = AsyncMock(return_value=SimpleNamespace(send=self.send))
        self.reader._get_location = AsyncMock(return_value=SimpleNamespace(id=1))

    async def read_origin(self, query):
        self.assertEqual((query.offset, query.limit), (0, PAGE))
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.origin_cancelled.set()
            raise
        return SimpleNamespace(bytes=self.page)

    async def read(self, name, start=0, token="same-token"):
        body = await self.reader.prefetch_stream(
            self.file_id, client_index=0, offset=0, first_part_cut=start, last_part_cut=start + 65536,
            part_count=1, chunk_size=PAGE, prefetch=1, parallelism=1, stream_id=name,
            chat_id=123, message_id=1, rewind_scope=token,
        )
        return b"".join([chunk async for chunk in body])

    async def wait_for_shared_readers(self, expected):
        async def ready():
            while sum(p.readers for p in self.reader._pending_page_reads.values()) != expected:
                await asyncio.sleep(0)
        await asyncio.wait_for(ready(), timeout=2)

    async def test_overlapping_ranges_share_one_read_after_first_request_cancels(self):
        original = asyncio.create_task(self.read("first"))
        await asyncio.wait_for(self.started.wait(), timeout=2)
        survivor = asyncio.create_task(self.read("overlap", start=4096))
        try:
            await self.wait_for_shared_readers(2)
            original.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(original, timeout=2)
            self.assertFalse(self.origin_cancelled.is_set())
            self.release.set()
            self.assertEqual(await asyncio.wait_for(survivor, timeout=2), self.page[4096:69632])
            self.send.assert_awaited_once()
            self.assertEqual(await self.read("repeat"), self.page[:65536])
            self.send.assert_awaited_once()
            self.assertFalse(self.reader._pending_page_reads)
        finally:
            self.release.set()
            await asyncio.gather(original, survivor, return_exceptions=True)

    async def test_last_cancel_stops_the_origin_and_allows_another_read(self):
        request = asyncio.create_task(self.read("cancel"))
        await asyncio.wait_for(self.started.wait(), timeout=2)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(request, timeout=2)
        self.assertTrue(self.origin_cancelled.is_set())
        self.assertFalse(self.reader._pending_page_reads)
        self.release.set()
        self.assertEqual(await self.read("retry"), self.page[:65536])
        self.assertEqual(self.send.await_count, 2)

    async def test_different_tokens_do_not_share_an_inflight_read(self):
        first = asyncio.create_task(self.read("first-token"))
        second = asyncio.create_task(self.read("second-token", token="different"))
        try:
            await self.wait_for_shared_readers(2)
            self.assertEqual(len(self.reader._pending_page_reads), 2)
            self.release.set()
            self.assertEqual(await asyncio.gather(first, second), [self.page[:65536]] * 2)
            self.assertEqual(self.send.await_count, 2)
        finally:
            self.release.set()
            await asyncio.gather(first, second, return_exceptions=True)
