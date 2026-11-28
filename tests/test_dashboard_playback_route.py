import asyncio
import json
import os
import time
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import FastAPI

with patch.dict(os.environ, {
    "DATABASE": "mongodb://127.0.0.1:1/tracking,mongodb://127.0.0.1:1/storage",
    "API_ID": "1234",
    "API_HASH": "00000000000000000000000000000000",
    "BOT_TOKEN": "1234:test-owner",
}):
    from Backend import db
    from Backend.fastapi.routes import cf_routes, stream_routes
    from Backend.helper import analytics, cf_live
    from Backend.helper.cf_stream import _hmac


class DashboardPlaybackRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_reports_identify_the_playback_route_independently_of_ip(self):
        now = time.time()
        direct = {
            "status": "active", "total_bytes": 4096, "last_ts": now - 20, "start_ts": now - 40,
            "meta": {"token": "viewer", "source": "direct", "title": "Direct clip"},
        }
        active = {"direct-clip": direct}
        history = deque()
        user = {"_id": "viewer", "name": "Viewer", "proxy": True, "last_title": "Previous clip"}
        token_cursor = MagicMock()
        token_cursor.__aiter__.return_value = [{"token": "viewer"}]
        activity = MagicMock()
        activity.delete_many = AsyncMock()
        activity.count_documents = AsyncMock(return_value=1)
        activity.find.return_value.sort.return_value.skip.return_value.limit.return_value.to_list = AsyncMock(return_value=[user])
        tracking = {
            "api_tokens": SimpleNamespace(find=MagicMock(return_value=token_cursor)),
            "user_activity": activity,
        }
        app = FastAPI()
        app.include_router(cf_routes.router)
        secret = "synthetic-worker-secret"

        async def viewer():
            return (await analytics.get_activity_overview())["users"][0]

        async def stats():
            return json.loads((await stream_routes.get_stream_stats()).body)

        with (
            patch.object(cf_routes.SettingsManager, "current", return_value=SimpleNamespace(cf_stream_secret=secret)),
            patch.object(cf_routes, "worker_tokens", new=AsyncMock(return_value=[])),
            patch.object(cf_live, "ACTIVE_STREAMS", active),
            patch.object(cf_live, "RECENT_STREAMS", history),
            patch.object(stream_routes, "ACTIVE_STREAMS", active),
            patch.object(stream_routes, "RECENT_STREAMS", history),
            patch.object(stream_routes, "_lookup_title", new=AsyncMock(return_value="Cloudflare clip")),
            patch.object(analytics, "ACTIVE_STREAMS", active),
            patch.object(db, "dbs", {"tracking": tracking}),
            patch.object(db, "get_api_token", new=AsyncMock(return_value={"name": "Viewer"})),
            patch.object(db, "log_stream_stats", new_callable=AsyncMock),
        ):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                async def report(rows, signed=True):
                    body = json.dumps({"member": "worker", "streams": rows})
                    stamp = str(int(time.time()))
                    signature = _hmac(secret, f"{stamp}\nPOST /api/cf/usage\n{body}")
                    return await client.post(
                        "/api/cf/usage", content=body,
                        headers={"x-cf-time": stamp, "x-cf-sig": signature if signed else "invalid"},
                    )

                cloudflare = [{
                    "id": "clip", "token": "viewer", "file": "file-id", "name": "clip.mkv",
                    "bytes": 1048576, "since": int((now - 40) * 1000), "last": int((now - 10) * 1000),
                }]
                self.assertEqual((await report(cloudflare, signed=False)).status_code, 401)
                self.assertEqual((await viewer())["playback_source"], "direct")

                self.assertEqual((await report(cloudflare)).json(), {"ok": True})
                row = await viewer()
                self.assertTrue(row["proxy"])
                self.assertEqual((row["now_playing"], row["playback_source"]), ("Cloudflare clip", "cloudflare"))
                self.assertEqual({s["source"] for s in (await stats())["active_streams"]}, {"direct", "cloudflare"})

                user["proxy"] = False
                self.assertFalse((await viewer())["proxy"])
                self.assertEqual((await viewer())["playback_source"], "cloudflare")
                direct["last_ts"] = now
                row = await viewer()
                self.assertEqual((row["now_playing"], row["playback_source"]), ("Direct clip", "direct"))

                direct["status"] = "finished"
                active["cf-clip"]["reported_at"] = now - 61
                self.assertIsNone((await viewer())["playback_source"])
                self.assertIsNone((await viewer())["now_playing"])
                self.assertEqual([s["source"] for s in (await stats())["active_streams"]], ["direct"])

                self.assertEqual((await report(cloudflare)).status_code, 200)
                self.assertEqual((await viewer())["playback_source"], "cloudflare")
                self.assertEqual((await report([])).status_code, 200)
                await asyncio.sleep(0)
                self.assertIsNone((await viewer())["playback_source"])
                self.assertEqual((await stats())["recent_streams"][0]["source"], "cloudflare")
