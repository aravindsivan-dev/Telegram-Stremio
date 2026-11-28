import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

with patch.dict(os.environ, {
    "DATABASE": "mongodb://127.0.0.1:1/tracking,mongodb://127.0.0.1:1/storage",
    "API_ID": "1234",
    "API_HASH": "00000000000000000000000000000000",
    "BOT_TOKEN": "1234:test-owner",
}):
    from Backend import db
    from Backend.fastapi.security import tokens


class UsageLimitTests(unittest.IsolatedAsyncioTestCase):
    async def limit_for(self, daily_date, monthly_date):
        count = 1024 ** 3
        token = {
            "subscription_exempt": True,
            "limits": {"daily_limit_gb": 1, "monthly_limit_gb": 1},
            "usage": {
                "daily": {"date": daily_date, "bytes": count},
                "monthly": {"month": monthly_date, "bytes": count},
            },
        }
        with (
            patch.object(db, "get_api_token", new=AsyncMock(return_value=token)),
            patch.object(tokens, "datetime") as clock,
        ):
            clock.now.return_value = datetime(2026, 1, 1, tzinfo=timezone.utc)
            return (await tokens.verify_token("fixture-only"))["limit_exceeded"]

    async def test_old_daily_and_monthly_usage_do_not_block_new_period(self):
        self.assertIsNone(await self.limit_for("2025-12-31", "2025-12"))

    async def test_current_monthly_limit_still_applies_after_the_daily_rollover(self):
        self.assertEqual(await self.limit_for("2025-12-31", "2026-01"), "monthly")

    async def test_current_daily_limit_takes_precedence(self):
        self.assertEqual(await self.limit_for("2026-01-01", "2026-01"), "daily")
