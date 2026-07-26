import json
import time
import threading
import requests
from loguru import logger

RENDER_URL = "https://render-1-ethy.onrender.com/api/update"
REPORT_INTERVAL = 30  # seconds


def _export_points_loop(account_manager, url, interval):
    while True:
        try:
            for status in account_manager.get_all_status():
                alias = status["alias"]
                data = {
                    "account": alias,
                    "platform": "kick",
                    "updated": int(time.time()),
                    "channels": {},
                    "streamer_status": {},
                }

                for name, s in status.get("streamers", {}).items():
                    data["channels"][name] = s.get("points", 0)
                    data["streamer_status"][name] = bool(s.get("online", False))

                # local backup, same as the Twitch bot does
                try:
                    with open(f"points_{alias}.json", "w") as f:
                        json.dump(data, f)
                except Exception as e:
                    logger.warning(f"[{alias}] local backup failed: {e}")

                try:
                    requests.post(url, json=data, timeout=10)
                    logger.debug(f"[{alias}] sent: {data}")
                except Exception as e:
                    logger.warning(f"[{alias}] send error: {e}")

        except Exception as e:
            logger.warning(f"export error: {e}")

        time.sleep(interval)


def start_reporter(account_manager, url: str = RENDER_URL, interval: int = REPORT_INTERVAL, enabled: bool = True):
    """Start the points exporter as a daemon thread, same pattern as the Twitch bot."""
    if not enabled:
        logger.info("📡 Render reporter disabled (RenderDashboard.enabled=false)")
        return
    if not url:
        logger.warning("📡 Render reporter has no URL configured, skipping")
        return

    threading.Thread(
        target=_export_points_loop,
        args=(account_manager, url, interval),
        daemon=True,
    ).start()
    logger.info(f"📡 Render reporter started -> {url} (every {interval}s)")
