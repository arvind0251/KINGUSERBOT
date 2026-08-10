import asyncio
import os
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from youtubesearchpython.__future__ import VideosSearch

from . import queues
from ..clients.clients import call
from ...console import USERBOT_PICTURE, BASE_URL, API_KEY, STORAGE_DIR

os.makedirs(STORAGE_DIR, exist_ok=True)


def _session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3)
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _clean(link: str) -> str:
    if "?si=" in link:
        link = link.split("?si=")[0]
    elif "&si=" in link:
        link = link.split("&si=")[0]
    return link


def _wait_until_ready(stream_url: str, max_attempts: int) -> bool:
    session = requests.Session()
    try:
        for attempt in range(max_attempts):
            try:
                r = session.get(stream_url, timeout=10, stream=True, allow_redirects=True)
                r.close()
                if r.status_code in (200, 206):
                    return True
                elif r.status_code in (204, 423, 404, 410):
                    time.sleep(2)
                    continue
                else:
                    return False
            except requests.exceptions.RequestException:
                time.sleep(2)
        return False
    finally:
        session.close()


def _baby_fetch_sync(vidid: str, want_video: bool = False):
    """Returns (stream_url, kind_type) or (None, None)."""
    try:
        max_attempts = 90 if want_video else 60
        kind = "video" if want_video else "song"
        url = f"{BASE_URL}/api/{kind}?query={vidid}&download=true&api={API_KEY}"

        session = _session()
        resp = session.get(url, timeout=60)
        data = resp.json()
        session.close()

        stream = data.get("stream")
        if not stream:
            return None, None

        kind_type = data.get("type")
        if kind_type == "live":
            return stream, kind_type

        ready = _wait_until_ready(stream, max_attempts)
        if not ready:
            return None, None

        return stream, kind_type

    except Exception as e:
        print(f"[BabyAPI] EXCEPTION for {vidid}: {type(e).__name__}: {e}")
        return None, None


async def get_result(query: str, video: bool = False):
    """
    Searches YouTube for the query, then asks BabyAPI for a playable
    stream URL for that video. Returns (stream_url, thumbnail) or
    (None, None) if nothing could be found/fetched.
    """
    query = _clean(query)
    results = VideosSearch(query, limit=1)
    vidid = thumbnail = None
    for result in (await results.next())["result"]:
        vidid = result["id"]
        try:
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        except Exception:
            thumbnail = USERBOT_PICTURE

    if not vidid:
        return None, None

    loop = asyncio.get_running_loop()
    stream_url, kind_type = await loop.run_in_executor(
        None, _baby_fetch_sync, vidid, video
    )
    if not stream_url:
        return None, None

    return stream_url, thumbnail


async def run_stream(link, type_val):
    """
    Create a stream object for pytgcalls v2.0.1
    Returns the stream URL directly (pytgcalls will handle the MediaStream internally)
    """
    return link


async def close_stream(chat_id):
    try:
        await queues.clear(chat_id)
    except Exception:
        pass
    try:
        return await call.leave_call(chat_id)
    except Exception:
        pass
