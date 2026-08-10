"""
Lightweight JSON "database" backed by a single Telegram message
inside LOG_GROUP_ID, used as a drop-in replacement for MongoDB.

Exposes a `mongodb` object with the same shape as a motor database
(`mongodb.<collection_name>`) so existing code in this folder
(pmguard.py, sudoers.py, raidzone.py, streams.py) does not need to
change how it calls find_one / find / insert_one / update_one / delete_one.

All data is kept in memory and mirrored into one Telegram message as
JSON. The message is edited on every write. On startup the message is
located by scanning recent history in LOG_GROUP_ID for our marker.
"""

import json
import asyncio

from ...console import LOG_GROUP_ID, LOGGER

_MARKER = "TGDB_JSON_V1"

_data = {}
_msg_id = None
_loaded = False
_client = None

_load_lock = asyncio.Lock()
_write_lock = asyncio.Lock()


def init_client(client):
    """Call once, after the pyrogram Client object is created."""
    global _client
    _client = client


async def _load():
    global _msg_id, _loaded
    if _loaded:
        return
    async with _load_lock:
        if _loaded:
            return
        if _client is not None:
            try:
                async for msg in _client.get_chat_history(LOG_GROUP_ID, limit=200):
                    if msg.text and msg.text.startswith(_MARKER):
                        payload = msg.text[len(_MARKER):].strip()
                        _data.update(json.loads(payload))
                        _msg_id = msg.id
                        break
            except Exception as e:
                LOGGER.error(f"TGDB: failed to load existing data - {e}")
        _loaded = True
        if _msg_id is None:
            await _persist()


async def _persist():
    global _msg_id
    if _client is None:
        return
    text = f"{_MARKER}\n{json.dumps(_data)}"
    async with _write_lock:
        try:
            if _msg_id:
                await _client.edit_message_text(LOG_GROUP_ID, _msg_id, text)
            else:
                m = await _client.send_message(LOG_GROUP_ID, text)
                _msg_id = m.id
        except Exception as e:
            LOGGER.error(f"TGDB: failed to persist data - {e}")


def _match(doc: dict, filt: dict) -> bool:
    if not filt:
        return True
    for k, v in filt.items():
        if isinstance(v, dict):
            for op, val in v.items():
                current = doc.get(k, 0)
                if op == "$gt" and not (current > val):
                    return False
                if op == "$lt" and not (current < val):
                    return False
                if op == "$gte" and not (current >= val):
                    return False
                if op == "$lte" and not (current <= val):
                    return False
        else:
            if doc.get(k) != v:
                return False
    return True


class _AsyncCursor:
    """Minimal async-iterable to mimic motor's find() cursor."""

    def __init__(self, docs):
        self._docs = docs
        self._i = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._i >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._i]
        self._i += 1
        return doc


class Collection:
    def __init__(self, name):
        self.name = name

    async def _ensure(self):
        await _load()
        if self.name not in _data:
            _data[self.name] = []

    async def find_one(self, filt=None):
        await self._ensure()
        for doc in _data[self.name]:
            if _match(doc, filt):
                return doc
        return None

    def find(self, filt=None):
        # Not awaited - returns an async cursor, matching motor's API
        docs = [d for d in _data.get(self.name, []) if _match(d, filt)]
        return _AsyncCursor(docs)

    async def insert_one(self, doc: dict):
        await self._ensure()
        _data[self.name].append(dict(doc))
        await _persist()

    async def update_one(self, filt: dict, update: dict, upsert: bool = False):
        await self._ensure()
        for doc in _data[self.name]:
            if _match(doc, filt):
                if "$set" in update:
                    doc.update(update["$set"])
                await _persist()
                return
        if upsert:
            new_doc = dict(filt)
            if "$set" in update:
                new_doc.update(update["$set"])
            _data[self.name].append(new_doc)
            await _persist()

    async def delete_one(self, filt: dict):
        await self._ensure()
        for i, doc in enumerate(_data[self.name]):
            if _match(doc, filt):
                del _data[self.name][i]
                await _persist()
                return


class _MongoDBShim:
    def __getattr__(self, name):
        return Collection(name)


mongodb = _MongoDBShim()
