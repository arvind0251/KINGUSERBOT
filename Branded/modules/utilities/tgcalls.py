from pyrogram import filters
from pytgcalls.types.update import (
    LeftVoiceChat,
    JoinedVoiceChat,
    StreamAudioEnded,
    StreamVideoEnded,
    Update,
)

from . import queues
from ..clients.clients import app, call
from .streams import run_stream, close_stream


async def run_async_calls():
    # Handle voice chat closed / left / kicked events
    @call.on_update(
        filters.update(
            (LeftVoiceChat,)
        )
    )
    async def voice_chat_closed_handler(_, update: Update):
        return await close_stream(update.chat_id)

    # Handle stream end events
    @call.on_update(filters.stream_end)
    async def stream_end_handler(_, update: Update):
        chat_id = update.chat_id
        queues.task_done(chat_id)
        if queues.is_empty(chat_id):
            return await close_stream(chat_id)
        check = queues.get(chat_id)
        file = check["file"]
        type = check["type"]
        stream = await run_stream(file, type)
        return await call.play(chat_id, stream)
