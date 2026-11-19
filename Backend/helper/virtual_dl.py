from typing import Dict, List, Optional, Tuple

from fastapi import Request

from Backend.helper.custom_dl import ByteStreamer
from Backend.logger import LOGGER


#----- Fetch metadata for each split part and compute cumulative offsets -> (parts, total_size)
async def resolve_virtual_parts(
    parts_payload: List[dict],
    streamer: ByteStreamer,
    prefix_100: bool = True,
) -> Tuple[List[Dict], int]:
    parts: List[Dict] = []
    cum = 0
    for idx, p in enumerate(parts_payload):
        raw_chat = int(p["chat_id"])
        chat_id = int(f"-100{raw_chat}") if prefix_100 and raw_chat > 0 else raw_chat
        msg_id = int(p["msg_id"])
        file_id = await streamer.get_file_properties(chat_id=chat_id, message_id=msg_id)
        size = file_id.file_size
        parts.append({
            "index": idx,
            "chat_id": chat_id,
            "msg_id": msg_id,
            "file_id": file_id,
            "size": size,
            "cum_start": cum,
        })
        cum += size
    return parts, cum


#----- Parts intersecting the virtual byte range [start, end]
def parts_overlapping_range(parts: List[Dict], start: int, end: int) -> List[Dict]:
    return [p for p in parts if not (p["cum_start"] + p["size"] - 1 < start or p["cum_start"] > end)]


#----- Yield bytes across the virtual range [start, end], transparently spanning parts
async def virtual_stream_generator(
    parts: List[Dict],
    start: int,
    end: int,
    chunk_size: int,
    streamer: ByteStreamer,
    client_index: int,
    request: Optional[Request],
    meta: Optional[dict],
    stream_id: str,
    parallelism: int,
    prefetch_count: int,
):
    pos = start
    for part in parts:
        part_start = part["cum_start"]
        part_end = part_start + part["size"] - 1
        if part_end < pos:
            continue
        if part_start > end:
            break

        local_start = max(pos, part_start) - part_start
        local_end = min(end, part_end) - part_start

        offset = local_start - (local_start % chunk_size)
        first_part_cut = local_start - offset
        last_part_cut = (local_end % chunk_size) + 1
        part_count = (local_end - offset) // chunk_size + 1

        body_gen = await streamer.prefetch_stream(
            file_id=part["file_id"],
            client_index=client_index,
            offset=offset,
            first_part_cut=first_part_cut,
            last_part_cut=last_part_cut,
            part_count=part_count,
            chunk_size=chunk_size,
            prefetch=prefetch_count,
            stream_id=f"{stream_id}-p{part['index']}",
            meta=meta,
            parallelism=parallelism,
            request=request,
            chat_id=part["chat_id"],
            message_id=part["msg_id"],
        )

        async for chunk in body_gen:
            yield chunk

        #----- Stop fetching further parts if the client has disconnected
        if request is not None:
            try:
                if await request.is_disconnected():
                    LOGGER.debug("Virtual stream %s: client gone, stopping at part %s", stream_id, part["index"])
                    return
            except Exception:
                pass

        pos = part_end + 1
        if pos > end:
            break


def ebml_video_mime(prefix: bytes) -> Optional[str]:
    """Identify Matroska or WebM by the DocType in a complete EBML header."""
    if not prefix.startswith(b"\x1a\x45\xdf\xa3"):
        return None

    def vint(offset: int, max_width: int, strip_marker: bool):
        if offset >= len(prefix) or prefix[offset] == 0:
            raise ValueError("Missing EBML variable-length integer")
        width = 9 - prefix[offset].bit_length()
        if width > max_width or offset + width > len(prefix):
            raise ValueError("Invalid EBML variable-length integer")
        value = int.from_bytes(prefix[offset:offset + width], "big")
        if strip_marker:
            marker = 1 << (7 * width)
            value -= marker
            if value == marker - 1:
                raise ValueError("Unknown EBML element size")
        return value, offset + width

    try:
        size, offset = vint(4, 8, True)
        header_end = offset + size
        if header_end > len(prefix):
            return None
        while offset < header_end:
            element, offset = vint(offset, 4, False)
            size, offset = vint(offset, 8, True)
            end = offset + size
            if end > header_end:
                return None
            if element == 0x4282:
                return {b"matroska": "video/x-matroska", b"webm": "video/webm"}.get(prefix[offset:end])
            offset = end
    except ValueError:
        return None
    return None
