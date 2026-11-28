_EBML = b"\x1a\x45\xdf\xa3"
_DOC_TYPE = 0x4282
_VIDEO_TYPES = {b"matroska": "video/x-matroska", b"webm": "video/webm"}


def _vint(data: bytes, offset: int, end: int, *, size: bool) -> tuple[int, int] | None:
    if offset >= end or not (first := data[offset]):
        return None
    length = 9 - first.bit_length()
    next_offset = offset + length
    if next_offset > end or (not size and length > 4):
        return None
    value = int.from_bytes(data[offset:next_offset], "big")
    if size:
        marker = 1 << (7 * length)
        value ^= marker
        if value == marker - 1:
            return None
    return value, next_offset


def split_video_content_type(prefix: bytes) -> str | None:
    """Identify Matroska or WebM from a complete EBML header."""
    if not prefix.startswith(_EBML):
        return None
    header_size = _vint(prefix, len(_EBML), len(prefix), size=True)
    if header_size is None:
        return None
    length, offset = header_size
    end = offset + length
    if end > len(prefix):
        return None
    while offset < end:
        element = _vint(prefix, offset, end, size=False)
        if element is None:
            return None
        tag, offset = element
        element_size = _vint(prefix, offset, end, size=True)
        if element_size is None:
            return None
        length, offset = element_size
        next_offset = offset + length
        if next_offset > end:
            return None
        if tag == _DOC_TYPE:
            return _VIDEO_TYPES.get(prefix[offset:next_offset])
        offset = next_offset
    return None
