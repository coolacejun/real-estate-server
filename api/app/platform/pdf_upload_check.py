"""One-shot untrusted PDF parser. Run only in a bounded child process."""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys


def inspect_pdf(data: bytes) -> dict:
    from pypdf import PdfReader
    from pypdf.generic import ArrayObject, DictionaryObject, IndirectObject, NameObject

    if not 0 < len(data) <= 16 * 1024 * 1024 or not data.startswith(b'%PDF-') or b'%%EOF' not in data[-1024:]:
        raise ValueError('invalid PDF')
    reader = PdfReader(io.BytesIO(data), strict=True)
    if reader.is_encrypted:
        raise ValueError('encrypted PDF')
    pages = len(reader.pages)
    if not 1 <= pages <= 200:
        raise ValueError('page count')
    forbidden = {'/JavaScript', '/JS', '/OpenAction', '/AA', '/Launch', '/EmbeddedFiles',
                 '/EF', '/RichMedia', '/XFA', '/AcroForm', '/SubmitForm', '/GoToR',
                 '/URI', '/FileAttachment', '/Sound', '/Movie', '/Rendition'}
    queue = [reader.trailer]
    seen = set()
    visited = 0
    while queue:
        value = queue.pop()
        visited += 1
        if visited > 100000:
            raise ValueError('PDF object limit')
        if isinstance(value, IndirectObject):
            key = (value.idnum, value.generation)
            if key in seen:
                continue
            seen.add(key)
            value = value.get_object()
        if isinstance(value, DictionaryObject):
            if forbidden.intersection(value.keys()):
                raise ValueError('active content')
            queue.extend(value.values())
        elif isinstance(value, ArrayObject):
            queue.extend(value)
        elif isinstance(value, NameObject) and str(value) in forbidden:
            raise ValueError('active content')
    return {'sha256': hashlib.sha256(data).hexdigest(), 'pageCount': pages, 'byteSize': len(data)}


if __name__ == '__main__':
    if os.name == 'posix':
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024, 512 * 1024 * 1024))
        resource.setrlimit(resource.RLIMIT_CPU, (8, 8))
        resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))
    try:
        print(json.dumps(inspect_pdf(sys.stdin.buffer.read(16 * 1024 * 1024 + 1))))
    except Exception:
        sys.exit(1)
