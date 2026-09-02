from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request


PKCE_CHALLENGE_RE = re.compile(r"^[A-Za-z0-9_-]{43,128}$")
DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9._~-]{16,160}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{8,160}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def random_token(prefix: str, byte_count: int = 32) -> str:
    return f"{prefix}{secrets.token_urlsafe(byte_count)}"


def constant_time_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def validate_pkce_challenge(value: object) -> str:
    challenge = str(value or "").strip()
    if not PKCE_CHALLENGE_RE.fullmatch(challenge):
        raise HTTPException(status_code=422, detail="codeChallenge must be an S256 PKCE challenge")
    return challenge


def validate_pkce_verifier(value: object, expected_challenge: str) -> None:
    verifier = str(value or "")
    if not PKCE_CHALLENGE_RE.fullmatch(verifier):
        raise HTTPException(status_code=401, detail="authorization code verification failed")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    actual = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    if not constant_time_equal(actual, expected_challenge):
        raise HTTPException(status_code=401, detail="authorization code verification failed")


def validate_device_id(value: object) -> str:
    device_id = str(value or "").strip()
    if not DEVICE_ID_RE.fullmatch(device_id):
        raise HTTPException(status_code=422, detail="deviceId is invalid")
    return device_id


def validate_request_id(value: object) -> str:
    request_id = str(value or "").strip()
    if not REQUEST_ID_RE.fullmatch(request_id):
        raise HTTPException(status_code=422, detail="requestId is invalid")
    return request_id


class FixedWindowRateLimiter:
    """Small-process safety net; edge/Redis limits should remain enabled in production."""

    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, key: str, *, limit: int, window_seconds: int) -> None:
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            events = self._events[key]
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= limit:
                raise HTTPException(status_code=429, detail="too many requests")
            events.append(now)


rate_limiter = FixedWindowRateLimiter()


def client_key(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    if forwarded:
        return forwarded[:80]
    return (request.client.host if request.client else "unknown")[:80]
