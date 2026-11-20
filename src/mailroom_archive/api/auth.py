"""Bearer tokens: the write token may also read; every route but /healthz needs one."""

import secrets

from fastapi import HTTPException, Request


def _matches(given: str, expected: str) -> bool:
    return bool(expected) and secrets.compare_digest(given.encode(), expected.encode())


def _bearer(request: Request) -> str:
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(401, "bearer token required", headers={"WWW-Authenticate": "Bearer"})
    return token


def require_read(request: Request) -> None:
    s = request.app.state.settings
    token = _bearer(request)
    if not (_matches(token, s.token) or _matches(token, s.read_token)):
        raise HTTPException(401, "invalid token", headers={"WWW-Authenticate": "Bearer"})


def require_write(request: Request) -> None:
    s = request.app.state.settings
    token = _bearer(request)
    if _matches(token, s.token):
        return
    if _matches(token, s.read_token):
        raise HTTPException(403, "read token cannot write")
    raise HTTPException(401, "invalid token", headers={"WWW-Authenticate": "Bearer"})
