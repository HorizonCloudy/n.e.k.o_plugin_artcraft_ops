"""artcraft_ops —— 应用层：写操作二次确认令牌。

深评缺口①：``require_confirm_on_write`` 一直存在但从未被任何调用点读过，写操作
等于零确认。本模块把它接上，并按三条口径实现：

  1. **一次性**：``consume`` 先 pop 再校验，重放同一个令牌直接拒；
  2. **绑请求**：令牌只对生成它的那次请求指纹有效，换 out_file / 换 steps 即失效；
  3. **不落盘**：内存里只存 SHA-256，重启即作废（重启后重放本来就不该成立）。

指纹用规范化后的 app / steps / 输入路径 / 输出路径 / new_doc，全部是入口层
**校验之后**的值（已归一的 roots 内路径），所以令牌没法靠改拼写绕过。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import time
from typing import Any

CONFIRM_TTL_S: int = 300

# token digest -> (fingerprint, expires_at)
_ISSUED: dict[str, tuple[str, float]] = {}


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _step_fields(step: Any) -> tuple[str, Any]:
    cmd = getattr(step, "cmd", None)
    params = getattr(step, "params", None)
    if cmd is None and isinstance(step, dict):
        cmd = step.get("cmd")
        params = step.get("params")
    return str(cmd or ""), params


def fingerprint(
    app_id: str, steps: Any, input_file: str = "",
    out_file: str = "", new_doc: Any = None,
) -> str:
    """规范化请求的 SHA-256。steps 可为 Step dataclass 或 dict。"""
    payload = {
        "app": str(app_id or ""),
        "steps": [[cmd, params] for cmd, params in map(_step_fields, steps or ())],
        "input_file": str(input_file or ""),
        "out_file": str(out_file or ""),
        "new_doc": new_doc if isinstance(new_doc, dict) else None,
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _prune(now: float) -> None:
    for digest, (_fp, expires_at) in list(_ISSUED.items()):
        if now >= expires_at:
            _ISSUED.pop(digest, None)


def issue(request_fingerprint: str) -> str:
    now = time.time()
    _prune(now)
    token = secrets.token_urlsafe(24)
    _ISSUED[_digest(token)] = (request_fingerprint, now + CONFIRM_TTL_S)
    return token


def consume(token: str, request_fingerprint: str) -> bool:
    """校验并作废令牌；指纹不符 / 过期 / 未知都返回 False（令牌同样被丢弃）。"""
    record = _ISSUED.pop(_digest(str(token or "")), None)
    if record is None:
        return False
    saved_fp, expires_at = record
    return saved_fp == request_fingerprint and time.time() < expires_at


def clear() -> None:
    _ISSUED.clear()


__all__ = ["CONFIRM_TTL_S", "fingerprint", "issue", "consume", "clear"]
