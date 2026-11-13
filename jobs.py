"""artcraft_ops —— 应用层：job 状态机 / 句柄回收 / 进度节流 / 恢复 / 消息面回执。

[FACT 链 2] 三条宿主约束决定了整个设计：
  1. 入口默认 10s 且超时只 ``task.cancel()``（`entry_executor.py:134-155`）**不杀 Popen**
     → 入口秒回 ``job_id``，后台 ``asyncio.create_task``，句柄存 ``_PROC_HANDLES``；
  2. ``CANCEL_RUN`` 只 cancel 协程（`host.py:1627-1632`）→ 取消入口必须自己
     ``terminate()`` 升级到 ``kill()``；
  3. 消息面 512 KiB 整条丢弃 → 只推固定模板（`job_id` / `status` / `pct` / `phase` /
     `result_handle`），**不插值异常对象**（§13.0 核心原则）。

P6 日志铁律：本文件只用预格式化参数，**禁 f-string 进 logger**、**禁 logger.exception**。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Iterable

from . import confirm, errors, locator, runner
from . import result as result_mod
from .errors import ArtcraftError, redact, redact_text
from .spec import (
    FIXED_PROGRESS_TEMPLATE,
    FORBIDDEN_AI_BEHAVIORS,
    INTERNAL_PROGRESS,
    JOB_TTL_S,
    MESSAGE_PLANE_MAX_BYTES,
    PUSH_MIN_INTERVAL_S,
    TERMINAL_STATUSES,
    USER_COMPLETION,
    USER_COMPLETION_TEMPLATE,
    ExecResult,
    JobState,
    JobStatus,
    RootsPolicy,
    SpillPolicy,
    Transport,
)

_JOBS: dict[str, JobState] = {}
_PROC_HANDLES: dict[str, runner.ProcHandle] = {}
_PERSIST_PATH: str = ""
JOB_MAX_RUNTIME_S: float = 300.0     # 后台长任务上限（LLM 硬上限 300s）
# 深评缺口③：旧实现没有任何并发上限，两个任务可以同时改同一份文档。
# 口径：全局最多 2 个 job 同时跑；同一个 app 串行（每 app 一把锁）。
JOB_MAX_CONCURRENCY: int = 2
_PRIMS: dict[str, Any] = {"loop": None, "semaphore": None, "app_locks": {}}

logger = logging.getLogger("artcraft_ops")


def init_handles() -> None:
    """startup 用：确保容器存在（纯内存，不 spawn、不读网络）。"""
    _JOBS.clear()
    _PROC_HANDLES.clear()
    _PRIMS.update(loop=None, semaphore=None, app_locks={})
    confirm.clear()


def _primitives(app_id: str) -> tuple[Any, Any]:
    """按**当前** event loop 取 (全局信号量, 该 app 的锁)；换 loop 整体重建。

    shortcut: asyncio 原语绑定创建它的 loop，宿主换 loop 复用旧原语会直接报错；
    重建等于放弃旧 loop 里的排队顺序 —— 若将来允许跨 loop 存活 job，需改为按 loop 分组。
    """
    loop = asyncio.get_running_loop()
    if _PRIMS["loop"] is not loop:
        _PRIMS["loop"] = loop
        _PRIMS["semaphore"] = asyncio.Semaphore(JOB_MAX_CONCURRENCY)
        _PRIMS["app_locks"] = {}
    locks: dict[str, Any] = _PRIMS["app_locks"]
    key = str(app_id or "")
    if key not in locks:
        locks[key] = asyncio.Lock()
    return _PRIMS["semaphore"], locks[key]


def status_value(job: JobState) -> str:
    status = getattr(job, "status", JobStatus.QUEUED)
    return status.value if isinstance(status, JobStatus) else str(status)


def set_persist_path(path: str) -> None:
    global _PERSIST_PATH
    _PERSIST_PATH = str(path or "")


# ---------------------------------------------------------------------------
# 创建 / 查询
# ---------------------------------------------------------------------------
def create_job(
    app_id: str,
    argv: Iterable[str],
    is_edit_sequence: bool = False,
    transport: Transport = Transport.CLI_RUN,
) -> JobState:
    """建 job（QUEUED）并登记。``job_id`` 短随机串，够用且不泄露时间线。"""
    now = time.time()
    job = JobState(
        job_id=f"job-{uuid.uuid4().hex[:12]}",
        app_id=str(app_id or ""),
        status=JobStatus.QUEUED,
        argv=tuple(str(a) for a in (argv or ())),
        created_at=now,
        updated_at=now,
        is_edit_sequence=bool(is_edit_sequence),
        transport=transport,
        last_push_at=0.0,
        last_progress_pct=-1,
    )
    _JOBS[job.job_id] = job
    _evict_expired(now)
    return job


def get_job(job_id: str) -> JobState | None:
    """取 job；未知 / 超过 TTL → ``None``（不臆造状态，由入口转 JOB_NOT_FOUND）。"""
    job = _JOBS.get(str(job_id or ""))
    if job is None:
        return None
    if _is_expired(job, time.time()):
        _JOBS.pop(job.job_id, None)
        return None
    return job


def _is_expired(job: JobState, now: float) -> bool:
    if status_value(job) not in TERMINAL_STATUSES:
        return False
    finished = float(getattr(job, "finished_at", 0.0) or 0.0)
    return bool(finished) and (now - finished) > JOB_TTL_S


def _evict_expired(now: float) -> None:
    for job_id, job in list(_JOBS.items()):
        if _is_expired(job, now):
            _JOBS.pop(job_id, None)


# ---------------------------------------------------------------------------
# 进度节流（§7）
# ---------------------------------------------------------------------------
def should_push(job: JobState, now: float, new_pct: int) -> bool:
    """跨 10% 边界 **且** 距上次推送 ≥2s 才推（状态变化由调用方另行推送）。"""
    pct = int(new_pct)
    last_pct = int(getattr(job, "last_progress_pct", -1))
    last_at = float(getattr(job, "last_push_at", 0.0) or 0.0)
    crossed_ten = (pct // 10) > (last_pct // 10)
    if not crossed_ten:
        return False
    return (float(now) - last_at) >= PUSH_MIN_INTERVAL_S


def _pct(job: JobState) -> int:
    try:
        return max(0, min(100, int(round(float(job.progress or 0.0) * 100))))
    except (TypeError, ValueError):  # pragma: no cover
        return 0


# ---------------------------------------------------------------------------
# 消息面（§13.2 P3 / §14.3 D3）
# ---------------------------------------------------------------------------
def _check_ai_behavior(ai_behavior: str) -> str:
    value = str(ai_behavior or "blind")
    if value in FORBIDDEN_AI_BEHAVIORS:
        raise ValueError(
            f"ai_behavior={value!r} is forbidden: it becomes background context on hot swap"
        )
    return value


def _message_payload(
    job: JobState, text: str, visibility: list[str], ai_behavior: str,
) -> dict[str, Any]:
    pct = _pct(job)
    payload: dict[str, Any] = {
        "parts": [{"text": redact_text(text)[:1000]}],
        "visibility": list(visibility),
        "ai_behavior": ai_behavior,
        "metadata": {
            "job_id": job.job_id,
            "phase": status_value(job),
            "progress": round(float(job.progress or 0.0), 3),
            "pct": pct,
            # 展示型 metadata：没有 isfile 消费者，一律走去标识化（S17）
            "result_path": redact_text(str(getattr(job, "result_path", "") or "")),
            "result_handle": redact_text(str(getattr(job, "result_handle", "") or "")),
        },
        "summary": redact_text(text)[:200],
        "detail": redact_text(text)[:500],
    }
    # 512 KiB 硬约束：超了逐级瘦身，绝不把整条 payload 交给宿主去丢弃
    if _payload_bytes(payload) > MESSAGE_PLANE_MAX_BYTES:
        payload.pop("detail", None)
    if _payload_bytes(payload) > MESSAGE_PLANE_MAX_BYTES:
        payload.pop("metadata", None)
        payload["summary"] = redact_text(text)[:80]
    if _payload_bytes(payload) > MESSAGE_PLANE_MAX_BYTES:
        payload = {
            "parts": [{"text": redact_text(text)[:200]}],
            "visibility": list(visibility),
            "ai_behavior": ai_behavior,
        }
    return redact(payload)


def _payload_bytes(payload: dict[str, Any]) -> int:
    try:
        return len(json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8"))
    except (TypeError, ValueError):  # pragma: no cover
        return MESSAGE_PLANE_MAX_BYTES + 1


async def push_progress(job: JobState, ctx: Any, ai_behavior: str = "blind") -> None:
    """推一条进度 / 完成摘要（内部进度不注入模型：``visibility=[]`` + ``blind``）。"""
    behavior = _check_ai_behavior(ai_behavior)
    if ctx is None or not callable(getattr(ctx, "push_message", None)):
        return

    terminal = status_value(job) in TERMINAL_STATUSES
    if terminal:
        text = USER_COMPLETION_TEMPLATE.format(
            job_id=job.job_id,
            status=status_value(job),
            result_handle=str(getattr(job, "result_handle", "") or getattr(job, "result_path", "")),
        )
        payload = _message_payload(job, text, USER_COMPLETION["visibility"], behavior)
    else:
        text = FIXED_PROGRESS_TEMPLATE.format(
            job_id=job.job_id, pct=_pct(job), phase=status_value(job),
        )
        payload = _message_payload(job, text, INTERNAL_PROGRESS["visibility"], behavior)

    job.last_push_at = time.time()
    job.last_progress_pct = _pct(job)
    try:
        ctx.push_message(**payload)
    except Exception:  # 推送失败不得影响 job 状态机
        logger.debug("artcraft_ops push_message failed for job %s", job.job_id)


def report_status_payload(job: JobState) -> dict[str, Any]:
    """§14.0 裁决 2：``{job_id, phase, progress, error_code}``，其余一律禁止。

    注意 ``PLUGIN_LOG_CTX_STATUS_UPDATE`` 默认 True → **整个 dict 会被 logger.info 落盘**。
    """
    return {
        "job_id": job.job_id,
        "phase": status_value(job),
        "progress": round(float(job.progress or 0.0), 3),
        "error_code": str(getattr(job, "error_code", "") or ""),
    }


async def report_completion(job: JobState, result: ExecResult | None, ctx: Any) -> None:
    """终态回执：``report_status``（结构化、字段白名单）+ ``push_message``（脱敏摘要）。

    execute 与 cancel_job 都可能对同一个 job 回执，`completion_reported` 保证只发一条。
    """
    if ctx is None:
        return
    if getattr(job, "completion_reported", False):
        return
    job.completion_reported = True
    report = getattr(ctx, "report_status", None)
    if callable(report):
        try:
            report(report_status_payload(job))
        except Exception:  # pragma: no cover
            logger.debug("artcraft_ops report_status failed for job %s", job.job_id)
    await push_progress(job, ctx)


# ---------------------------------------------------------------------------
# 执行
# ---------------------------------------------------------------------------
def _cwd_for(job: JobState, spec: Any) -> str:
    cli = str(job.argv[0]) if job.argv else ""
    base = os.path.dirname(cli) if cli else ""
    if base and os.path.isdir(base):
        return base
    return os.path.abspath(os.sep)


def _readonly_retry_allowed(job: JobState) -> bool:
    return (not job.is_edit_sequence) and (not job.retried_readonly)


def _finish(job: JobState, status: JobStatus, exit_code: int | None = None) -> None:
    """先到先赢：已有终态不得被后到的成功/失败覆盖（取消后 CLI 的退出码不是结果）。"""
    if status_value(job) in TERMINAL_STATUSES:
        return
    now = time.time()
    job.status = status
    job.exit_code = exit_code
    job.finished_at = now
    job.updated_at = now
    if status is JobStatus.SUCCEEDED:
        job.progress = 1.0


def _cancel_final(job: JobState) -> "ExecResult":
    job.error_code = "JOB_CANCELLED"
    job.error_message = errors.build_error_message("JOB_CANCELLED")
    _finish(job, JobStatus.CANCELLED, -1)
    return ExecResult(
        ok=False, app_id=job.app_id, job_id=job.job_id,
        error_code=job.error_code, error_message=job.error_message,
    )


def _crash(job: JobState, exc: BaseException) -> None:
    # 先落终态（写 finished_at/updated_at），再补错误字段：
    # _finish 现在先到先赢，把 status 提前写死会让它整段跳过。
    job.error_code = "JOB_CRASHED"
    job.error_message = errors.build_error_message("JOB_CRASHED")
    job.warnings = (job.warnings or ()) + (redact_text(f"crashed: {type(exc).__name__}"),)
    _finish(job, JobStatus.CRASHED, -1)


def _preempted(job: JobState) -> bool:
    """等锁期间被取消或已被判终态 —— 这两种都不得再 spawn。"""
    return bool(job.cancel_requested) or status_value(job) in TERMINAL_STATUSES


async def _cancelled(job: JobState, ctx: Any) -> ExecResult:
    final = _cancel_final(job)
    persist_if_configured()
    await report_completion(job, final, ctx)
    return final


def _spill_policy() -> SpillPolicy:
    """大响应落盘目录 = job 表所在目录（插件 data/），失败回落系统临时目录。"""
    base = os.path.dirname(_PERSIST_PATH) if _PERSIST_PATH else ""
    return SpillPolicy(spill_dir=base)


async def execute(
    job: JobState, ctx: Any = None, spec: Any = None, roots: RootsPolicy | None = None,
) -> ExecResult:
    """排队后执行：同一个 app 串行，全局最多 ``JOB_MAX_CONCURRENCY`` 个任务在跑。

    锁在写 ``RUNNING`` **之前**取：等锁期间 job 仍是 QUEUED，此时取消能干净地
    落在 CANCELLED（否则会出现「已经 RUNNING、进程却还没起」的窗口）。
    shortcut: 排队中的协程要等拿到锁才发现自己已终态；取消是即时生效在 **job 状态**
    上（get_job 立刻返回 cancelled），只是后台 task 的退出点延后，绝不额外跑一步。
    """
    semaphore, app_lock = _primitives(job.app_id)
    async with app_lock:
        if _preempted(job):
            return await _cancelled(job, ctx)
        async with semaphore:
            if _preempted(job):
                return await _cancelled(job, ctx)
            return await _run_locked(job, ctx, spec, roots)


async def _run_locked(
    job: JobState, ctx: Any, spec: Any, roots: RootsPolicy | None,
) -> ExecResult:
    """后台执行一次 ``run``；终态写回 ``job`` 并推消息面回执。

    只读序列遇 OSError 允许**重发一次**；编辑类绝不重发（§7 / §10 #13/#14）。
    """
    job.status = JobStatus.RUNNING
    job.started_at = time.time()
    job.updated_at = job.started_at
    if ctx is not None:
        await push_progress(job, ctx)

    if job.cancel_requested:
        final = _cancel_final(job)
        await report_completion(job, final, ctx)
        return final

    env = locator.build_cli_env()
    cwd = _cwd_for(job, spec)
    total_steps = max(1, int(getattr(job, "step_total", 0) or 0))
    attempt = 0
    final: ExecResult | None = None

    def _on_line(_line: str) -> None:
        job.step_done += 1
        job.updated_at = time.time()
        job.progress = min(0.99, job.step_done / total_steps)

    while True:
        if job.cancel_requested:
            final = _cancel_final(job)
            break
        attempt += 1
        job.attempts = attempt
        try:
            handle = await runner.spawn(
                list(job.argv), cwd=cwd, env=env, job_id=job.job_id,
                timeout=JOB_MAX_RUNTIME_S,
            )
        except (OSError, ValueError) as exc:
            if _readonly_retry_allowed(job):
                job.retried_readonly = True
                logger.warning("artcraft_ops spawn failed, retrying readonly once: %s",
                               redact_text(type(exc).__name__))
                continue
            _crash(job, exc)
            final = ExecResult(
                ok=False, app_id=job.app_id, job_id=job.job_id, exit_code=-1,
                error_code=job.error_code, error_message=job.error_message,
                warnings=job.warnings, output_files=(),
            )
            break

        job.pid = int(getattr(handle.proc, "pid", 0) or 0)
        # 单事件循环内 dict 赋值是原子的；不引入跨 loop 的 Lock 语义风险
        _PROC_HANDLES[job.job_id] = handle
        if job.cancel_requested:
            await runner.terminate(handle)
        try:
            raw = await asyncio.wait_for(
                runner.stream_lines(handle, _on_line), timeout=JOB_MAX_RUNTIME_S,
            )
        except (asyncio.TimeoutError, TimeoutError):
            await runner.terminate(handle)
            _PROC_HANDLES.pop(job.job_id, None)
            job.error_code = "JOB_TIMEOUT"
            job.error_message = errors.build_error_message(
                "JOB_TIMEOUT", seconds=int(JOB_MAX_RUNTIME_S),
            )
            _finish(job, JobStatus.FAILED, -1)
            final = ExecResult(
                ok=False, app_id=job.app_id, job_id=job.job_id, exit_code=-1,
                error_code=job.error_code, error_message=job.error_message,
            )
            break
        except OSError as exc:
            _PROC_HANDLES.pop(job.job_id, None)
            if _readonly_retry_allowed(job):
                job.retried_readonly = True
                logger.warning("artcraft_ops stream failed, retrying readonly once: %s",
                               redact_text(type(exc).__name__))
                continue
            _crash(job, exc)
            final = ExecResult(
                ok=False, app_id=job.app_id, job_id=job.job_id, exit_code=-1,
                error_code=job.error_code, error_message=job.error_message,
                warnings=job.warnings,
            )
            break
        finally:
            _PROC_HANDLES.pop(job.job_id, None)

        if job.cancel_requested:
            final = _cancel_final(job)
            break

        final = result_mod.parse_exec_output(raw, spec, job)
        job.exit_code = raw.returncode
        job.stdout_lines = len(raw.stdout_lines)
        job.inspect = final.inspect
        # 消息面裁剪 + 图片回传：终态之前做，`get_job` 才看得到 preview_url / 产物清单
        await result_mod.materialize(final, _spill_policy(), ctx)
        job.warnings = final.warnings
        job.result_handle = final.result_handle
        job.output_files = tuple(final.output_files or ())
        job.preview_url = final.preview_url
        job.progress = 1.0 if final.ok else job.progress
        if final.ok:
            _finish(job, JobStatus.SUCCEEDED, raw.returncode)
        else:
            job.error_code = final.error_code
            job.error_message = final.error_message
            _finish(job, JobStatus.FAILED, raw.returncode)
        break

    job.result_path = str(getattr(job, "result_path", "") or "")
    persist_if_configured()
    if final is None:  # pragma: no cover - 防御式
        final = ExecResult(ok=False, app_id=job.app_id, job_id=job.job_id)
    await report_completion(job, final, ctx)
    return final


# ---------------------------------------------------------------------------
# 取消（§7 核心修正）
# ---------------------------------------------------------------------------
async def cancel_job(job_id: str, ctx: Any = None) -> JobState:
    """取消：serve/bridge 先显式转发远端取消；CLI_RUN 直接收本地句柄。

    [FACT 链 2] 宿主 ``CANCEL_RUN`` 只 ``task.cancel()`` → 本地 ``terminate`` 必须自己发。
    """
    job = _JOBS.get(str(job_id or ""))
    if job is None:
        raise ArtcraftError("JOB_NOT_FOUND")

    if status_value(job) in TERMINAL_STATUSES:
        return job          # 终态任务幂等返回：不把 succeeded/failed 改写成 cancelled

    job.cancel_requested = True
    if job.transport in (Transport.CLI_SERVE, Transport.HTTP_BRIDGE) and job.cancel_method:
        # v1 不启用这两条通道；保留显式转发位点，失败不影响本地回收
        logger.warning("artcraft_ops remote cancel requested (%s) but channel is not implemented",
                       job.cancel_method)

    handle = _PROC_HANDLES.pop(job.job_id, None)
    if handle is not None:
        await runner.terminate(handle)

    job.error_code = "JOB_CANCELLED"
    job.error_message = errors.build_error_message("JOB_CANCELLED")
    _finish(job, JobStatus.CANCELLED, -1)
    persist_if_configured()
    await report_completion(
        job,
        ExecResult(
            ok=False, app_id=job.app_id, job_id=job.job_id,
            error_code=job.error_code, error_message=job.error_message,
        ),
        ctx,
    )
    return job


async def shutdown_all() -> None:
    """shutdown：遍历全部句柄 ``terminate()``（宿主不会替我们杀进程）。"""
    for job_id, handle in list(_PROC_HANDLES.items()):
        try:
            await runner.terminate(handle)
        except Exception:  # pragma: no cover
            logger.debug("artcraft_ops terminate failed for job %s", job_id)
        _PROC_HANDLES.pop(job_id, None)
    _PROC_HANDLES.clear()


# ---------------------------------------------------------------------------
# 持久化 / 恢复
# ---------------------------------------------------------------------------
def _job_to_dict(job: JobState) -> dict[str, Any]:
    return {
        "job_id": job.job_id, "app_id": job.app_id, "status": status_value(job),
        "argv": list(job.argv), "cwd": job.cwd,
        "created_at": job.created_at, "started_at": job.started_at,
        "updated_at": job.updated_at, "finished_at": job.finished_at,
        "progress": job.progress, "progress_label": job.progress_label,
        "pid": job.pid, "exit_code": job.exit_code, "stdout_lines": job.stdout_lines,
        "cancel_requested": job.cancel_requested,
        "is_edit_sequence": job.is_edit_sequence,
        "retried_readonly": job.retried_readonly,
        "error_code": job.error_code, "error_message": job.error_message,
        "result_path": job.result_path,
        "result_handle": job.result_handle,
        "warnings": list(job.warnings),
        "inspect": job.inspect,
        "attempts": job.attempts,
        "transport": job.transport.value if isinstance(job.transport, Transport)
        else str(job.transport),
        "remote_job_id": job.remote_job_id, "cancel_method": job.cancel_method,
        "step_total": job.step_total, "step_done": job.step_done,
        "output_files": list(job.output_files or ()),
        "preview_url": job.preview_url,
        "completion_reported": bool(job.completion_reported),
    }


def persist_jobs(path: str) -> None:
    """落盘 job 表（best-effort：失败只影响重启后恢复，不影响当前执行）。"""
    target = str(path or "")
    if not target:
        return
    try:
        os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
        payload = [_job_to_dict(job) for job in _JOBS.values()]
        tmp = f"{target}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(redact(payload), fh, ensure_ascii=False, default=str)
        os.replace(tmp, target)
    except OSError:
        return


def persist_if_configured() -> None:
    if _PERSIST_PATH:
        persist_jobs(_PERSIST_PATH)


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    if os.name != "nt":  # pragma: no cover
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    try:  # pragma: no cover - Windows 专用
        import ctypes

        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    except Exception:
        return False


def restore_jobs(path: str) -> None:
    """从磁盘恢复 job 表；「RUNNING 但 pid 不存在」标 CRASHED（编辑类不自动重发）。"""
    target = str(path or "")
    if not target or not os.path.isfile(target):
        return
    try:
        with open(target, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return
    if not isinstance(data, list):
        return

    for record in data:
        if not isinstance(record, dict):
            continue
        job_id = str(record.get("job_id") or "")
        if not job_id:
            continue
        try:
            status = JobStatus(str(record.get("status") or JobStatus.QUEUED.value))
        except ValueError:
            status = JobStatus.CRASHED
        try:
            transport = Transport(str(record.get("transport") or Transport.CLI_RUN.value))
        except ValueError:
            transport = Transport.CLI_RUN
        job = JobState(
            job_id=job_id,
            app_id=str(record.get("app_id") or ""),
            status=status,
            argv=tuple(str(a) for a in (record.get("argv") or ())),
            cwd=str(record.get("cwd") or ""),
            created_at=float(record.get("created_at") or 0.0),
            started_at=float(record.get("started_at") or 0.0),
            updated_at=float(record.get("updated_at") or 0.0),
            finished_at=float(record.get("finished_at") or 0.0),
            progress=float(record.get("progress") or 0.0),
            progress_label=str(record.get("progress_label") or ""),
            pid=int(record.get("pid") or 0),
            exit_code=record.get("exit_code"),
            stdout_lines=int(record.get("stdout_lines") or 0),
            cancel_requested=bool(record.get("cancel_requested", False)),
            is_edit_sequence=bool(record.get("is_edit_sequence", False)),
            retried_readonly=bool(record.get("retried_readonly", False)),
            error_code=str(record.get("error_code") or ""),
            error_message=str(record.get("error_message") or ""),
            result_path=str(record.get("result_path") or ""),
            result_handle=str(record.get("result_handle") or ""),
            warnings=tuple(str(w) for w in (record.get("warnings") or ())),
            inspect=record.get("inspect") if isinstance(record.get("inspect"), dict) else None,
            attempts=int(record.get("attempts") or 0),
            transport=transport,
            remote_job_id=str(record.get("remote_job_id") or ""),
            cancel_method=str(record.get("cancel_method") or ""),
            step_total=int(record.get("step_total") or 0),
            step_done=int(record.get("step_done") or 0),
            output_files=tuple(
                str(p) for p in (record.get("output_files") or ())
            ),
            preview_url=str(record.get("preview_url") or ""),
            completion_reported=bool(record.get("completion_reported", False)),
        )
        if job.status is JobStatus.RUNNING and not _pid_alive(job.pid):
            job.status = JobStatus.CRASHED
            job.error_code = "JOB_CRASHED"
            job.error_message = errors.build_error_message("JOB_CRASHED")
        _JOBS[job.job_id] = job


__all__ = [
    "_JOBS", "_PROC_HANDLES", "JOB_MAX_RUNTIME_S", "JOB_MAX_CONCURRENCY",
    "init_handles", "create_job", "get_job", "status_value",
    "should_push", "push_progress", "report_status_payload", "report_completion",
    "execute", "cancel_job", "shutdown_all",
    "persist_jobs", "persist_if_configured", "restore_jobs", "set_persist_path",
]
