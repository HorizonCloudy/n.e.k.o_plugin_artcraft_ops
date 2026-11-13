"""§10 #11 / #12 / #15 / #21 — job 状态机、进度节流、句柄回收。

覆盖：正常路径（create_job QUEUED、get_job、persist/restore、shutdown terminate）
      + 边界（进度节流 2s/10%、512 KiB payload、编辑类/只读分类）
      + 错误输入（未知 job、spawn 前取消）。

说明：``jobs.execute(job, deps)`` 的 ``Deps`` 结构在 ARCHITECTURE §2 未定义
（[INFERENCE]），故断线重发的**真实驱动路径**放到 test_entries_e2e.py 用 stub-SDK
入口跑；本文件只测 §1 明确列签名、不需要 Deps 的纯逻辑函数。
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from _fixtures import PUSH_MAX_BYTES, FakeCtx, FakeProcess, fake_handle

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 正常路径
# ---------------------------------------------------------------------------
def test_create_job_starts_queued(impl):
    """#11 正常：create_job 生成 QUEUED 状态、argv 归档、字段初值精确。"""
    jobs = impl("jobs")
    j = jobs.create_job("photocraft", ["cli", "run", "--cmd", "edit.fill"], False)

    assert j.status == "queued"
    assert j.app_id == "photocraft"
    assert tuple(j.argv) == ("cli", "run", "--cmd", "edit.fill")
    assert j.is_edit_sequence is False
    assert j.retried_readonly is False
    assert j.attempts == 0
    assert j.cancel_requested is False
    assert j.progress == 0.0
    assert isinstance(j.job_id, str) and j.job_id
    assert jobs.get_job(j.job_id) is j


def test_job_status_enum_values(impl):
    """正常：§7 六个状态值精确。"""
    spec = impl("spec")
    assert {s.value for s in spec.JobStatus} == {
        "queued", "running", "succeeded", "failed", "cancelled", "crashed",
    }


def test_persist_restore_jobs(impl, tmp_path):
    """正常：persist_jobs -> restore_jobs 往返能找回同一 job_id 与 inspect。"""
    jobs = impl("jobs")
    saved = dict(jobs._JOBS)
    try:
        jobs._JOBS.clear()
        j = jobs.create_job("effectcraft", ["run"], False)
        j.inspect = {"name": "ProbeComp", "width": 320}
        j.result_handle = "C:/tmp/out.ecproj.spill.json"   # 句柄必须跟着落盘
        path = str(tmp_path / "jobs.json")

        jobs.persist_jobs(path)
        assert os.path.isfile(path)

        jobs._JOBS.clear()
        jobs.restore_jobs(path)
        restored = jobs.get_job(j.job_id)
        assert restored is not None
        assert restored.inspect == {"name": "ProbeComp", "width": 320}
        assert restored.result_handle == "C:/tmp/out.ecproj.spill.json"
    finally:
        jobs._JOBS.clear()
        jobs._JOBS.update(saved)


def test_restore_ignores_non_dict_inspect(impl, tmp_path):
    """边界：磁盘上的 inspect 不是对象 -> 恢复为 None，不把坏数据带进内存。"""
    jobs = impl("jobs")
    saved = dict(jobs._JOBS)
    try:
        jobs._JOBS.clear()
        j = jobs.create_job("effectcraft", ["run"], False)
        path = tmp_path / "jobs.json"
        path.write_text(json.dumps([{
            "job_id": j.job_id, "app_id": j.app_id, "status": "succeeded",
            "inspect": "not-a-dict",
        }]), encoding="utf-8")

        jobs.restore_jobs(str(path))
        restored = jobs.get_job(j.job_id)
        assert restored is not None
        assert restored.inspect is None
    finally:
        jobs._JOBS.clear()
        jobs._JOBS.update(saved)


def test_shutdown_terminates_all_handles(impl):
    """#15 正常：shutdown_all 后 _PROC_HANDLES=={} 且每个 Process.terminate 被调。"""
    jobs = impl("jobs")
    saved = dict(jobs._PROC_HANDLES)
    try:
        jobs._PROC_HANDLES.clear()
        p1, p2 = FakeProcess(), FakeProcess()
        jobs._PROC_HANDLES["j1"] = fake_handle("j1", p1)
        jobs._PROC_HANDLES["j2"] = fake_handle("j2", p2)

        asyncio.run(jobs.shutdown_all())

        assert jobs._PROC_HANDLES == {}
        assert p1.terminate_calls >= 1
        assert p2.terminate_calls >= 1
    finally:
        jobs._PROC_HANDLES.clear()
        jobs._PROC_HANDLES.update(saved)


# ---------------------------------------------------------------------------
# 边界
# ---------------------------------------------------------------------------
def test_progress_throttle(impl):
    """#12 正常：fake clock 每步 +2s 且 +1% 循环 100 次，push 次数 = 跨 10% 次数（11~12）。

    §7 公式：``crossed_ten = (new_pct//10) > (last_progress_pct//10)``
    且 ``now - last_push_at >= 2.0s``。0/10/20/.../100 共 11 个跨十边界，
    加上可能的状态变化推送 -> 允许 11~12。
    """
    jobs = impl("jobs")
    spec = impl("spec")
    job = spec.JobState(
        job_id="j-throttle", app_id="photocraft", status="running",
        last_push_at=0.0, last_progress_pct=-1,
    )
    now = 0.0
    pushes = 0
    for pct in range(101):
        if jobs.should_push(job, now, pct):
            pushes += 1
            job.last_push_at = now
            job.last_progress_pct = pct
        now += 2.0                       # 每步 2s，满足 PUSH_MIN_INTERVAL_S
    assert 11 <= pushes <= 12, f"节流后 push 次数异常: {pushes}"


def test_progress_throttle_suppresses_fast_updates(impl):
    """#12 边界：每步只 +0.02s（100 步共 2s）时，跨 10% 也必须被 2s 间隔抑制。"""
    jobs = impl("jobs")
    spec = impl("spec")
    job = spec.JobState(
        job_id="j-fast", app_id="photocraft", status="running",
        last_push_at=0.0, last_progress_pct=0,
    )
    now = 0.0
    pushes = 0
    for pct in range(101):
        if jobs.should_push(job, now, pct):
            pushes += 1
            job.last_push_at = now
            job.last_progress_pct = pct
        now += 0.02
    assert pushes <= 3, f"密集更新未被节流: {pushes}"


def test_push_payload_512kib(impl, tmp_path):
    """#21 边界：摘要 >512 KiB 时只发摘要 + 产物路径，整条 payload 不超 512 KiB。"""
    jobs = impl("jobs")
    spec = impl("spec")
    huge = "x" * (PUSH_MAX_BYTES + 4096)
    job = spec.JobState(
        job_id="j-big", app_id="photocraft", status="succeeded",
        progress_label=huge, result_path="probe/e2e_layers.png",
    )
    ctx = FakeCtx(storage_dir=tmp_path, plugin_dir=PLUGIN_ROOT)

    asyncio.run(jobs.push_progress(job, ctx))

    assert ctx.pushes, "终态必须推送至少一条进度/结果"
    payload = json.dumps(ctx.pushes[-1], default=str, ensure_ascii=False)
    assert len(payload.encode("utf-8")) <= PUSH_MAX_BYTES
    assert huge not in payload
    assert "e2e_layers.png" in payload or "result_path" in payload


def test_edit_sequence_classification_blocks_retry(impl):
    """#13 边界（纯逻辑层）：含任一编辑命令即整体判编辑类 -> 绝不进入重发分支。"""
    sequence = impl("sequence")
    spec = impl("app_registry").get_spec("photocraft")
    jobs = impl("jobs")
    steps = ["layer.new.layer", "edit.fill", "document.inspect"]
    is_edit = any(not sequence.is_readonly(c, spec) for c in steps)

    assert is_edit is True
    state = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=is_edit)
    assert state.is_edit_sequence is True
    assert state.retried_readonly is False


def test_readonly_sequence_classification_allows_retry_gate(impl):
    """#14 边界（纯逻辑层）：全只读序列 is_edit_sequence=False，retry 门初始关闭。

    [INFERENCE / 规格冲突] §4 与 §7 尾部注明「v1 CLI_RUN 不触发只读重发，仅预留
    bridge 通道生效」，而 §10 #14 期望「重发一次」。本测试只锁定**分类决策**这一
    确定性部分；实际 respawn 行为见 test_entries_e2e.py，冲突已上报主理人裁决。
    """
    sequence = impl("sequence")
    spec = impl("app_registry").get_spec("photocraft")
    jobs = impl("jobs")
    steps = ["document.inspect", "session.list", "jobs.list"]
    is_edit = any(not sequence.is_readonly(c, spec) for c in steps)

    assert is_edit is False
    state = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=is_edit)
    assert state.is_edit_sequence is False
    assert state.retried_readonly is False


# ---------------------------------------------------------------------------
# 错误输入
# ---------------------------------------------------------------------------
def test_cancel_terminal_job_keeps_status(impl):
    """错误/边界：已成功的任务再取消 -> 状态不变，不覆盖结果。"""
    jobs = impl("jobs")
    j = jobs.create_job("photocraft", ["cli", "run"], False)
    jobs._finish(j, jobs.JobStatus.SUCCEEDED, 0)
    got = asyncio.run(jobs.cancel_job(j.job_id))
    assert got.status == "succeeded"
    assert j.error_code == ""


def test_cancel_before_spawn_goes_cancelled(impl):
    """#11 错误/边界：spawn 前到达的 cancel -> QUEUED→CANCELLED 终态。"""
    jobs = impl("jobs")
    j = jobs.create_job("photocraft", ["cli", "run"], False)
    got = asyncio.run(jobs.cancel_job(j.job_id))
    assert got.status == "cancelled"


def test_cancel_during_run_stays_cancelled(impl, monkeypatch):
    """运行中取消：被杀 CLI 的退出码不得把 CANCELLED 覆盖成 CLI_TASK_FAILED。"""
    jobs = impl("jobs")

    class BlockingProc:
        def __init__(self) -> None:
            self.pid = 424243
            self.returncode = None
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()

        def terminate(self) -> None:
            self.returncode = -15
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        def kill(self) -> None:
            self.terminate()

        async def wait(self) -> int:
            return int(self.returncode or 0)

    async def fake_exec(*args, **kwargs):
        return BlockingProc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec, raising=False)
    job = jobs.create_job("photocraft", ["cli", "run"], is_edit_sequence=True)
    job.step_total = 40

    async def scenario():
        task = asyncio.create_task(jobs.execute(job))
        for _ in range(100):
            await asyncio.sleep(0)
            if jobs._PROC_HANDLES.get(job.job_id):
                break
        assert jobs.status_value(job) == "running"
        await jobs.cancel_job(job.job_id)
        return await task

    result = asyncio.run(scenario())
    assert jobs.status_value(job) == "cancelled"
    assert job.error_code == "JOB_CANCELLED"
    assert result.error_code == "JOB_CANCELLED"


def test_get_job_unknown_returns_none(impl):
    """错误：未知 job_id 返回 None（不臆造状态，供入口转 JOB_NOT_FOUND）。"""
    jobs = impl("jobs")
    assert jobs.get_job("no-such-job-9999") is None


# ---------------------------------------------------------------------------
# v1.0.13：并发上限、每 app 互斥、终态先到先赢、回执只一次
# ---------------------------------------------------------------------------
async def _wait_until(cond, what: str = "", limit: int = 20000) -> None:
    """只让出事件循环、不睡固定时长；超上限直接判失败（防用例卡死整轮）。"""
    for _ in range(limit):
        if cond():
            return
        await asyncio.sleep(0)
    raise AssertionError(f"条件未达成：{what or 'cond'}")


async def _await_task(task, what: str = ""):
    await _wait_until(lambda: task.done(), what or "task not done")
    return await task


class _BlockingProc:
    """stdout/stderr 永不 EOF，直到 release()：用来把 execute 钉在「运行中」。"""

    def __init__(self) -> None:
        self.pid = 424244
        self.returncode = None
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.terminate_calls = 0

    def release(self) -> None:
        self.returncode = 0
        self.stdout.feed_eof()
        self.stderr.feed_eof()

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.release()

    def kill(self) -> None:
        self.terminate()

    async def wait(self) -> int:
        return int(self.returncode or 0)


def _blocking_exec(monkeypatch, bucket: list):
    async def fake_exec(*args, **kwargs):
        proc = _BlockingProc()
        bucket.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec, raising=False)
    return fake_exec


def test_same_app_jobs_run_one_at_a_time(impl, monkeypatch):
    """同一 app 的两个任务不得并行改同一份文档：第二个必须等第一个结束。"""
    jobs = impl("jobs")
    j1 = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
    procs: list = []
    _blocking_exec(monkeypatch, procs)
    seen: dict = {}
    holder: dict = {}

    async def scenario():
        t1 = asyncio.create_task(jobs.execute(j1))
        await _wait_until(lambda: bool(jobs._PROC_HANDLES.get(j1.job_id)), "第一个任务未 spawn")
        seen["after_first"] = (jobs.status_value(j1), len(procs))
        j2 = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
        holder["j2"] = j2
        t2 = asyncio.create_task(jobs.execute(j2))
        for _ in range(200):          # 给循环机会：第二个任务仍不该 spawn
            await asyncio.sleep(0)
        seen["queued"] = (jobs.status_value(j1), jobs.status_value(j2), len(procs))
        procs[0].release()
        await _await_task(t1, "第一个任务未在释放后结束")
        await _wait_until(lambda: jobs.status_value(j2) == "running", "第二个任务未被放行")
        seen["promoted"] = (jobs.status_value(j2), len(procs))
        procs[1].release()
        await _await_task(t2, "第二个任务未在释放后结束")
        seen["final"] = (jobs.status_value(j1), jobs.status_value(j2))
        return seen

    out = asyncio.run(scenario())
    assert out["after_first"] == ("running", 1), out
    assert out["queued"] == ("running", "queued", 1), f"第二个任务不得抢先 spawn：{out}"
    assert out["promoted"] == ("running", 2), out
    assert out["final"] == ("succeeded", "succeeded"), out


def test_global_concurrency_cap(impl, monkeypatch):
    """不同 app 的任务也受全局上限约束：同时 RUNNING 不超过 JOB_MAX_CONCURRENCY。"""
    jobs = impl("jobs")
    assert jobs.JOB_MAX_CONCURRENCY == 2
    procs: list = []
    _blocking_exec(monkeypatch, procs)
    made: list = []

    async def scenario():
        for app_id in ("photocraft", "vectorcraft", "pdfcraft"):
            made.append(jobs.create_job(app_id, ("cli", "run"), is_edit_sequence=True))
        tasks = [asyncio.create_task(jobs.execute(job)) for job in made]
        await _wait_until(
            lambda: len(procs) >= jobs.JOB_MAX_CONCURRENCY,
            "全局上限内没有起满",
        )
        running = [jobs.status_value(job) for job in made]
        waiting = len(procs)
        for _ in range(20000):
            for proc in procs:
                proc.release()
            if all(task.done() for task in tasks):
                break
            await asyncio.sleep(0)
        await asyncio.gather(*tasks)
        return running, waiting

    running, waiting = asyncio.run(scenario())
    assert running.count("running") == jobs.JOB_MAX_CONCURRENCY, running
    assert waiting == jobs.JOB_MAX_CONCURRENCY, f"第 3 个任务不得在限额内 spawn：{waiting}"
    assert all(jobs.status_value(job) == "succeeded" for job in made), [
        jobs.status_value(job) for job in made
    ]


def test_cancel_while_queued_does_not_run(impl, monkeypatch):
    """排队中被取消：进入 CANCELLED，绝不 spawn，也不得被回写成成功。"""
    jobs = impl("jobs")
    j1 = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
    procs: list = []
    _blocking_exec(monkeypatch, procs)
    outcome: dict = {}

    async def scenario():
        t1 = asyncio.create_task(jobs.execute(j1))
        await _wait_until(lambda: bool(jobs._PROC_HANDLES.get(j1.job_id)), "第一个任务未 spawn")
        j2 = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
        t2 = asyncio.create_task(jobs.execute(j2))
        await _wait_until(lambda: jobs.status_value(j2) == "queued", "第二个任务未处于排队")
        await jobs.cancel_job(j2.job_id)
        outcome["after_cancel"] = jobs.status_value(j2)
        # 排队任务卡在 app lock 上：等前一个跑完放行后才能看到自己已被取消
        procs[0].release()
        await _await_task(t1, "第一个任务未在释放后结束")
        res2 = await _await_task(t2, "被取消的排队任务未返回")
        outcome["final_status"] = jobs.status_value(j2)
        outcome["final_error"] = res2.error_code
        outcome["spawn_count"] = len(procs)

    asyncio.run(scenario())
    assert outcome["after_cancel"] == "cancelled", outcome
    assert outcome["final_status"] == "cancelled", outcome
    assert outcome["final_error"] == "JOB_CANCELLED", outcome
    assert outcome["spawn_count"] == 1, f"排队任务被取消后不得 spawn：{outcome}"


def test_first_terminal_status_wins(impl):
    """已有终态不得被后到的 _finish 覆盖（取消成功后 CLI 退出码不能翻成 succeeded）。"""
    jobs = impl("jobs")
    job = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
    asyncio.run(jobs.cancel_job(job.job_id))
    jobs._finish(job, jobs.JobStatus.SUCCEEDED, 0)
    assert jobs.status_value(job) == "cancelled"
    assert job.exit_code == -1


def test_crash_finishes_once(impl):
    """_crash 走「先到先赢」的 _finish 之后，仍必须写 finished_at。"""
    jobs = impl("jobs")
    job = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
    jobs._crash(job, RuntimeError("boom"))
    assert jobs.status_value(job) == "crashed"
    assert job.error_code == "JOB_CRASHED"
    assert job.finished_at > 0.0
    assert job.updated_at > 0.0


def test_completion_reported_only_once(impl, tmp_path):
    """execute 与 cancel_job 都回执时，消息面只收到一条完成。"""
    jobs = impl("jobs")
    ctx = FakeCtx(storage_dir=tmp_path, plugin_dir=PLUGIN_ROOT)
    job = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
    jobs._finish(job, jobs.JobStatus.SUCCEEDED, 0)

    async def scenario():
        await jobs.report_completion(job, None, ctx)
        await jobs.report_completion(job, None, ctx)

    asyncio.run(scenario())
    assert len(ctx.pushes) == 1, ctx.pushes
    assert len(ctx.statuses) == 1, ctx.statuses


def test_persist_keeps_preview_and_output_files(impl, tmp_path):
    """preview_url / output_files 是功能字段：重启后必须还在。"""
    jobs = impl("jobs")
    saved = dict(jobs._JOBS)
    try:
        jobs._JOBS.clear()
        job = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)
        job.output_files = ("C:/state/data/out.png",)
        job.preview_url = "stub://uploaded"
        job.completion_reported = True
        jobs._finish(job, jobs.JobStatus.SUCCEEDED, 0)
        path = str(tmp_path / "jobs.json")
        jobs.persist_jobs(path)
        jobs._JOBS.clear()
        jobs.restore_jobs(path)
        restored = jobs.get_job(job.job_id)
        assert restored is not None
        assert tuple(restored.output_files) == ("C:/state/data/out.png",)
        assert restored.preview_url == "stub://uploaded"
        assert restored.completion_reported is True
    finally:
        jobs._JOBS.clear()
        jobs._JOBS.update(saved)


def test_execute_materializes_preview_before_terminal(impl, monkeypatch, tmp_path):
    """终态前必须 materialize：否则 preview_url 永远是空串（图片回传整条链路是死的）。"""
    jobs = impl("jobs")
    procs: list = []
    _blocking_exec(monkeypatch, procs)
    job = jobs.create_job("photocraft", ("cli", "run"), is_edit_sequence=True)

    png = tmp_path / "out.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 40)
    job.result_path = str(png)
    ctx = FakeCtx(storage_dir=tmp_path, plugin_dir=PLUGIN_ROOT)

    async def scenario():
        task = asyncio.create_task(jobs.execute(job, ctx=ctx))
        await _wait_until(lambda: bool(jobs._PROC_HANDLES.get(job.job_id)), "未 spawn")
        procs[0].release()
        return await _await_task(task, "未结束")

    final = asyncio.run(scenario())
    assert jobs.status_value(job) == "succeeded", jobs.status_value(job)
    assert tuple(job.output_files) == (str(png),), job.output_files
    assert job.preview_url == "stub://uploaded", job.preview_url
    assert final.preview_url == "stub://uploaded"
