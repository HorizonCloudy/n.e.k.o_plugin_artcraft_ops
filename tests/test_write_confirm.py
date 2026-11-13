"""写操作二次确认：默认开启、令牌一次性、指纹绑定本次请求。

对应深评缺口 ①（require_confirm_on_write 默认 False 且代码里从未真正使用）。
入口一律走 plugin.plugins.artcraft_ops（真实 __init__.py），子模块从 module 属性取，
避免与 impl("jobs") 拿到两份不同的全局状态。
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from _fixtures import FakeCtx, FakeProcess

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
WRITE_STEPS = [{"cmd": "edit.fill", "params": {"color": "#fff"}}]
READONLY_STEPS = [{"cmd": "document.inspect"}]


def _load(load_plugin_init, tmp_path):
    module = load_plugin_init()
    from plugin.sdk.plugin import NekoPluginBase

    classes = [
        obj for obj in vars(module).values()
        if isinstance(obj, type) and issubclass(obj, NekoPluginBase)
        and obj is not NekoPluginBase
    ]
    ctx = FakeCtx(storage_dir=tmp_path, plugin_dir=PLUGIN_ROOT)
    return module, classes[0](ctx), ctx


def _no_spawn(monkeypatch, calls: list) -> None:
    async def fake_exec(*args, **kwargs):
        calls.append(tuple(args))
        return FakeProcess(stdout_lines=(), returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec, raising=False)


def test_write_request_requires_confirmation_first(load_plugin_init, tmp_path, monkeypatch):
    """首次写请求：不建 job、不 spawn，只回 confirmation_required + 一次性令牌。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)
    jobs_mod = module.jobs
    before = len(jobs_mod._JOBS)

    res = asyncio.run(fn(app="photocraft", steps=WRITE_STEPS, out_file="out.png", _ctx=ctx))

    payload = getattr(res, "value", None)
    assert isinstance(payload, dict), f"期望 Ok(dict)，实际 {res!r}"
    assert payload.get("confirmation_required") is True, payload
    assert payload.get("job_id") is None or not payload.get("job_id"), payload
    assert isinstance(payload.get("confirmation_handle"), str)
    assert payload["confirmation_handle"], "令牌必须非空"
    assert payload["confirmation_expires_in_s"] == module.confirm.CONFIRM_TTL_S
    assert calls == [], "确认之前绝不能 spawn 子进程"
    assert len(jobs_mod._JOBS) == before, "确认之前不得建 job"


def test_confirmed_request_creates_job(load_plugin_init, tmp_path, monkeypatch):
    """第二次带 confirmed + 令牌：正常建 job 并秒回 queued。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)

    async def scenario():
        first = await fn(app="photocraft", steps=WRITE_STEPS, out_file="out.png", _ctx=ctx)
        token = first.value["confirmation_handle"]
        return await fn(
            app="photocraft", steps=WRITE_STEPS, out_file="out.png",
            confirmed=True, confirmation_handle=token, _ctx=ctx,
        )

    res = asyncio.run(scenario())
    payload = getattr(res, "value", None)
    assert isinstance(payload, dict) and payload.get("status") == "queued", payload
    assert payload.get("job_id")


def test_confirmation_handle_is_single_use(load_plugin_init, tmp_path, monkeypatch):
    """同一令牌用第二次必须拒绝（一次性），不得静默再跑一遍。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)

    async def scenario():
        first = await fn(app="photocraft", steps=WRITE_STEPS, out_file="out.png", _ctx=ctx)
        token = first.value["confirmation_handle"]
        ok_second = await fn(
            app="photocraft", steps=WRITE_STEPS, out_file="out.png",
            confirmed=True, confirmation_handle=token, _ctx=ctx,
        )
        third = await fn(
            app="photocraft", steps=WRITE_STEPS, out_file="out.png",
            confirmed=True, confirmation_handle=token, _ctx=ctx,
        )
        return ok_second, third

    second, third = asyncio.run(scenario())
    assert getattr(second, "value", {}).get("status") == "queued"
    assert isinstance(third, dict) and third.get("is_error") is True, \
        f"重复使用令牌必须回错误 envelope，实际 {third!r}"
    assert third["error"]["code"] == "POLICY_VIOLATION", third["error"]


def test_confirmation_handle_is_bound_to_the_request(load_plugin_init, tmp_path, monkeypatch):
    """令牌绑请求指纹：拿 A 的令牌提交 B（换了 out_file）必须被拒。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)

    async def scenario():
        first = await fn(app="photocraft", steps=WRITE_STEPS, out_file="a.png", _ctx=ctx)
        token = first.value["confirmation_handle"]
        return await fn(
            app="photocraft", steps=WRITE_STEPS, out_file="b.png",
            confirmed=True, confirmation_handle=token, _ctx=ctx,
        )

    res = asyncio.run(scenario())
    assert isinstance(res, dict) and res.get("is_error") is True, \
        f"令牌与请求不符必须回错误 envelope，实际 {res!r}"
    assert res["error"]["code"] == "POLICY_VIOLATION", res["error"]
    assert calls == []


def test_readonly_request_needs_no_confirmation(load_plugin_init, tmp_path, monkeypatch):
    """只读序列（无 out_file / new_doc）：直接建 job，不打扰用户。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)

    res = asyncio.run(fn(app="photocraft", steps=READONLY_STEPS, _ctx=ctx))
    payload = getattr(res, "value", None)
    assert isinstance(payload, dict) and payload.get("status") == "queued", payload
    assert "confirmation_required" not in payload


def test_new_doc_counts_as_write(load_plugin_init, tmp_path, monkeypatch):
    """new_doc 会落新文档：同样必须确认。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)

    res = asyncio.run(fn(
        app="photocraft", steps=READONLY_STEPS,
        new_doc={"width": 64, "height": 64}, _ctx=ctx,
    ))
    payload = getattr(res, "value", None)
    assert isinstance(payload, dict) and payload.get("confirmation_required") is True, payload


def test_confirmation_can_be_disabled(load_plugin_init, tmp_path, monkeypatch):
    """配置显式关闭时直接执行（owner 可选，不是默认）。"""
    module, app, ctx = _load(load_plugin_init, tmp_path)
    fn = getattr(app, "artcraft_ops_run_sequence")
    calls: list = []
    _no_spawn(monkeypatch, calls)
    roots_mod = module.roots_mod
    roots_mod.configure_security({"require_confirm_on_write": False})
    try:
        res = asyncio.run(fn(app="photocraft", steps=WRITE_STEPS, out_file="out.png", _ctx=ctx))
    finally:
        roots_mod.configure_security({})
    payload = getattr(res, "value", None)
    assert isinstance(payload, dict) and payload.get("status") == "queued", payload


def test_confirm_schema_and_manifest_fields(load_plugin_init):
    """RUN_SCHEMA 收 confirmed/confirmation_handle；出口字段进白名单与清单。"""
    module = load_plugin_init()
    schema = module.RUN_SCHEMA["properties"]
    assert "confirmed" in schema and "confirmation_handle" in schema
    allowed = module.spec.TOOL_OUTPUT_ALLOWLIST
    for field_name in ("confirmation_required", "confirmation_handle", "confirmation_expires_in_s"):
        assert field_name in allowed, field_name


def test_default_policy_is_confirm_on_write(impl):
    """默认值必须为 True（两处：CommandPolicy 与运行时配置默认表）。"""
    spec = impl("spec")
    config = impl("config")
    assert spec.CommandPolicy().require_confirm_on_write is True
    assert config._DEFAULT_SECURITY["require_confirm_on_write"] is True
