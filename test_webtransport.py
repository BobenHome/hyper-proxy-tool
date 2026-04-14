#!/usr/bin/env python3
"""
WebTransport over HTTP/3 测试脚本
使用 uv 管理依赖，aioquic 发送 Extended CONNECT 请求并测试 bidirectional stream

用法:
    uv run test_webtransport.py
    # 或
    .venv/bin/python test_webtransport.py

环境变量:
    SERVER_URL - 服务器地址，默认 https://127.0.0.1:8443
"""

import asyncio
import os
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import (
    DataReceived,
    HeadersReceived,
)
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

_raw_server_url = os.environ.get("SERVER_URL", "https://127.0.0.1:8443")
# localhost 可能优先解析到 IPv6 ::1，但 hyper-proxy-tool 的 QUIC 只监听 IPv4，
# 因此将 localhost 替换为 127.0.0.1 确保 QUIC 连接成功。
SERVER_URL = _raw_server_url.replace("://localhost:", "://127.0.0.1:")
TEST_PATH = "/api/public/webtransport"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))


class H3ClientProtocol(QuicConnectionProtocol):
    """QUIC protocol wrapper that feeds events into an H3Connection.
    Also captures raw WebTransport stream data directly from QUIC events."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic, enable_webtransport=True)
        self._http_events = asyncio.Queue()
        self._wt_data_events: dict[int, asyncio.Queue[bytes]] = {}
        self._wt_stream_ended: dict[int, bool] = {}

    def quic_event_received(self, event: QuicEvent) -> None:
        # Intercept WebTransport stream data before H3Connection misparses it
        from aioquic.quic.events import StreamDataReceived

        if isinstance(event, StreamDataReceived):
            if event.stream_id in self._wt_data_events:
                self._wt_data_events[event.stream_id].put_nowait(event.data)
                if event.end_stream:
                    self._wt_stream_ended[event.stream_id] = True
                return
        for http_event in self._http.handle_event(event):
            self._http_events.put_nowait(http_event)

    async def wait_for_event(self, timeout: float = 10.0):
        return await asyncio.wait_for(self._http_events.get(), timeout)

    def register_wt_stream(self, stream_id: int) -> None:
        self._wt_data_events[stream_id] = asyncio.Queue()

    async def read_wt_stream(
        self, stream_id: int, timeout: float = 5.0
    ) -> bytes | None:
        queue = self._wt_data_events.get(stream_id)
        if queue is None:
            return None
        chunks = []
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                remaining = max(0.05, deadline - asyncio.get_event_loop().time())
                chunk = await asyncio.wait_for(queue.get(), remaining)
                chunks.append(chunk)
                if self._wt_stream_ended.get(stream_id):
                    break
            except asyncio.TimeoutError:
                break
        return b"".join(chunks) if chunks else None


def is_server_running() -> bool:
    """检查服务器 /health 端点是否可访问"""
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(f"{SERVER_URL}/health", method="HEAD")
        with urllib.request.urlopen(req, context=ctx, timeout=2) as resp:
            return resp.status < 400
    except Exception:
        return False


def start_server() -> subprocess.Popen:
    """启动 hyper-proxy-tool 服务器（release 模式）"""
    print("[INFO] 服务器未运行，尝试启动...")
    proc = subprocess.Popen(
        ["./target/release/hyper-proxy-tool", "--config", "config.toml"],
        stdout=open("/tmp/hyper-proxy-tool-wt.log", "w"),
        stderr=subprocess.STDOUT,
        cwd=PROJECT_DIR,
    )

    for i in range(30):
        if is_server_running():
            print(f"[PASS] 服务器启动成功 (PID: {proc.pid})")
            return proc
        if proc.poll() is not None:
            print("[FAIL] 服务器启动失败")
            with open("/tmp/hyper-proxy-tool-wt.log") as f:
                print(f.read())
            sys.exit(1)
        time.sleep(1)
        print(f"[INFO] 等待服务器启动... ({i + 1}/30)")

    print("[FAIL] 服务器启动超时")
    proc.kill()
    sys.exit(1)


async def test_webtransport() -> bool:
    """测试 WebTransport 连接和 bidirectional stream"""
    parsed = urllib.parse.urlparse(SERVER_URL)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8443

    config = QuicConfiguration(alpn_protocols=H3_ALPN, is_client=True)
    config.verify_mode = False

    print(f"[INFO] 连接 WebTransport: {host}:{port}{TEST_PATH}")

    async with connect(
        host, port, configuration=config, create_protocol=H3ClientProtocol
    ) as protocol:
        assert isinstance(protocol, H3ClientProtocol)
        print("[PASS] QUIC 连接建立成功")

        h3_conn = protocol._http

        # 发送 Extended CONNECT 请求建立 WebTransport session
        stream_id = protocol._quic.get_next_available_stream_id()
        h3_conn.send_headers(
            stream_id=stream_id,
            headers=[
                (b":method", b"CONNECT"),
                (b":scheme", b"https"),
                (b":authority", f"{host}:{port}".encode()),
                (b":path", TEST_PATH.encode()),
                (b":protocol", b"webtransport"),
            ],
        )
        protocol.transmit()

        # 等待响应头
        response_status = None
        while response_status is None:
            event = await protocol.wait_for_event()
            if isinstance(event, HeadersReceived):
                headers = dict(event.headers)
                status_bytes = headers.get(b":status")
                response_status = int(status_bytes) if status_bytes else None
                print(f"[INFO] 收到响应: HTTP {response_status}")
            elif isinstance(event, DataReceived):
                print(f"[INFO] 收到数据: {event.data!r}")

        if response_status != 200:
            print(f"[FAIL] WebTransport 握手失败，状态码: {response_status}")
            return False

        print("[PASS] WebTransport session 建立成功")

        # 创建 bidirectional stream 并发送数据
        wt_stream_id = h3_conn.create_webtransport_stream(session_id=stream_id)
        protocol.register_wt_stream(wt_stream_id)
        test_message = b"Hello WebTransport!"
        # WebTransport stream 数据直接通过 QUIC 发送，不经过 H3Connection.send_data
        protocol._quic.send_stream_data(wt_stream_id, test_message)
        protocol.transmit()
        print(f"[INFO] 发送数据: {test_message.decode()}")

        # 等待 echo 回显（直接从 QUIC 事件读取，绕过 H3Connection 的帧解析）
        received = await protocol.read_wt_stream(wt_stream_id, timeout=5.0)
        if received == test_message:
            print(f"[INFO] 收到回显: {received.decode()}")
            print("[PASS] Bidirectional stream echo 测试通过")
        else:
            print(f"[FAIL] 未收到预期的 echo 回显，收到: {received!r}")
            return False

        # 关闭 stream
        protocol._quic.send_stream_data(wt_stream_id, b"", end_stream=True)
        protocol.transmit()

    print("[PASS] WebTransport 连接正常关闭")
    return True


async def main() -> None:
    proc = None
    try:
        if not is_server_running():
            proc = start_server()

        success = await test_webtransport()
        sys.exit(0 if success else 1)
    finally:
        if proc is not None:
            print("[INFO] 停止服务器...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
