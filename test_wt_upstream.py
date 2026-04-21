#!/usr/bin/env python3
"""
本地 WebTransport Echo 上游服务
用于测试 hyper-proxy-tool 的 WebTransport 代理功能

用法:
    # 终端 1: 启动 WT 上游
    uv run python test_wt_upstream.py

    # 终端 2: 启动代理 (配置指向本地 WT 上游)
    cargo run

    # 终端 3: 运行测试客户端
    uv run python test_webtransport.py
"""

import asyncio
import os

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.asyncio.server import serve
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import (
    DataReceived,
    HeadersReceived,
    WebTransportStreamDataReceived,
)
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived

HOST = os.environ.get("WT_HOST", "127.0.0.1")
PORT = int(os.environ.get("WT_PORT", "9443"))
CERT_FILE = "cert.pem"
KEY_FILE = "key.pem"


class H3ServerProtocol(QuicConnectionProtocol):
    """HTTP/3 + WebTransport 服务端协议"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic, enable_webtransport=True)
        self._wt_sessions: set[int] = set()

    def quic_event_received(self, event: QuicEvent) -> None:
        # 先让 H3Connection 处理 HTTP/3 事件
        http_events = self._http.handle_event(event)
        for http_event in http_events:
            self._handle_http_event(http_event)

        # 额外处理：如果是 WebTransport stream 的原始 QUIC 数据，直接 echo
        # 注意：H3Connection 已经处理了帧头，这里的数据是纯 payload
        if isinstance(event, StreamDataReceived):
            # 检查这个 stream 是否是已知的 WebTransport stream
            # 我们通过 H3Connection 的事件来跟踪
            pass

    def _handle_http_event(self, event) -> None:
        if isinstance(event, HeadersReceived):
            headers = dict(event.headers)
            method = headers.get(b":method")
            protocol = headers.get(b":protocol")
            stream_id = event.stream_id

            if method == b"CONNECT" and protocol == b"webtransport":
                print(f"[WT] 接受 WebTransport session, stream_id={stream_id}")
                self._wt_sessions.add(stream_id)

                self._http.send_headers(
                    stream_id=stream_id,
                    headers=[
                        (b":status", b"200"),
                        (b"sec-webtransport-http3-draft", b"draft02"),
                    ],
                )
                self.transmit()
            else:
                self._http.send_headers(
                    stream_id=stream_id,
                    headers=[(b":status", b"404")],
                )
                self.transmit()

        elif isinstance(event, WebTransportStreamDataReceived):
            # WebTransport stream 数据 - 直接 echo
            print(
                f"[WT] 收到 stream {event.stream_id} 数据: {event.data!r} "
                f"(session={event.session_id})"
            )
            # 直接通过 QUIC 层回写（不经过 H3Connection 帧封装）
            self._quic.send_stream_data(event.stream_id, event.data)
            if event.stream_ended:
                self._quic.send_stream_data(event.stream_id, b"", end_stream=True)
            self.transmit()

        elif isinstance(event, DataReceived):
            # session 流上的数据
            pass


async def main() -> None:
    if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
        print(f"[ERROR] 需要证书文件: {CERT_FILE} 和 {KEY_FILE}")
        print(
            "请运行: openssl req -x509 -newkey rsa:4096 "
            "-keyout key.pem -out cert.pem -days 365 -nodes -subj '/CN=localhost'"
        )
        return

    configuration = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=False,
        max_datagram_frame_size=65536,
    )
    configuration.load_cert_chain(CERT_FILE, KEY_FILE)

    print(f"[INFO] WebTransport Echo 上游服务启动: https://{HOST}:{PORT}")
    print("[INFO] 等待 WebTransport 连接...")

    await serve(
        HOST,
        PORT,
        configuration=configuration,
        create_protocol=H3ServerProtocol,
    )

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
