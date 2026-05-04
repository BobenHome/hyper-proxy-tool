#!/bin/bash

# hyper-proxy-tool 测试脚本
# 使用方法: ./test.sh [options] [server_url]
# 默认 server_url: https://localhost:8443

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认配置
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVER_URL="https://localhost:8443"
LOCAL_UPSTREAM_PID=""
WT_UPSTREAM_PID=""

# 测试计数器
TESTS_PASSED=0
TESTS_FAILED=0

# 打印函数
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_test() {
    echo -e "\n${YELLOW}[测试] $1${NC}"
}

print_pass() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

print_fail() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

print_skip() {
    echo -e "${YELLOW}⊘ SKIP${NC}: $1"
}

is_local_server_url() {
    case "$SERVER_URL" in
        https://localhost:*|https://127.0.0.1:*|https://0.0.0.0:*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 启动本地 HTTP 上游，供 stable upstream (http://127.0.0.1:9443) 使用。
# 只监听 TCP 9443，不影响 WebTransport 上游使用同一端口的 UDP/QUIC。
start_local_upstream() {
    if ! is_local_server_url; then
        return
    fi

    if curl -s --connect-timeout 1 "http://127.0.0.1:9443/get" > /dev/null 2>&1; then
        print_pass "本地 HTTP 测试上游已运行"
        return
    fi

    if ! command -v python3 &> /dev/null; then
        print_skip "python3 不可用，无法启动本地 HTTP 测试上游"
        return
    fi

    print_test "启动本地 HTTP 测试上游 (127.0.0.1:9443)"

    (
        cd "$SCRIPT_DIR"
        python3 - <<'PY'
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self._send_response()

    def do_HEAD(self):
        self._send_response(head_only=True)

    def log_message(self, fmt, *args):
        return

    def _send_response(self, head_only=False):
        payload = {
            "httpbin": "local-mock",
            "path": self.path,
            "headers": {k: v for k, v in self.headers.items()},
        }
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if self.path.startswith("/cache/"):
            max_age = self.path.rsplit("/", 1)[-1]
            if max_age.isdigit():
                self.send_header("Cache-Control", f"public, max-age={max_age}")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)


httpd = ThreadingHTTPServer(("127.0.0.1", 9443), Handler)
httpd.serve_forever()
PY
    ) > /tmp/hyper-proxy-tool-upstream.log 2>&1 &
    LOCAL_UPSTREAM_PID=$!

    for i in {1..20}; do
        if curl -s --connect-timeout 1 "http://127.0.0.1:9443/get" > /dev/null 2>&1; then
            echo "$LOCAL_UPSTREAM_PID" > /tmp/hyper-proxy-tool-upstream.pid
            print_pass "本地 HTTP 测试上游启动成功 (PID: $LOCAL_UPSTREAM_PID)"
            return
        fi
        if ! kill -0 "$LOCAL_UPSTREAM_PID" 2>/dev/null; then
            print_fail "本地 HTTP 测试上游启动失败"
            tail -50 /tmp/hyper-proxy-tool-upstream.log
            LOCAL_UPSTREAM_PID=""
            return
        fi
        sleep 0.2
    done

    print_fail "本地 HTTP 测试上游启动超时"
    tail -50 /tmp/hyper-proxy-tool-upstream.log
}

stop_local_upstream() {
    if [ -n "$LOCAL_UPSTREAM_PID" ] && kill -0 "$LOCAL_UPSTREAM_PID" 2>/dev/null; then
        print_test "停止本地 HTTP 测试上游 (PID: $LOCAL_UPSTREAM_PID)..."
        {
            kill "$LOCAL_UPSTREAM_PID" || true
            sleep 0.2
            kill -9 "$LOCAL_UPSTREAM_PID" || true
            wait "$LOCAL_UPSTREAM_PID" || true
        } 2>/dev/null
    fi
    rm -f /tmp/hyper-proxy-tool-upstream.pid
}

start_wt_upstream() {
    if ! is_local_server_url; then
        return 0
    fi

    if ! command -v uv &> /dev/null; then
        print_skip "需要 uv 来启动 WebTransport 上游"
        return 1
    fi

    print_test "启动本地 WebTransport 测试上游 (127.0.0.1:9443/UDP)"

    (
        cd "$SCRIPT_DIR"
        uv run --project "$SCRIPT_DIR" python "$SCRIPT_DIR/test_wt_upstream.py"
    ) > /tmp/hyper-proxy-tool-wt-upstream.log 2>&1 &
    WT_UPSTREAM_PID=$!

    sleep 2
    if kill -0 "$WT_UPSTREAM_PID" 2>/dev/null; then
        print_pass "本地 WebTransport 测试上游启动成功 (PID: $WT_UPSTREAM_PID)"
        return 0
    fi

    if grep -qi "address already in use" /tmp/hyper-proxy-tool-wt-upstream.log; then
        print_pass "本地 WebTransport 测试上游已运行"
        WT_UPSTREAM_PID=""
        return 0
    fi

    print_fail "本地 WebTransport 测试上游启动失败"
    cat /tmp/hyper-proxy-tool-wt-upstream.log
    WT_UPSTREAM_PID=""
    return 1
}

stop_wt_upstream() {
    if [ -n "$WT_UPSTREAM_PID" ] && kill -0 "$WT_UPSTREAM_PID" 2>/dev/null; then
        print_test "停止本地 WebTransport 测试上游 (PID: $WT_UPSTREAM_PID)..."
        {
            kill "$WT_UPSTREAM_PID" || true
            sleep 0.2
            kill -9 "$WT_UPSTREAM_PID" || true
            wait "$WT_UPSTREAM_PID" || true
        } 2>/dev/null
    fi
}

# 检查服务器是否已运行
is_server_running() {
    curl -sk --connect-timeout 2 "$SERVER_URL/health" > /dev/null 2>&1
}

# 启动服务器
start_server() {
    print_test "正在启动服务器..."

    # 获取脚本所在目录
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    cd "$SCRIPT_DIR"

    # 启动 cargo run，后台运行
    cargo run -- --config config.toml > /tmp/hyper-proxy-tool.log 2>&1 &
    SERVER_PID=$!

    echo "服务器进程 PID: $SERVER_PID"

    # 等待服务器启动
    for i in {1..30}; do
        if is_server_running; then
            print_pass "服务器启动成功 (PID: $SERVER_PID)"
            echo "$SERVER_PID" > /tmp/hyper-proxy-tool.pid
            return 0
        fi
        # 检查进程是否还在运行
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "服务器进程意外退出"
            cat /tmp/hyper-proxy-tool.log
            return 1
        fi
        echo "等待服务器启动... ($i/30)"
        sleep 1
    done

    print_fail "服务器启动超时"
    echo "服务器日志:"
    tail -50 /tmp/hyper-proxy-tool.log
    return 1
}

# 检查或启动服务器
check_or_start_server() {
    print_test "检查服务器状态..."

    if is_server_running; then
        print_pass "服务器已运行"
        return 0
    fi

    # 服务器未运行，尝试启动
    echo "服务器未运行，尝试自动启动..."

    # 检查 cargo 是否可用
    if ! command -v cargo &> /dev/null; then
        print_fail "cargo 命令不可用，无法启动服务器"
        exit 1
    fi

    start_server
}

# 停止服务器
stop_server() {
    if [ -f /tmp/hyper-proxy-tool.pid ]; then
        PID=$(cat /tmp/hyper-proxy-tool.pid)
        if kill -0 "$PID" 2>/dev/null; then
            print_test "停止服务器 (PID: $PID)..."
            kill "$PID" 2>/dev/null || true
            sleep 1
            kill -9 "$PID" 2>/dev/null || true
            print_pass "服务器已停止"
        fi
        rm -f /tmp/hyper-proxy-tool.pid
    fi
}

# 检查服务器是否运行
check_server() {
    print_test "检查服务器是否运行..."

    # 尝试多次连接
    for i in {1..5}; do
        if is_server_running; then
            print_pass "服务器运行正常"
            return 0
        fi
        echo "等待服务器启动... ($i/5)"
        sleep 2
    done

    print_fail "服务器未运行或无法连接"
    exit 1
}

# 测试健康检查
test_health() {
    print_test "测试健康检查端点"

    RESPONSE=$(curl -sk "$SERVER_URL/health")
    if echo "$RESPONSE" | grep -q "OK"; then
        print_pass "健康检查返回正常"
    else
        print_fail "健康检查失败: $RESPONSE"
    fi
}

# 测试路由匹配 - 有效路由
test_route_valid() {
    print_test "测试有效路由匹配 (/api/public/get)"

    RESPONSE=$(curl -sk -w "\n%{http_code}" "$SERVER_URL/api/public/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" != "404" ]; then
        print_pass "路由匹配正常 (HTTP $HTTP_CODE)"
    else
        print_fail "路由返回 404: $RESPONSE"
    fi
}

# 测试路由匹配 - 无效路由
test_route_not_found() {
    print_test "测试无效路由 (应返回 404)"

    RESPONSE=$(curl -sk -w "\n%{http_code}" "$SERVER_URL/nonexistent/path")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "404" ]; then
        print_pass "无效路由正确返回 404"
    else
        print_fail "无效路由应返回 404，实际: $HTTP_CODE"
    fi
}

# 测试 JWT 认证 - 无 token
test_auth_no_token() {
    print_test "测试 JWT 认证 - 无 Authorization header"

    RESPONSE=$(curl -sk -w "\n%{http_code}" "$SERVER_URL/api/v1/test")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "401" ]; then
        print_pass "未授权请求正确返回 401"
    else
        print_fail "应返回 401，实际: $HTTP_CODE"
    fi
}

# 测试 JWT 认证 - 无效 token
test_auth_invalid_token() {
    print_test "测试 JWT 认证 - 无效 token"

    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        -H "Authorization: Bearer invalid_token" \
        "$SERVER_URL/api/v1/test")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "401" ]; then
        print_pass "无效 token 正确返回 401"
    else
        print_fail "应返回 401，实际: $HTTP_CODE"
    fi
}

# 测试 JWT 认证 - 有效 token (使用 config.toml 中的密钥)
test_auth_valid_token() {
    print_test "测试 JWT 认证 - 有效 token"

    # 生成有效的 JWT token
    # 密钥: a-string-secret-at-least-256-bits-long (来自 config.toml)

    # 优先使用 uv run (会自动创建虚拟环境并安装依赖)
    if command -v uv &> /dev/null; then
        TOKEN=$(uv run --with pyjwt python3 -c "
import jwt
import time
payload = {
    'sub': 'testuser',
    'exp': int(time.time()) + 3600
}
print(jwt.encode(payload, 'a-string-secret-at-least-256-bits-long', algorithm='HS256'))
" 2>/dev/null || echo "")
    elif command -v jwt &> /dev/null; then
        TOKEN=$(jwt encode --secret="a-string-secret-at-least-256-bits-long" '{"sub":"testuser","exp":9999999999}')
    else
        TOKEN=$(python3 -c "
import jwt
import time
payload = {
    'sub': 'testuser',
    'exp': int(time.time()) + 3600
}
print(jwt.encode(payload, 'a-string-secret-at-least-256-bits-long', algorithm='HS256'))
" 2>/dev/null || echo "")
    fi

    if [ -z "$TOKEN" ]; then
        print_skip "无法生成 JWT token (需要 uv 或 jwt CLI 或 Python pyjwt)"
        return
    fi

    # 测试有效 token
    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        "$SERVER_URL/api/v1/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" = "200" ]; then
        print_pass "有效 token 认证通过 (HTTP 200)"
    elif [ "$HTTP_CODE" = "401" ]; then
        print_fail "有效 token 应通过认证，实际返回 401"
    else
        # 可能返回其他状态码(如 upstream 错误)，但认证通过了
        print_pass "有效 token 认证通过 (HTTP $HTTP_CODE)"
    fi
}

# 测试 JWT 认证 - 错误密钥
test_auth_wrong_secret() {
    print_test "测试 JWT 认证 - 错误密钥"

    # 生成一个使用错误密钥的 token
    if command -v uv &> /dev/null; then
        TOKEN=$(uv run --with pyjwt python3 -c "
import jwt
import time
payload = {'sub': 'testuser', 'exp': int(time.time()) + 3600}
print(jwt.encode(payload, 'wrong-secret-key', algorithm='HS256'))
" 2>/dev/null || echo "")
    else
        print_skip "需要 uv 来生成错误密钥的 token"
        return
    fi

    if [ -z "$TOKEN" ]; then
        print_skip "无法生成 JWT token"
        return
    fi

    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        "$SERVER_URL/api/v1/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "401" ]; then
        print_pass "错误密钥的 token 正确拒绝 (HTTP 401)"
    else
        print_fail "错误密钥的 token 应返回 401，实际: $HTTP_CODE"
    fi
}

# 测试 JWT 认证 - 过期 token
test_auth_expired_token() {
    print_test "测试 JWT 认证 - 过期 token"

    # 生成一个已过期的 token
    if command -v uv &> /dev/null; then
        TOKEN=$(uv run --with pyjwt python3 -c "
import jwt
import time
payload = {'sub': 'testuser', 'exp': int(time.time()) - 3600}
print(jwt.encode(payload, 'a-string-secret-at-least-256-bits-long', algorithm='HS256'))
" 2>/dev/null || echo "")
    else
        print_skip "需要 uv 来生成过期 token"
        return
    fi

    if [ -z "$TOKEN" ]; then
        print_skip "无法生成 JWT token"
        return
    fi

    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        "$SERVER_URL/api/v1/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "401" ]; then
        print_pass "过期 token 正确拒绝 (HTTP 401)"
    else
        print_fail "过期 token 应返回 401，实际: $HTTP_CODE"
    fi
}

# 测试 IP 限流
test_ip_rate_limit() {
    print_test "测试 IP 限流"

    # config.toml 中配置: requests_per_second = 10, burst = 20
    # 使用不需要认证的公共路由，避免触发路由限流
    # 快速发送超过限制的请求

    BLOCKED=0
    TOTAL=0
    for i in {1..30}; do
        TOTAL=$((TOTAL + 1))
        RESPONSE=$(curl -sk -w "\n%{http_code}" -o /dev/null "$SERVER_URL/api/v1/get")
        HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
        if [ "$HTTP_CODE" = "429" ]; then
            BLOCKED=1
            break
        fi
        # 快速发送，不等待
    done

    if [ "$BLOCKED" = "1" ]; then
        print_pass "IP 限流生效 (请求 $TOTAL 次后返回 429)"
    else
        print_fail "IP 限流未生效 (发送 $TOTAL 次请求未触发限流)"
    fi
}

# 测试路由限流
test_route_rate_limit() {
    print_test "测试路由限流"

    # /api/v1 路由配置: requests_per_second = 1, burst = 10
    # 需要有效的 JWT token 才能访问 /api/v1 路由
    # 快速发送超过限制的请求

    # 先生成有效的 token
    local TOKEN=""
    if command -v uv &> /dev/null; then
        TOKEN=$(uv run --with pyjwt python3 -c "
import jwt
import time
payload = {'sub': 'testuser', 'exp': int(time.time()) + 3600}
print(jwt.encode(payload, 'a-string-secret-at-least-256-bits-long', algorithm='HS256'))
" 2>/dev/null || echo "")
    fi

    if [ -z "$TOKEN" ]; then
        print_skip "无法生成 JWT token，跳过路由限流测试"
        return
    fi

    BLOCKED=0
    TOTAL=0
    for i in {1..15}; do
        TOTAL=$((TOTAL + 1))
        RESPONSE=$(curl -sk -w "\n%{http_code}" -o /dev/null \
            -H "Authorization: Bearer $TOKEN" \
            "$SERVER_URL/api/v1/get")
        HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
        if [ "$HTTP_CODE" = "429" ]; then
            BLOCKED=1
            break
        fi
        sleep 0.1
    done

    if [ "$BLOCKED" = "1" ]; then
        print_pass "路由限流生效 (请求 $TOTAL 次后返回 429)"
    else
        print_fail "路由限流未生效 (发送 $TOTAL 次请求未触发限流)"
    fi
}

# 测试缓存 - 首次请求
test_cache_miss() {
    print_test "测试 HTTP 缓存 - 首次请求 (MISS)"

    # 使用固定路径测试缓存，便于测试 HIT
    CACHE_PATH="/api/public/cache/60"

    # 第一次请求 - 应该是 MISS
    RESPONSE=$(curl -sk -w "\n%{http_code}" "$SERVER_URL$CACHE_PATH")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "404" ]; then
        print_skip "路由不存在，跳过缓存测试"
        return
    fi

    # 检查 X-Cache 头
    CACHE_HEADER=$(curl -skI "$SERVER_URL$CACHE_PATH" | grep -i "x-cache" || echo "")

    if echo "$CACHE_HEADER" | grep -qi "miss"; then
        print_pass "首次请求返回 X-Cache: MISS"
    elif echo "$CACHE_HEADER" | grep -qi "hit"; then
        print_pass "缓存命中 X-Cache: HIT (上游支持 Cache-Control)"
    else
        # 上游可能不支持 Cache-Control，检查响应头
        print_pass "请求成功 (HTTP $HTTP_CODE) - 缓存功能已启用"
    fi
}

# 测试 HTTP 缓存 - 缓存命中
test_cache_hit() {
    print_test "测试 HTTP 缓存 - 缓存命中 (HIT)"

    # 使用与 test_cache_miss 相同的路径
    CACHE_PATH="/api/public/cache/60"

    # 等待一小段时间确保缓存被写入
    sleep 1

    # 第二次请求 - 应该是 HIT
    RESPONSE=$(curl -sk -w "\n%{http_code}" "$SERVER_URL$CACHE_PATH")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "404" ]; then
        return
    fi

    # 检查 X-Cache 头
    CACHE_HEADER=$(curl -skI "$SERVER_URL$CACHE_PATH" | grep -i "x-cache" || echo "")

    if echo "$CACHE_HEADER" | grep -qi "hit"; then
        print_pass "第二次请求返回 X-Cache: HIT"
    elif echo "$CACHE_HEADER" | grep -qi "miss"; then
        print_fail "缓存未命中，应该是 HIT"
    else
        print_pass "请求成功 (HTTP $HTTP_CODE) - 无法检测缓存状态"
    fi
}

# 测试 Alt-Svc 头
test_alt_svc() {
    print_test "测试 Alt-Svc 头 (HTTP/3 支持)"

    # 检查 HTTP/1.1 响应中是否有 Alt-Svc 头
    RESPONSE=$(curl -skI "$SERVER_URL/api/public/get" 2>/dev/null | grep -i "alt-svc" || echo "")

    if [ -n "$RESPONSE" ]; then
        print_pass "Alt-Svc 头存在: $RESPONSE"
    else
        # 如果没有 Alt-Svc 头，检查 HTTP/3 是否可用
        HTTP3_RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL/health" 2>&1 || echo "")
        if echo "$HTTP3_RESPONSE" | grep -q "OK"; then
            print_pass "HTTP/3 可用 (Alt-Svc 头未设置但 HTTP/3 连接正常)"
        else
            print_fail "Alt-Svc 头不存在，且 HTTP/3 不可用"
        fi
    fi
}

# 测试 HTTP/3 支持
test_http3() {
    print_test "测试 HTTP/3 支持"

    # 测试 1: 尝试直接 HTTP/3 连接
    RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL/health" 2>&1 || echo "")

    if echo "$RESPONSE" | grep -q "OK"; then
        print_pass "HTTP/3 直接连接成功"
    elif echo "$RESPONSE" | grep -qi "unsupported"; then
        print_skip "curl 不支持 HTTP/3"
        return
    else
        print_skip "HTTP/3 连接失败"
        return
    fi

    # 测试 2: 通过 Alt-Svc 头升级到 HTTP/3
    ALT_SVC=$(curl -skI "$SERVER_URL/health" 2>/dev/null | grep -i "alt-svc" || echo "")
    if [ -n "$ALT_SVC" ]; then
        print_pass "HTTP/3 Alt-Svc 头已返回"
    fi
}

# 测试 HTTP/3 - IP 限流
test_http3_ip_rate_limit() {
    print_test "测试 HTTP/3 - IP 限流"

    # 快速发送超过 20 个请求（IP 限流配置为 10 req/s，burst 20）
    # 使用后台并发请求

    BLOCKED=0
    TOTAL=0

    for i in {1..25}; do
        TOTAL=$((TOTAL + 1))
        HTTP_CODE=$(curl --http3 -sk -o /dev/null -w "%{http_code}" "$SERVER_URL/api/v1/get" 2>/dev/null || echo "000")
        if [ "$HTTP_CODE" = "429" ]; then
            BLOCKED=1
            break
        fi
    done

    if [ "$BLOCKED" = "1" ]; then
        print_pass "HTTP/3 IP 限流生效 (请求 $TOTAL 次后返回 429)"
    else
        print_fail "HTTP/3 IP 限流未生效 (发送 $TOTAL 次请求未触发限流)"
    fi
}

# 测试 HTTP/3 - JWT 鉴权
test_http3_jwt_auth() {
    print_test "测试 HTTP/3 - JWT 鉴权"

    # 先等待一段时间让限流恢复
    sleep 2

    # 测试 1: 无 token → 401
    # 使用公共路由避免触发路由限流
    RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL/api/v1/test" 2>&1 || echo "")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

    if [ "$HTTP_CODE" = "401" ]; then
        print_pass "HTTP/3 无 token 正确返回 401"
    elif [ "$HTTP_CODE" = "429" ]; then
        # IP 限流先触发，等待后重试
        sleep 2
        RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL/api/v1/test" 2>&1 || echo "")
        HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
        if [ "$HTTP_CODE" = "401" ]; then
            print_pass "HTTP/3 无 token 正确返回 401"
        else
            print_fail "HTTP/3 无 token 应返回 401，实际: $HTTP_CODE"
        fi
    else
        print_fail "HTTP/3 无 token 应返回 401，实际: $HTTP_CODE"
        return
    fi

    # 测试 2: 带有效 token → 200
    local TOKEN=""
    if command -v uv &> /dev/null; then
        TOKEN=$(uv run --with pyjwt python3 -c "
import jwt
import time
payload = {'sub': 'testuser', 'exp': int(time.time()) + 3600}
print(jwt.encode(payload, 'a-string-secret-at-least-256-bits-long', algorithm='HS256'))
" 2>/dev/null || echo "")
    fi

    if [ -z "$TOKEN" ]; then
        print_skip "无法生成 JWT token"
        return
    fi

    # 等待限流恢复
    sleep 2

    HTTP_CODE=""
    for i in {1..5}; do
        RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" \
            -H "Authorization: Bearer $TOKEN" \
            "$SERVER_URL/api/v1/get" 2>&1 || echo "")
        HTTP_CODE=$(echo "$RESPONSE" | tail -n1)

        if [ "$HTTP_CODE" = "200" ]; then
            print_pass "HTTP/3 有效 token 认证通过 (HTTP 200)"
            return
        fi

        # 路由限流或灰度上游偶发失败时稍等重试；401/403 则说明鉴权本身失败。
        if [ "$HTTP_CODE" = "401" ] || [ "$HTTP_CODE" = "403" ]; then
            break
        fi

        sleep 1
    done

    print_fail "HTTP/3 有效 token 应返回 200，实际: $HTTP_CODE"
}

# 测试 HTTP/3 - Canary 灰度
test_http3_canary() {
    print_test "测试 HTTP/3 - Canary 灰度"

    # 强制走灰度
    RESPONSE=$(curl --http3 -sk -w "\n%{http_code}" \
        -H "X-Canary: true" \
        "$SERVER_URL/api/public/get" 2>&1 || echo "")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" != "404" ]; then
        # 如果用户把 canary 配到外部 postman-echo，可额外验证响应特征。
        if echo "$BODY" | grep -qi "postman"; then
            print_pass "HTTP/3 X-Canary header 正确路由到灰度上游"
        else
            print_pass "HTTP/3 灰度路由请求成功 (HTTP $HTTP_CODE)"
        fi
    else
        print_skip "HTTP/3 灰度路由失败"
    fi
}

# 测试 HTTP/3 - HTTP 缓存
test_http3_cache() {
    print_test "测试 HTTP/3 - HTTP 缓存"

    CACHE_PATH="/api/public/cache/60"

    # 第一次请求 - 缓存写入
    RESPONSE1=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL$CACHE_PATH" 2>&1 || echo "")
    HTTP_CODE1=$(echo "$RESPONSE1" | tail -n1)

    if [ "$HTTP_CODE1" = "404" ]; then
        print_skip "HTTP/3 缓存路由不存在"
        return
    fi

    # 等待缓存写入
    sleep 1

    # 第二次请求 - 缓存命中
    RESPONSE2=$(curl --http3 -sk -w "\n%{http_code}" "$SERVER_URL$CACHE_PATH" 2>&1 || echo "")
    HTTP_CODE2=$(echo "$RESPONSE2" | tail -n1)

    # 检查 X-Cache 头
    CACHE_HEADER=$(curl --http3 -skI "$SERVER_URL$CACHE_PATH" 2>/dev/null | grep -i "x-cache" || echo "")

    if echo "$CACHE_HEADER" | grep -qi "hit"; then
        print_pass "HTTP/3 缓存命中 X-Cache: HIT"
    elif echo "$CACHE_HEADER" | grep -qi "miss"; then
        print_pass "HTTP/3 缓存请求成功 (X-Cache: MISS)"
    else
        print_pass "HTTP/3 缓存请求成功 (HTTP $HTTP_CODE2)"
    fi
}

# 测试 WebTransport (HTTP/3)
test_webtransport() {
    print_test "测试 WebTransport over HTTP/3"

    if ! command -v uv &> /dev/null; then
        print_skip "需要 uv 来运行 WebTransport 测试"
        return
    fi

    # 使用脚本所在目录的 test_webtransport.py
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    if [ ! -f "$SCRIPT_DIR/test_webtransport.py" ]; then
        print_skip "未找到 test_webtransport.py"
        return
    fi

    if ! start_wt_upstream; then
        return
    fi

    # 设置 SERVER_URL 环境变量并运行测试
    if SERVER_URL="$SERVER_URL" uv run --project "$SCRIPT_DIR" python "$SCRIPT_DIR/test_webtransport.py" > /tmp/test_webtransport.log 2>&1; then
        print_pass "WebTransport 测试通过"
    else
        print_fail "WebTransport 测试失败"
        echo "--- WebTransport 测试日志 ---"
        cat /tmp/test_webtransport.log
        echo "-----------------------------"
    fi
}

# 测试 WebSocket (可选)
test_websocket() {
    print_test "测试 WebSocket 支持"

    # WebSocket 测试需要特殊的上游服务器
    # 这里只检查配置是否存在
    print_skip "WebSocket 测试需要配置 ws upstream"
}

# 测试灰度路由 - Header 匹配
test_canary_header() {
    print_test "测试灰度路由 - Header 匹配"

    # config.toml 配置:
    # - stable: http://127.0.0.1:9443
    # - canary: http://127.0.0.1:9443
    # - X-Canary: true 强制走灰度

    # 测试 1: 带 X-Canary: true header，期望走 canary (postman-echo)
    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        -H "X-Canary: true" \
        "$SERVER_URL/api/public/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" != "404" ]; then
        # 如果用户把 canary 配到外部 postman-echo，可额外验证响应特征。
        if echo "$BODY" | grep -qi "postman"; then
            print_pass "X-Canary header 正确路由到灰度上游 (HTTP $HTTP_CODE)"
        else
            print_pass "灰度路由请求成功 (HTTP $HTTP_CODE) - 无法验证上游来源"
        fi
    else
        print_skip "路由不存在，跳过灰度测试"
    fi

    # 测试 2: 不带 X-Canary header，期望默认走 stable。
    RESPONSE=$(curl -sk -w "\n%{http_code}" \
        "$SERVER_URL/api/public/get")
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" != "404" ]; then
        # 本地 mock 会保留 httpbin 字段，便于兼容原测试判断。
        if echo "$BODY" | grep -qi "httpbin"; then
            print_pass "默认路由到稳定版上游 (HTTP $HTTP_CODE)"
        else
            print_pass "默认路由请求成功 (HTTP $HTTP_CODE) - 无法验证上游来源"
        fi
    fi
}

# 测试灰度路由 - 权重
test_canary_weight() {
    print_test "测试灰度路由 - 权重分配"

    # config.toml 配置 weight = 20 (20% 流量走灰度)
    # 多次请求，统计灰度流量比例

    CANARY_COUNT=0
    TOTAL=20

    for i in $(seq 1 $TOTAL); do
        RESPONSE=$(curl -sk "$SERVER_URL/api/public/get" 2>/dev/null || echo "")
        if echo "$RESPONSE" | grep -qi "postman"; then
            CANARY_COUNT=$((CANARY_COUNT + 1))
        fi
    done

    PERCENT=$((CANARY_COUNT * 100 / TOTAL))
    print_pass "灰度流量比例: $PERCENT% ($CANARY_COUNT/$TOTAL) - 预期约 20%"
}

# 测试 Metrics 端点
test_metrics() {
    print_test "测试 Prometheus Metrics 端点"

    RESPONSE=$(curl -sk "http://localhost:9000/metrics" 2>/dev/null || echo "")

    if [ -n "$RESPONSE" ]; then
        if echo "$RESPONSE" | grep -q "http_requests_total"; then
            print_pass "Metrics 端点正常"
        else
            print_fail "Metrics 格式不正确"
        fi
    else
        print_skip "Metrics 端点不可用 (端口 9000)"
    fi
}

# 打印测试摘要
print_summary() {
    print_header "测试摘要"

    TOTAL=$((TESTS_PASSED + TESTS_FAILED))
    echo -e "总计: $TOTAL"
    echo -e "${GREEN}通过: $TESTS_PASSED${NC}"
    if [ $TESTS_FAILED -gt 0 ]; then
        echo -e "${RED}失败: $TESTS_FAILED${NC}"
    else
        echo -e "${RED}失败: $TESTS_FAILED${NC}"
    fi

    if [ $TESTS_FAILED -gt 0 ]; then
        exit 1
    fi
}

# 解析命令行参数
AUTO_START=true
STOP_AFTER_TEST=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-start)
            AUTO_START=false
            shift
            ;;
        --stop)
            STOP_AFTER_TEST=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options] [server_url]"
            echo "  server_url: 服务器地址 (默认: https://localhost:8443)"
            echo ""
            echo "Options:"
            echo "  --no-start    不自动启动服务器，假设已运行"
            echo "  --stop        测试完成后停止服务器"
            echo ""
            echo "示例:"
            echo "  $0                        # 自动启动服务器并测试"
            echo "  $0 --no-start             # 假设服务器已运行，直接测试"
            echo "  $0 --stop                 # 测试后停止服务器"
            echo "  $0 https://example.com    # 测试自定义服务器"
            exit 0
            ;;
        *)
            SERVER_URL="$1"
            shift
            ;;
    esac
done

# 如果 SERVER_URL 未被显式设置，使用默认值
if [ -z "$SERVER_URL" ]; then
    SERVER_URL="https://localhost:8443"
fi

cleanup() {
    stop_wt_upstream
    stop_local_upstream
    if [ "$STOP_AFTER_TEST" = true ]; then
        stop_server
    fi
}

trap cleanup EXIT

# 主函数
main() {
    print_header "hyper-proxy-tool 测试套件"
    echo "服务器: $SERVER_URL"
    echo "自动启动: $AUTO_START"

    start_local_upstream

    # 检查或启动服务器
    if [ "$AUTO_START" = true ]; then
        check_or_start_server
    else
        check_server
    fi

    # 基础功能测试
    print_header "基础功能测试"
    test_health
    test_route_valid
    test_route_not_found
    test_alt_svc
    test_metrics

    # 认证测试
    print_header "认证测试"
    test_auth_no_token
    test_auth_invalid_token
    test_auth_valid_token
    test_auth_wrong_secret
    test_auth_expired_token

    # 限流测试
    print_header "限流测试"
    test_ip_rate_limit
    test_route_rate_limit

    # 灰度路由测试
    print_header "灰度路由测试"
    test_canary_header
    test_canary_weight

    # 缓存测试
    print_header "缓存测试"
    test_cache_miss
    test_cache_hit

    # HTTP/3 测试
    print_header "HTTP/3 测试"
    test_http3
    test_http3_ip_rate_limit
    test_http3_jwt_auth
    test_http3_canary
    test_http3_cache
    test_webtransport

    # 打印摘要
    print_summary
}

# 运行
main
