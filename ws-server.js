// ws-server.js
const port = 9001;
console.log(`WebSocket Server running on ws://localhost:${port}`);

Bun.serve({
    port: port,
    fetch(req, server) {
        // Bun 原生方法：会尝试将 HTTP 请求升级为 WebSocket
        if (server.upgrade(req)) {
            return; // 升级成功，Bun 会自动处理后续握手
        }
        return new Response("Upgrade failed", { status: 500 });
    },
    websocket: {
        open(ws) {
            console.log("Client connected");
            ws.send("Welcome to Bun!");
        },
        message(ws, message) {
            console.log(`Received: ${message}`);
            ws.send(`Echo: ${message}`);
        },
        close(ws) {
            console.log("Client disconnected");
        },
    },
});
