// Minimal h2c gRPC-like upstream used by test.sh.
const http2 = require("http2");

const port = Number(process.env.GRPC_PORT || 50051);
const name = process.env.GRPC_NAME || `grpc-${port}`;
const delayMs = Number(process.env.GRPC_DELAY_MS || 12000);

function grpcFrame(payload) {
    const data = Buffer.from(payload);
    const frame = Buffer.alloc(5 + data.length);
    frame[0] = 0;
    frame.writeUInt32BE(data.length, 1);
    data.copy(frame, 5);
    return frame;
}

const server = http2.createServer();

server.on("stream", (stream, headers) => {
    const path = headers[":path"] || "";
    const contentType = headers["content-type"] || "";

    if (!String(contentType).startsWith("application/grpc")) {
        stream.respond({ ":status": 415 });
        stream.end("unsupported media type");
        return;
    }

    stream.respond({
        ":status": 200,
        "content-type": "application/grpc",
        "grpc-status": "0",
    });

    if (String(path).endsWith("/Slow")) {
        setTimeout(() => {
            stream.end(grpcFrame(`${name}:slow`));
        }, delayMs);
        return;
    }

    stream.end(grpcFrame(name));
});

server.listen(port, "127.0.0.1", () => {
    console.log(`gRPC h2c test upstream ${name} listening on 127.0.0.1:${port}`);
});
