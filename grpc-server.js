// Minimal h2c gRPC-like upstream used by test.sh.
const http2 = require("http2");
const fs = require("fs");

const port = Number(process.env.GRPC_PORT || 50051);
const name = process.env.GRPC_NAME || `grpc-${port}`;
const delayMs = Number(process.env.GRPC_DELAY_MS || 12000);
const healthFile = process.env.GRPC_HEALTH_FILE || "";

function grpcFrame(payload) {
    const data = Buffer.from(payload);
    const frame = Buffer.alloc(5 + data.length);
    frame[0] = 0;
    frame.writeUInt32BE(data.length, 1);
    data.copy(frame, 5);
    return frame;
}

function encodeHealthResponse(status) {
    return Buffer.from([0x08, status]);
}

function currentHealthStatus() {
    if (!healthFile) {
        return "SERVING";
    }

    try {
        return String(fs.readFileSync(healthFile, "utf8")).trim() || "SERVING";
    } catch {
        return "SERVING";
    }
}

const server = http2.createServer();

function sendGrpcResponse(stream, payload, trailers, delay = 0) {
    stream.respond(
        {
            ":status": 200,
            "content-type": "application/grpc",
        },
        { waitForTrailers: true },
    );

    stream.on("wantTrailers", () => {
        stream.sendTrailers(trailers);
    });

    const finish = () => {
        if (payload === undefined) {
            stream.end();
        } else {
            stream.end(grpcFrame(payload));
        }
    };

    if (delay > 0) {
        setTimeout(finish, delay);
    } else {
        finish();
    }
}

server.on("stream", (stream, headers) => {
    const path = headers[":path"] || "";
    const contentType = headers["content-type"] || "";

    if (!String(contentType).startsWith("application/grpc")) {
        stream.respond({ ":status": 415 });
        stream.end("unsupported media type");
        return;
    }

    if (String(path) === "/grpc.health.v1.Health/Check") {
        const status = currentHealthStatus();
        const servingStatus =
            status === "NOT_SERVING" ? 2 : status === "SERVICE_UNKNOWN" ? 3 : 1;
        sendGrpcResponse(stream, encodeHealthResponse(servingStatus), {
            "grpc-status": "0",
        });
        return;
    }

    if (String(path).endsWith("/Slow")) {
        sendGrpcResponse(stream, `${name}:slow`, { "grpc-status": "0" }, delayMs);
        return;
    }

    if (String(path).endsWith("/Fail")) {
        sendGrpcResponse(stream, undefined, {
            "grpc-status": "14",
            "grpc-message": "upstream unavailable",
        });
        return;
    }

    if (String(path).endsWith("/RetryOn503") && name === "grpc-a") {
        stream.respond({ ":status": 503 });
        stream.end("retryable upstream unavailable");
        return;
    }

    if (String(path).endsWith("/MaybeStream503") && name === "grpc-a") {
        stream.respond({ ":status": 503 });
        stream.end("streaming upstream unavailable");
        return;
    }

    if (String(path).endsWith("/BusinessFail") && name === "grpc-a") {
        sendGrpcResponse(stream, undefined, {
            "grpc-status": "14",
            "grpc-message": "business unavailable",
        });
        return;
    }

    if (String(path).endsWith("/EchoTimeout")) {
        sendGrpcResponse(stream, String(headers["grpc-timeout"] || ""), {
            "grpc-status": "0",
        });
        return;
    }

    sendGrpcResponse(stream, name, { "grpc-status": "0" });
});

server.listen(port, "127.0.0.1", () => {
    console.log(`gRPC h2c test upstream ${name} listening on 127.0.0.1:${port}`);
});
