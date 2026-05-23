#!/usr/bin/env node
const http2 = require("http2");

const target = process.env.SERVER_URL || "https://127.0.0.1:8443";
const path = process.argv[2] || "/helloworld.Greeter/SayHello";
const grpcTimeout = process.env.GRPC_TIMEOUT_HEADER || "";
const bearerToken = process.env.BEARER_TOKEN || "";
const requestPayloads = (process.env.GRPC_REQUEST_PAYLOADS || "ping")
    .split(",")
    .filter(Boolean);

function grpcFrame(payload) {
    const data = Buffer.from(payload);
    const frame = Buffer.alloc(5 + data.length);
    frame[0] = 0;
    frame.writeUInt32BE(data.length, 1);
    data.copy(frame, 5);
    return frame;
}

function decodeGrpcPayload(buffer) {
    if (buffer.length < 5) {
        return "";
    }

    const compressed = buffer[0];
    const length = buffer.readUInt32BE(1);
    if (compressed !== 0 || buffer.length < 5 + length) {
        return "";
    }

    return buffer.subarray(5, 5 + length).toString();
}

const client = http2.connect(target, {
    rejectUnauthorized: false,
});

client.on("error", (err) => {
    console.error(err.message);
    process.exitCode = 1;
});

const requestHeaders = {
    ":method": "POST",
    ":path": path,
    "content-type": "application/grpc",
    te: "trailers",
};

if (grpcTimeout) {
    requestHeaders["grpc-timeout"] = grpcTimeout;
}

if (bearerToken) {
    requestHeaders.authorization = `Bearer ${bearerToken}`;
}

const req = client.request(requestHeaders);

let status = 0;
let sawTrailers = false;
let grpcStatusInHeaders = "";
let grpcStatus = "";
let grpcMessage = "";
const chunks = [];

req.on("response", (headers) => {
    status = Number(headers[":status"] || 0);
    grpcStatusInHeaders = String(headers["grpc-status"] || "");
});

req.on("trailers", (headers) => {
    sawTrailers = true;
    grpcStatus = String(headers["grpc-status"] || grpcStatus);
    grpcMessage = String(headers["grpc-message"] || grpcMessage);
});

req.on("data", (chunk) => chunks.push(chunk));

req.on("end", () => {
    client.close();
    const payload = decodeGrpcPayload(Buffer.concat(chunks));
    if (status !== 200) {
        console.error(`HTTP ${status}`);
        process.exit(1);
    }

    if (grpcStatusInHeaders) {
        console.error(`grpc-status appeared in initial headers: ${grpcStatusInHeaders}`);
        process.exit(1);
    }

    if (!sawTrailers) {
        console.error("missing grpc trailers");
        process.exit(1);
    }

    if (!grpcStatus) {
        console.error("missing grpc-status trailer");
        process.exit(1);
    }

    if (grpcStatus !== "0") {
        if (grpcMessage) {
            console.error(`grpc-status ${grpcStatus} ${grpcMessage}`);
        } else {
            console.error(`grpc-status ${grpcStatus}`);
        }
        process.exit(1);
    }

    console.log(payload);
});

req.end(Buffer.concat(requestPayloads.map((payload) => grpcFrame(payload))));
