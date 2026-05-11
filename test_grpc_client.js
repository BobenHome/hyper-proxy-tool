#!/usr/bin/env node
const http2 = require("http2");

const target = process.env.SERVER_URL || "https://127.0.0.1:8443";
const path = process.argv[2] || "/helloworld.Greeter/SayHello";

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

const req = client.request({
    ":method": "POST",
    ":path": path,
    "content-type": "application/grpc",
    te: "trailers",
});

let status = 0;
let grpcStatus = "";
const chunks = [];

req.on("response", (headers) => {
    status = Number(headers[":status"] || 0);
});

req.on("trailers", (headers) => {
    grpcStatus = String(headers["grpc-status"] || grpcStatus);
});

req.on("data", (chunk) => chunks.push(chunk));

req.on("end", () => {
    client.close();
    const payload = decodeGrpcPayload(Buffer.concat(chunks));
    if (status !== 200) {
        console.error(`HTTP ${status}`);
        process.exit(1);
    }

    if (grpcStatus && grpcStatus !== "0") {
        console.error(`grpc-status ${grpcStatus}`);
        process.exit(1);
    }

    console.log(payload);
});

req.end(grpcFrame("ping"));
