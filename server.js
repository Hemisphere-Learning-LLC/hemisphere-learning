'use strict';
const cluster = require('cluster');
const http = require('http');
const crypto = require('crypto');
const os = require('os');
const WebSocket = require('ws');

const PORT = 8080;
const WORKERS = Math.min(4, os.availableParallelism());
const MAX_CONNS = 15000;
const MAX_PER_IP = 50;
const MAX_MSGS_PER_TICK = 100;
const TICK_MS = 5000;
const HANDSHAKE_MS = 10000;
const MAX_MISSED = 2;
const MAX_PAYLOAD = 8192;

const HEADER = 0x91;
const OP_CONN_REGISTERED = 0x00;
const OP_CLIENT_ID = 0x03;
const OP_INVALID_PACKET = 0x04;
const OP_PING_PONG = 0x05;
const OP_REQUEST_RESOURCE = 0x06;

const ERR_NOT_A_BUFFER = 0x00;
const ERR_INVALID_LENGTH = 0x01;
const ERR_INVALID_PAYLOAD = 0x02;

if (cluster.isPrimary) {
    cluster.setupPrimary({ execArgv: ['--max-old-space-size=200'] });
    for (let i = 0; i < WORKERS; i++) cluster.fork();
    cluster.on('exit', () => setTimeout(() => cluster.fork(), 1000));
    for (const s of ['SIGINT', 'SIGTERM']) {
        process.on(s, () => {
            for (const w of Object.values(cluster.workers)) w.process.kill();
            process.exit(0);
        });
    }
} else {
    runWorker();
}

function runWorker() {
    const clients = new Map();
    const ipCounts = new Map();
    const PING = Buffer.from([HEADER, OP_PING_PONG]);
    const HELLO = Buffer.from([HEADER, OP_CONN_REGISTERED]);
    const noop = () => {};

    const server = http.createServer(
        { headersTimeout: 5000, requestTimeout: 10000 },
        (req, res) => res.writeHead(426).end()
    );

    const wss = new WebSocket.Server({
        noServer: true,
        maxPayload: MAX_PAYLOAD,
        perMessageDeflate: false,
        skipUTF8Validation: true,
    });

    server.on('upgrade', (req, socket, head) => {
        socket.on('error', noop);
        const ip = socket.remoteAddress;
        if (clients.size >= MAX_CONNS || (ipCounts.get(ip) || 0) >= MAX_PER_IP) {
            return socket.destroy();
        }
        wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws, ip));
    });

    function destroy(c) {
        if (c.dead) return;
        c.dead = true;
        clients.delete(c.key);
        const n = ipCounts.get(c.ip);
        if (n > 1) ipCounts.set(c.ip, n - 1);
        else ipCounts.delete(c.ip);
        c.ws.terminate();
    }

    function fail(c, code) {
        c.closing = true;
        c.ws.send(Buffer.from([HEADER, OP_INVALID_PACKET, code]), () => c.ws.close());
    }

    function onMessage(c, data, isBinary) {
        if (c.closing) return;
        if (++c.msgs > MAX_MSGS_PER_TICK) return destroy(c);
        if (!isBinary) return fail(c, ERR_NOT_A_BUFFER);
        if (data.length < 2) return fail(c, ERR_INVALID_LENGTH);
        if (data[0] !== HEADER) return fail(c, ERR_INVALID_PAYLOAD);

        const op = data[1];
        if (op === OP_PING_PONG) {
            c.missed = 0;
            return;
        }

        if (!c.established) {
            if (op !== OP_CLIENT_ID) return fail(c, ERR_INVALID_PAYLOAD);
            if (data.length !== 18) return fail(c, ERR_INVALID_LENGTH);
            c.clientId = data.toString('hex', 2, 18);
            c.established = true;
            return;
        }

        if (op === OP_REQUEST_RESOURCE) {
            // TODO
        }
    }

    function onConnection(ws, ip) {
        const id = crypto.randomBytes(16);
        const c = {
            ws,
            ip,
            key: id.toString('hex'),
            clientId: null,
            established: false,
            closing: false,
            dead: false,
            missed: 0,
            msgs: 0,
            born: Date.now(),
        };
        clients.set(c.key, c);
        ipCounts.set(ip, (ipCounts.get(ip) || 0) + 1);

        ws.on('message', (data, isBinary) => onMessage(c, data, isBinary));
        ws.on('close', () => destroy(c));
        ws.on('error', () => destroy(c));
        ws.send(Buffer.concat([HELLO, id]), (err) => err && destroy(c));
    }

    setInterval(() => {
        const now = Date.now();
        for (const c of clients.values()) {
            if (c.closing || c.missed >= MAX_MISSED || (!c.established && now - c.born >= HANDSHAKE_MS)) {
                destroy(c);
                continue;
            }
            c.msgs = 0;
            c.missed++;
            c.ws.send(PING, noop);
        }
    }, TICK_MS);

    server.listen(PORT);
}
