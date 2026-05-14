const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3001;

let apiResponseData = {
    "Phien": null,
    "Xuc_xac_1": null,
    "Xuc_xac_2": null,
    "Xuc_xac_3": null,
    "Tong": null,
    "Ket_qua": "",
    "id": "@NguyenTung1920"
};

let currentSessionId = null;
let sessionsunloncac = [];

const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";

const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits=15; server_max_window_bits=15",
    "Connection": "Upgrade",
    "Upgrade": "websocket"
};

const RECONNECT_DELAY = 500;
const PING_INTERVAL = 3000;

const initialMessages = [
    [
        1,
        "MiniGame",
        "GM_apivopnha",
        "WangLin",
        {
            "info": "{\"ipAddress\":\"14.249.227.107\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiI5ODE5YW5zc3MiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMjMyODExNTEsImFmZklkIjoic3VuLndpbiIsImJhbm5lZCI6ZmFsc2UsImJyYW5kIjoiZ2VtIiwidGltZXN0YW1wIjoxNzYzMDMyOTI4NzcwLCJsb2NrR2FtZXMiOltdLCJhbW91bnQiOjAsImxvY2tDaGF0IjpmYWxzZSwicGhvbmVWZXJpZmllZCI6ZmFsc2UsImlwQWRkcmVzcyI6IjE0LjI0OS4yMjcuMTA3IiwibXV0ZSI6ZmFsc2UsImF2YXRhciI6Imh0dHBzOi8vaW1hZ2VzLnN3aW5zaG9wLm5ldC9pbWFnZXMvYXZhdGFyL2F2YXRhcl8wNS5wbmciLCJwbGF0Zm9ybUlkIjo0LCJ1c2VySWQiOiI4ODM4NTMzZS1kZTQzLTRiOGQtOTUwMy02MjFmNDA1MDUzNGUiLCJyZWdUaW1lIjoxNzYxNjMyMzAwNTc2LCJwaG9uZSI6IiIsImRlcG9zaXQiOmZhbHNlLCJ1c2VybmFtZSI6IkdNX2FwaXZvcG5oYSJ9.guH6ztJSPXUL1cU8QdMz8O1Sdy_SbxjSM-CDzWPTr-0\",\"locale\":\"vi\",\"userId\":\"8838533e-de43-4b8d-9503-621f4050534e\",\"username\":\"GM_apivopnha\",\"timestamp\":1763032928770,\"refreshToken\":\"e576b43a64e84f789548bfc7c4c8d1e5.7d4244a361e345908af95ee2e8ab2895\"}",
            "signature": "45EF4B318C883862C36E1B189A1DF5465EBB60CB602BA05FAD8FCBFCD6E0DA8CB3CE65333EDD79A2BB4ABFCE326ED5525C7D971D9DEDB5A17A72764287FFE6F62CBC2DF8A04CD8EFF8D0D5AE27046947ADE45E62E644111EFDE96A74FEC635A97861A425FF2B5732D74F41176703CA10CFEED67D0745FF15EAC1065E1C8BCBFA"
        }
    ],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;
let isConnected = false;

function connectWebSocket() {
    if (ws) {
        ws.removeAllListeners();
        ws.terminate();
        ws = null;
    }

    ws = new WebSocket(WEBSOCKET_URL, {
        headers: WS_HEADERS,
        handshakeTimeout: 3000,
        maxPayload: 104857600,
        skipUTF8Validation: true,
        perMessageDeflate: {
            zlibDeflateOptions: {
                chunkSize: 512,
                memLevel: 9,
                level: 1
            },
            zlibInflateOptions: {
                chunkSize: 512
            },
            clientNoContextTakeover: true,
            serverNoContextTakeover: true,
            clientMaxWindowBits: 15,
            serverMaxWindowBits: 15,
            concurrencyLimit: 20,
            threshold: 0
        }
    });

    ws.on("open", () => {
        console.log("[✅] Connected! Delay ~0ms");
        isConnected = true;

        initialMessages.forEach((msg, i) => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(msg), { compress: true, fin: true });
                console.log("[📤] Sent msg " + (i + 1));
            }
        });

        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.ping();
            }
        }, PING_INTERVAL);
    });

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message);

            if (!Array.isArray(data) || typeof data[1] !== "object") {
                return;
            }

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
            }

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;

                const total = d1 + d2 + d3;
                const result = (total > 10) ? "Tài" : "Xỉu";

                const sessionData = {
                    "Phien": currentSessionId,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result,
                    "id": "@NguyenTung1920",
                    "timestamp": Date.now()
                };

                apiResponseData = sessionData;

                sessionsunloncac.unshift({
                    "Phien": currentSessionId,
                    "Xuc_xac_1": d1,
                    "Xuc_xac_2": d2,
                    "Xuc_xac_3": d3,
                    "Tong": total,
                    "Ket_qua": result
                });

                console.log("[🎲] Phien " + currentSessionId + ": " + d1 + "-" + d2 + "-" + d3 + " = " + total + " (" + result + ") | sunloncac: " + sessionsunloncac.length);

                currentSessionId = null;
            }
        } catch (e) {
            console.error("[❌] Parse error:", e.message);
        }
    });

    ws.on("close", (code, reason) => {
        console.log("[🔌] Closed. Code: " + code);
        isConnected = false;
        clearInterval(pingInterval);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on("error", (err) => {
        console.error("[❌] Error:", err.message);
        isConnected = false;
        ws.terminate();
    });

    ws.on("ping", () => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.pong();
        }
    });
}

app.get("/api/sunloncac", (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.json({
        "current": apiResponseData,
        "sunloncac": sessionsunloncac,
        "total": sessionsunloncac.length,
        "id": "@NguyenTung1920"
    });
});

app.get("/", (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Connection", "keep-alive");
    res.redirect("/api/sunloncac");
});

app.listen(PORT, "0.0.0.0", () => {
    console.log("[🌐] Server: http://localhost:" + PORT);
    console.log("[📊] API sunloncac: http://localhost:" + PORT + "/api/sunloncac");
    console.log("[♾️] cách treo : tùng up lên vps rồi treo 1-2 tiếng là okay đủ dữ liệu nhé");
    connectWebSocket();
});