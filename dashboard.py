import json, logging, os, threading, time
from collections import defaultdict, deque
from datetime import datetime

import socketio
from confluent_kafka import Consumer
from werkzeug.serving import run_simple

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "snmp-traffic")
PORT = int(os.getenv("PORT", "8000"))
HISTORY = int(os.getenv("HISTORY", "90"))   # 90 จุด x 10 วินาที = 15 นาที

sio = socketio.Server(async_mode="threading", cors_allowed_origins="*")
latest = {}
history = defaultdict(lambda: deque(maxlen=HISTORY))
lock = threading.Lock()


def age_of(ts):
    try:
        return round(time.time() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError:
        return None


def with_age(d):
    return dict(d, age=age_of(d["time"]))


def snapshot():
    with lock:
        return {"latest": [with_age(v) for v in latest.values()],
                "history": {k: list(v) for k, v in history.items()}}


@sio.on("snapshot_request")
def on_snapshot_request(sid):
    sio.emit("snapshot", snapshot(), to=sid)


def consume():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "snmp-dashboard",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC])
    print(f"Dashboard consuming {BOOTSTRAP}/{TOPIC}", flush=True)
    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        try:
            d = json.loads(m.value())
        except ValueError:
            continue
        key = f'{d["router"]}|{d["ifname"]}'
        with lock:
            latest[key] = d
            if d.get("in_bps") is not None:
                history[key].append([d["time"], d["in_bps"], d["out_bps"]])
        row = with_age(d)
        # ส่งเฉพาะข้อมูลสด (ตอนอ่านย้อนหลังจาก Kafka ตอนเริ่มระบบจะไม่ส่งให้ client)
        if row["age"] is not None and row["age"] < 60:
            sio.emit("traffic", row)


def build_metrics():
    def lab(r):
        esc = lambda v: str(v).replace("\\", "\\\\").replace('"', '\\"')
        return 'router="%s",ifname="%s"' % (esc(r["router"]), esc(r["ifname"]))
    with lock:
        rows = list(latest.values())
    defs = [
        ("snmp_if_in_octets_total", "counter", "ifHCInOctets via Kafka", lambda r, a: r["in_octets"]),
        ("snmp_if_out_octets_total", "counter", "ifHCOutOctets via Kafka", lambda r, a: r["out_octets"]),
        ("snmp_if_up", "gauge", "1 if ifOperStatus is up", lambda r, a: 1 if r["status"] == "up" else 0),
        ("snmp_sample_age_seconds", "gauge", "Age of the latest sample", lambda r, a: a if a is not None else -1),
    ]
    out = []
    for name, typ, helptext, fn in defs:
        out.append("# HELP %s %s" % (name, helptext))
        out.append("# TYPE %s %s" % (name, typ))
        for r in rows:
            out.append("%s{%s} %s" % (name, lab(r), fn(r, age_of(r["time"]))))
    return ("\n".join(out) + "\n").encode()


def http_app(environ, start_response):
    path = environ.get("PATH_INFO", "/")
    if path == "/metrics":
        body, ctype = build_metrics(), "text/plain; version=0.0.4"
    elif path == "/api/data":
        body, ctype = json.dumps(snapshot()).encode(), "application/json"
    elif path in ("/", "/index.html"):
        with open("index.html", "rb") as f:
            body, ctype = f.read(), "text/html; charset=utf-8"
    else:
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"not found"]
    start_response("200 OK", [("Content-Type", ctype), ("Cache-Control", "no-store"),
                              ("Content-Length", str(len(body)))])
    return [body]


application = socketio.WSGIApp(sio, http_app)

if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    threading.Thread(target=consume, daemon=True).start()
    print(f"Dashboard (Socket.IO) on http://0.0.0.0:{PORT}", flush=True)
    run_simple("0.0.0.0", PORT, application, threaded=True)
