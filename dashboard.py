import json, os, threading, time
from collections import defaultdict, deque
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "snmp-traffic")
PORT = int(os.getenv("PORT", "8000"))
HISTORY = int(os.getenv("HISTORY", "90"))   # 90 จุด x 10 วินาที = 15 นาที

latest = {}
history = defaultdict(lambda: deque(maxlen=HISTORY))
lock = threading.Lock()


def consume():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "snmp-dashboard",
        "auto.offset.reset": "earliest",   # เปิดมาแล้วมีข้อมูลย้อนหลังให้เห็นทันที
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


def age_of(ts):
    try:
        return round(time.time() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError:
        return None



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

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/data":
            with lock:
                rows = [dict(v, age=age_of(v["time"])) for v in latest.values()]
                body = json.dumps({"latest": rows,
                                   "history": {k: list(v) for k, v in history.items()}})
            self._send(200, body.encode(), "application/json")
        elif path == "/metrics":
            self._send(200, build_metrics(), "text/plain; version=0.0.4")
        elif path in ("/", "/index.html"):
            with open("index.html", "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain")

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=consume, daemon=True).start()
    print(f"Dashboard on http://0.0.0.0:{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
