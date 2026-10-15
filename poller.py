import json, os, subprocess, time
from datetime import datetime
from confluent_kafka import Producer

ROUTERS = dict(r.split("=") for r in os.getenv("ROUTERS", "R1=192.168.199.143").split(","))
COMMUNITY = os.getenv("COMMUNITY", "public")
INTERVAL = int(os.getenv("INTERVAL", "10"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "snmp-traffic")

OIDS = {
    "name":   "1.3.6.1.2.1.31.1.1.1.1",   # ifName
    "in":     "1.3.6.1.2.1.31.1.1.1.6",   # ifHCInOctets
    "out":    "1.3.6.1.2.1.31.1.1.1.10",  # ifHCOutOctets
    "status": "1.3.6.1.2.1.2.2.1.8",      # ifOperStatus
}
STATUS = {"1": "up", "2": "down"}


def walk(ip, oid):
    """เรียก snmpwalk แล้วคืนค่าเป็น dict {ifIndex: value}"""
    cmd = ["snmpwalk", "-v2c", "-c", COMMUNITY, "-t", "3", "-r", "1", "-Oqne", ip, oid]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or "snmpwalk failed")
    result = {}
    for line in p.stdout.splitlines():
        parts = line.split(" ", 1)
        if len(parts) == 2:
            idx = parts[0].rsplit(".", 1)[-1]
            result[idx] = parts[1].strip().strip('"')
    return result


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    prev = {}
    print(f"Polling {ROUTERS} every {INTERVAL}s -> {BOOTSTRAP}/{TOPIC}", flush=True)
    while True:
        start = time.time()
        for router, ip in ROUTERS.items():
            try:
                data = {k: walk(ip, oid) for k, oid in OIDS.items()}
            except Exception as e:
                print(f"[{router}] SNMP error: {e}", flush=True)
                continue
            now = time.time()
            for idx, ifname in data["name"].items():
                if idx not in data["in"] or idx not in data["out"]:
                    continue
                in_o, out_o = int(data["in"][idx]), int(data["out"][idx])
                in_bps = out_bps = None
                key = (router, idx)
                if key in prev:
                    t0, i0, o0 = prev[key]
                    dt = now - t0
                    if dt > 0 and in_o >= i0 and out_o >= o0:
                        in_bps = round((in_o - i0) * 8 / dt)
                        out_bps = round((out_o - o0) * 8 / dt)
                prev[key] = (now, in_o, out_o)
                msg = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "router": router, "ip": ip,
                    "ifindex": int(idx), "ifname": ifname,
                    "status": STATUS.get(data["status"].get(idx, ""), "unknown"),
                    "in_octets": in_o, "out_octets": out_o,
                    "in_bps": in_bps, "out_bps": out_bps,
                }
                producer.produce(TOPIC, key=f"{router}:{ifname}", value=json.dumps(msg))
            producer.poll(0)
            print(f"[{router}] sent {len(data['name'])} interfaces", flush=True)
        producer.flush(10)
        time.sleep(max(0, INTERVAL - (time.time() - start)))


if __name__ == "__main__":
    main()
