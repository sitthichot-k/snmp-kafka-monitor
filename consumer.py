import json, os
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "snmp-traffic")

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "snmp-viewer",
    "auto.offset.reset": "latest",
})
c.subscribe([TOPIC])
print(f"Listening on {BOOTSTRAP}/{TOPIC}", flush=True)

while True:
    m = c.poll(1.0)
    if m is None:
        continue
    if m.error():
        print("Kafka:", m.error(), flush=True)
        continue
    d = json.loads(m.value())
    if d["in_bps"] is None:
        continue  # รอบแรกยังไม่มีค่าก่อนหน้าให้คำนวณ
    print(f'{d["time"]}  {d["router"]:<3} {d["ifname"]:<7} {d["status"]:<5} '
          f'in={d["in_bps"]/1e6:7.2f} Mbps  out={d["out_bps"]/1e6:7.2f} Mbps', flush=True)
