from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("--- PRODUSER BANK AKTIF (Mengirim data setiap 2 detik) ---")

while True:
    data = {
        "nama": random.choice(["Andi", "Budi", "Citra", "Dina", "Rana"]),
        "rekening": str(random.randint(100000, 999999)),
        "jumlah": random.randint(100000, 100000000),
        "lokasi": random.choice(["Jakarta", "Surabaya", "Bandung", "Luar Negeri"])
    }
    producer.send("bank_topic", value=data)
    print(f"Transaksi Terkirim: {data}")
    time.sleep(2)
