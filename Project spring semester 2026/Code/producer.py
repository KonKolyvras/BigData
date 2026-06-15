# https://toruseo.jp/UXsim/docs/index.html
# pip install uxsim pandas kafka-python

from uxsim import *
import random
from PIL import Image
import numpy as np
import time
import json
from kafka import KafkaProducer

# Μετατρέπει numpy τύπους σε Python types ώστε να γίνει JSON serialize
def convert(obj):
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    if isinstance(obj, (np.ndarray,)): return obj.tolist()
    return obj

# Χτίζει το 4x4 grid δίκτυο κυκλοφορίας με κόμβους, ακμές, φανάρια και ζήτηση
def simulate_traffic():
    seed = None

    W = World(
        name="",
        deltan=5,
        tmax=3600,
        print_mode=1, save_mode=0, show_mode=1,
        random_seed=seed,
        duo_update_time=600
    )
    random.seed(seed)

    signal_time = 20
    sf_1 = 1
    sf_2 = 2
    I1 = W.addNode("I1", 1, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I2 = W.addNode("I2", 2, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I3 = W.addNode("I3", 3, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    I4 = W.addNode("I4", 4, 0, signal=[signal_time * sf_1, signal_time * sf_2])
    W1 = W.addNode("W1", 0, 0)
    E1 = W.addNode("E1", 5, 0)
    N1 = W.addNode("N1", 1, 1)
    N2 = W.addNode("N2", 2, 1)
    N3 = W.addNode("N3", 3, 1)
    N4 = W.addNode("N4", 4, 1)
    S1 = W.addNode("S1", 1, -1)
    S2 = W.addNode("S2", 2, -1)
    S3 = W.addNode("S3", 3, -1)
    S4 = W.addNode("S4", 4, -1)

    # Οριζόντιες ακμές (Δυτικά ↔ Ανατολικά)
    for n1, n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)

    # Κατακόρυφες ακμές (Βόρεια → Νότια)
    for n1, n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
        W.addLink(n1.name + n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    # Κατακόρυφες ακμές (Νότια → Βόρεια)
    for n1, n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
        W.addLink(n2.name + n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

    # Τυχαία ζήτηση οχημάτων κάθε 30 δευτερόλεπτα
    dt = 30
    demand = 2
    for t in range(0, 3600, dt):
        dem = random.uniform(0, demand)
        for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.25)
        for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1], [S4, W1]]:
            W.adddemand(n1, n2, t, t + dt, dem * 0.75)
    return W

# Αποστέλλει τα δεδομένα της προσομοίωσης στο Redpanda topic ανά N δευτερόλεπτα
def kafka_producer_loop(W, bootstrap_servers="localhost:19092", topic="vehicle_positions", interval=1.0):

    # Σύνδεση στο Redpanda — αν αποτύχει, σταματά με error
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"Connected to Redpanda/Kafka at {bootstrap_servers} successfully.")
    except Exception as e:
        print(f"Error connecting to Redpanda/Kafka: {e}")
        return

    # Εκτέλεση ολόκληρης της προσομοίωσης και εξαγωγή σε DataFrame
    print("Running UXSIM traffic simulation...")
    W.exec_simulation()
    df = W.analyzer.vehicles_to_pandas()
    print(f"Simulation completed. Columns: {df.columns.tolist()}")

    time_col = 't'
    if time_col not in df.columns:
        print(f"Error: Time column '{time_col}' not found in DataFrame.")
        return

    times = sorted(df[time_col].unique())
    print(f"Replaying timeline to Kafka topic '{topic}' with step N={interval}s. Filtering speed (v) > 0...")

    for t in times:
        snapshot = df[df[time_col] == t]
        sent_count = 0
        for _, row in snapshot.iterrows():
            row_dict = row.to_dict()
            # Φίλτρο: στέλνει ΜΟΝΟ οχήματα σε κίνηση (v > 0)
            if row_dict.get('v', 0) > 0:
                try:
                    serialized_row = {k: convert(v) for k, v in row_dict.items()}
                    # Αποστολή JSON record στο Redpanda topic
                    producer.send(topic, serialized_row)
                    sent_count += 1
                except Exception as e:
                    print(f"Error sending record: {e}")

        if sent_count > 0:
            try:
                # Βεβαιώνει ότι όλα τα μηνύματα έφτασαν στο broker
                producer.flush()
                print(f"Time {t}: Sent {sent_count} active vehicle positions to topic '{topic}' and flushed.")
            except Exception as e:
                print(f"Error flushing: {e}")

        # Αναμονή N δευτερόλεπτα για real-time προσομοίωση
        time.sleep(interval)

# Παραμετροποίηση από command line: -n (interval), -t (topic), -b (broker)
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="UXSIM Traffic Simulation Producer for Redpanda/Kafka")
    parser.add_argument("-n", "--interval", type=float, default=1.0, help="Interval in seconds between steps (default: 1.0)")
    parser.add_argument("-t", "--topic", type=str, default="vehicle_positions", help="Kafka topic name (default: vehicle_positions)")
    parser.add_argument("-b", "--bootstrap-servers", type=str, default="localhost:19092", help="Bootstrap servers (default: localhost:19092)")
    args = parser.parse_args()

    W = simulate_traffic()
    kafka_producer_loop(W, bootstrap_servers=args.bootstrap_servers, topic=args.topic, interval=args.interval)
    print("Process completed.")
