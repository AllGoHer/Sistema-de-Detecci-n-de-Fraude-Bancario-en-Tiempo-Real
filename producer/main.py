import json
import time
import uuid
import threading
import logging
import random
from dataclasses import dataclass, asdict
from kafka import KafkaProducer

# Silenciar logs innecesarios para alto rendimiento
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "bank_transactions"
TPS_PER_THREAD = 200 # 5 hilos * 200 = 1000 TPS

@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    amount: float
    event_type: str
    is_fraud: int
    city: str
    device_type: str
    epoch_ms: int

class DynamicFraudSimulator:
    def __init__(self, num_users=100):
        self.users = [f"USR-{i:05d}" for i in range(1, num_users + 1)]
        # Cada usuario tiene un estado: si está atacando y hasta cuándo
        self.user_state = {uid: {"is_attacking": False, "attack_end_time": 0} for uid in self.users}
        self.last_attack_cycle = time.time()

    def update_attackers(self):
        """Lógica de estado: Cada 20 segundos, elige 2-4 usuarios al azar para que ataquen"""
        current_time = time.time()
        if current_time - self.last_attack_cycle > 20:
            self.last_attack_cycle = current_time
            
            # Reiniciar a todos a estado normal
            for uid in self.users:
                self.user_state[uid]["is_attacking"] = False
            
            # Elegir de 2 a 4 atacantes aleatorios para la próxima ronda
            num_attackers = random.randint(2, 4)
            attackers = random.sample(self.users, num_attackers)
            
            for uid in attackers:
                self.user_state[uid]["is_attacking"] = True
                # El ataque durará entre 10 y 25 segundos
                self.user_state[uid]["attack_end_time"] = current_time + random.uniform(10, 25)

    def get_transaction(self):
        self.update_attackers()
        
        # Filtrar quién está atacando actualmente
        active_attackers = [uid for uid, state in self.user_state.items() if state["is_attacking"] and time.time() < state["attack_end_time"]]
        
        # Si hay atacantes, el 80% del tráfico irá hacia ellos (simula ráfaga)
        if active_attackers and random.random() < 0.80:
            user_id = random.choice(active_attackers)
            return Transaction(
                transaction_id=str(uuid.uuid4()),
                user_id=user_id,
                amount=round(random.uniform(400.0, 2500.0), 2),
                event_type=random.choice(["VELOCITY_BURST", "FRAUD_CASH_OUT", "SALAMI_SLICING"]),
                is_fraud=1,
                city=random.choice(["NYC", "LONDON", "TOKYO", "MADRID"]),
                device_type="Mobile App",
                epoch_ms=int(time.time() * 1000)
            )
        else:
            # Transacción normal
            user_id = random.choice(self.users)
            return Transaction(
                transaction_id=str(uuid.uuid4()),
                user_id=user_id,
                amount=round(random.uniform(10.0, 150.0), 2),
                event_type="PURCHASE",
                is_fraud=0,
                city="NYC",
                device_type=random.choice(["POS", "Online Web", "Mobile App"]),
                epoch_ms=int(time.time() * 1000)
            )

def producer_thread(simulator, thread_id):
    try:
        print(f"🔗 Hilo {thread_id} conectando a Kafka...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            api_version=(2, 8, 0), 
            request_timeout_ms=5000,
            value_serializer=lambda v: json.dumps(asdict(v)).encode('utf-8')
        )
        print(f"✅ Hilo {thread_id} conectado exitosamente.")
    except Exception as e:
        print(f"❌ Error conectando hilo {thread_id}: {e}")
        return

    while True:
        tx = simulator.get_transaction()
        producer.send(TOPIC, key=tx.user_id.encode('utf-8'), value=tx)
        time.sleep(1.0 / TPS_PER_THREAD)

if __name__ == "__main__":
    print("🚀 Iniciando Producer Senior (Simulación Dinámica de Ataques)...")
    time.sleep(5) 
    
    simulator = DynamicFraudSimulator()
    threads = [threading.Thread(target=producer_thread, args=(simulator, i), daemon=True) for i in range(5)]
    
    for t in threads: 
        t.start()
    
    print("🔥 Todos los hilos lanzados. Inyectando ataques aleatorios dinámicos...")
    
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Detenido.")