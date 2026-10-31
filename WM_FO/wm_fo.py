import sys
import threading
import time
import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

class WM_FO:
    def __init__(self, broker_address: str, operator_id:str):
        self.broker_address = broker_address
        self.operator_id = operator_id
        self.running = False
        self.producer = None
        self.consumer = None
        self.estacion = None
        self.evento_fin_riego = threading.Event()

    def iniciar(self):
        self.running = True
        self.configurar_kafka()
        threading.Thread(target=self.escuchar_kafka, daemon=True).start()
        self.mostrar_menu()

    def configurar_kafka(self):
        try:
            self.producer = Producer({'bootstrap.servers': self.broker_address})
            self.consumer = Consumer({
                'bootstrap.servers': self.broker_address,
                'group.id': f'fo-group-{self.operator_id}',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': True
            })
            self.consumer.subscribe(['wm-responses', 'wm-telemetry'])
            print(f"[OPERARIO {self.operator_id}] Conectado a Kafka en {self.broker_address}")
        except Exception as e:
            print(f"[OPERARIO {self.operator_id}] Error inicializando Kafka: {e}")
            sys.exit(1)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            pass
            #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def escuchar_kafka(self):
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    datos = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    if topic == 'wm-responses':
                        if datos.get("operator_id") == self.operator_id:
                            estado = datos.get("status")
                            mensaje = datos.get("msg")
                            ws_id = datos.get("ws_id")
                            print(f"\n>[RESPUESTA CENTRAL] {estado}: {mensaje} (Estación {ws_id})<")
                            if estado == "APPROVED":
                                self.estacion = ws_id
                            elif estado in ["FINISHED", "DENIED"]:
                                self.estacion = None
                                self.evento_fin_riego.set()
                    elif topic == 'wm-telemetry':
                        if datos.get("operator_id") == self.operator_id and datos.get("ws_id") == self.estacion:
                            caudal = datos.get("flow")
                            acumulado = datos.get("accumulated_volume")
                            print(f"[TELEMETRIA {self.estacion}] Regando... Caudal: {caudal}L/min | Total acumulado: {acumulado}L ")
        finally:
            if self.consumer:
                self.consumer.close()

    def imprimir_menu(self):
        print("\nElige una opción (1. Regar | 2. Salir): ")

    def mostrar_menu(self):
        time.sleep(1)
        while self.running:
            try:
                self.imprimir_menu()
                opcion = input().strip()
                if opcion == "1":
                    ws_id = input("Introduce el ID de la estación (ej. WS-01): \n").strip()
                    duracion_str = input("Introduce la duración en segundos: \n").strip()
                    if duracion_str.isdigit() and int(duracion_str) > 0:
                        duracion = int(duracion_str)
                        self.solicitar_riego(ws_id, duracion)  
                        self.evento_fin_riego.wait()
                        time.sleep(0.5)
                    else:
                        print("Duración invalida. Debe ser un numero.")
                elif opcion == "2":
                    print("Saliendo de la aplicación...")
                    self.running = False
                else:
                    print("Opción no válida.")
            except EOFError:
                break
            except KeyboardInterrupt:
                self.running = False
        if self.producer:
            self.producer.flush()

    def solicitar_riego(self, ws_id: str, duracion: int):
        peticion = {
            "operator_id": self.operator_id,
            "ws_id": ws_id,
            "duration": duracion
        }
        self.producer.produce('wm-requests', value=json.dumps(peticion).encode('utf-8'), callback=self.delivery_report)
        self.producer.poll(0)
        print(f"[OPERARIO] Petición enviada a Central. Esperando validación...")

def main():
    if (len(sys.argv) != 3):
        print("Uso: python wm_fo.py <ip:puerto_broker> <id_operario>")
        sys.exit(1)
    broker_address = sys.argv[1]
    operator_id = sys.argv[2]
    fo = WM_FO(broker_address, operator_id)
    fo.iniciar()

if __name__ == "__main__":
    main()


