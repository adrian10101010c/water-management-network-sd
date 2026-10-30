import sys
import socket
import time
import threading
import protocol
import json
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

class WM_WS_E:
    def __init__(self, broker_address, monitor_address, ws_id):
        self.broker_address = broker_address
        self.monitor_address = monitor_address
        self.ws_id = ws_id
        self.client_socket = None
        self.running = False
        self.estado = "OK"
        self.is_watering = False
        self.operator = None
        self.flow_minutos = 10.0
        self.acummulated_volume = 0.0
        self.producer = None
        
    def iniciar(self):
        self.running = True
        try:
            self.producer = Producer({'bootstrap.servers': self.broker_address})
            print(f"[ENGINE {self.ws_id}] Productor Kafka conectado a {self.broker_address}")
        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Error conectando Productor Kafka: {e}")
            sys.exit(1)

        threading.Thread(target=self.capturar_teclado, daemon=True).start()
        threading.Thread(target=self.consumir_ordenes, daemon=True).start()
        threading.Thread(target=self.bucle_telemetria, daemon=True).start()
        print(f"[ENGINE {self.ws_id}] Iniciado. Pulsa ENTER para alternar OK/KO")
        while self.running:
            if self.conectar_monitor():
                print(f"[ENGINE {self.ws_id}] Conectado al Monitor. Esperando chequeos de salud...")
                self.escuchar_monitor()
            else:
                time.sleep(2)
        
    def conectar_monitor(self):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(self.monitor_address)
            self.client_socket = client
            return True
        except (ConnectionRefusedError, socket.error):
            print(f"[ENGINE] Error: Monitor no disponible en {self.monitor_address}")
            return False
        
    def escuchar_monitor(self):
        try:
            while self.running:
                datos = self.client_socket.recv(1024)
                if not datos:
                    print("[ENGINE] El Monitor ha cerrado la conexión.")
                    break
                
                mensaje = protocol.extraerMensaje(datos)
                if mensaje == "CHECK_HEALTH":
                    respuesta = protocol.empaquetarCadena(self.estado)
                    self.client_socket.sendall(respuesta)
        except (ConnectionResetError, BrokenPipeError):
            print("[ENGINE] Conexión perdida con el Monitor.")
        except Exception as e:
            if self.running:
                print(f"[ENGINE] Error comunicando con Monitor: {e}")
        finally:
            if self.client_socket:
                self.client_socket.close()
            self.client_socket = None

    def capturar_teclado(self):
        while self.running:
            try:
                input() 
                if self.estado == "OK":
                    self.estado = "KO"
                    print("\n[ENGINE] ¡Avería detectada!")
                else:
                    self.estado = "OK"
                    print("\n[ENGINE] Avería reparada.")
            except EOFError:
                break

    def consumir_ordenes(self):
        consumer = Consumer({
            'bootstrap.servers': self.broker_address,
            'group.id': f'engine-group-{self.ws_id}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        consumer.subscribe(['wm-orders'])
        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    datos = json.loads(msg.value().decode('utf-8'))
                    if datos.get("ws_id") != self.ws_id:
                        continue
                    orden = datos.get("order")
                    print(f"[ENGINE {self.ws_id}] Orden Kafka recibida: {orden}")
                    if orden == "START" and self.estado == "OK":
                        self.is_watering = True
                        self.operator = datos.get("operator_id")
                        print(f"[ENGINE {self.ws_id}] Abriendo válvula. Riego iniciado.")
                    elif orden == "STOP":
                        self.detener_riego()
                        pass
                    elif orden == "ACTIVATE":
                        print(f"[ENGINE {self.ws_id}] Estación reactivada por Central.")
        finally:
            consumer.close()

    def detener_riego(self):
        if self.is_watering:
            self.is_watering = False
            self.operator = None
            print(f"[ENGINE {self.ws_id}] Cerrando válvula. Riego detenido.")

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def bucle_telemetria(self):
        while self.running:
            if self.is_watering and self.estado == "OK":
                litros_segundo = self.flow_minutos / 60.0
                self.acummulated_volume += litros_segundo
                telemetria = {
                    "ws_id": self.ws_id,
                    "flow": self.flow_minutos,
                    "accumulated_volume": round(self.acummulated_volume, 2),
                    "operator_id": self.operator
                }
                if self.producer:
                    self.producer.produce('wm-telemetry', value=json.dumps(telemetria).encode('utf-8'), callback=self.delivery_report)
                    self.producer.poll(0)
                    print(f"[TELEMETRIA] Regando... Caudal: {self.flow_minutos}L/min | Acumulado: {telemetria['accumulated_volume']}L")
            time.sleep(1)
        

def main():
    if len(sys.argv) == 4:
        broker_address = sys.argv[1]  
        monitor_address = sys.argv[2]
        monitor_ip, monitor_port = monitor_address.split(":")
        monitor_address = (monitor_ip, int(monitor_port))

        ws_id = sys.argv[3]
    else:
        print("Uso: python wm_ws_e.py <ip:puerto_broker> <ip:puerto_monitor> <ws_id>")
        sys.exit(1)

    engine = WM_WS_E(broker_address, monitor_address, ws_id)
    engine.iniciar()

if __name__ == "__main__": 
    main()