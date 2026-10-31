import sys
import socket
import threading
import time
import json
import database
import protocol
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from flask import Flask, render_template, jsonify
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app = Flask(__name__)
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stations')
def get_stations():
    estacionesBD = database.obtenerEstaciones()
    estaciones = []
    for ws in estacionesBD:
        estaciones.append({
            "id": ws[0],
            "location": ws[1],
            "status": ws[2],
            "flow": ws[3],
            "accumulated_volume": ws[4],
            "operator": ws[5]
        })
    return jsonify(estaciones)

def iniciar_servidor_web(puerto=8080):
    app.run(host='0.0.0.0', port=puerto, debug=False, use_reloader= False)

class WM_Central:
    def __init__(self, socket_port, broker_address):
        self.socket_port = socket_port
        self.broker_address = broker_address
        self.server_socket = None
        self.running = False
        self.producer = None

    def iniciar(self):
        self.running = True
        database.iniciarBD()
        print(f"[CENTRAL] Base de datos inicializada")
        self.configurar_productor()
        threading.Thread(target=self.escuchar_sockets, daemon=True).start()
        threading.Thread(target=self.consumir_peticiones, daemon=True).start()
        threading.Thread(target=self.consumir_telemetria, daemon=True).start()
        threading.Thread(target=iniciar_servidor_web, args=(8080,), daemon=True).start()
        print("[CENTRAL] Panel web de monitorización activo en http://localhost:8080")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[CENTRAL] Apagando servidor central...")
            self.running = False
        finally:
            if self.server_socket:
                self.server_socket.close()
            if self.producer:
                self.producer.flush()

    def escuchar_sockets(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.socket_port))
        self.server_socket.listen(20)
        print(f"[CENTRAL] Escuchando conexiones en el puerto {self.socket_port}...")
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"[CENTRAL] Conexión entrante desde {addr}")
                threading.Thread(target=self.manejar_estacion, args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                print(f"[CENTRAL] Error al aceptar conexión: {e}")
                break

    def manejar_estacion(self, client_socket, addr):
        ws_id = None
        try:
            while self.running:
                datos = client_socket.recv(1024)
                if not datos:
                    print(f"[CENTRAL] La estación en {addr} se ha desconectado.")
                    break

                mensaje = protocol.extraerMensaje(datos)
                if mensaje is None:
                    print(f"[CENTRAL] Mensaje corrupto (LRC inválido) desde {addr}. Enviando NACK.")
                    client_socket.sendall(protocol.NACK)
                    continue

                client_socket.sendall(protocol.ACK)
                print(f"[CENTRAL] Mensaje recibido de {addr}: {mensaje}")

                respuesta, id_detectado = self.procesar_mensaje(mensaje)
                if id_detectado:
                    ws_id = id_detectado

                if respuesta:
                    trama_respuesta = protocol.empaquetarCadena(respuesta)
                    client_socket.sendall(trama_respuesta)

        except (ConnectionResetError, BrokenPipeError):
            print(f"[CENTRAL] Conexión perdida con {addr}.")
        except Exception as e:
            print(f"[CENTRAL] Error con la estación {addr}: {e}")
        finally:
            if ws_id:
                database.actualizarEstado(ws_id, "DISCONNECTED")
                print(f"[CENTRAL] Estación {ws_id} marcada como DISCONNECTED en la BD.")
            client_socket.close()
        
    def procesar_mensaje(self, mensaje):
        partesMensaje = mensaje.split(protocol.SEP)
        comando = partesMensaje[0]

        if comando == "REG" and len(partesMensaje) >= 3:
            ws_id = partesMensaje[1]
            location = partesMensaje[2]
            respuesta = self.manejar_registro(ws_id, location)
            return respuesta, ws_id
        elif comando == "LEAK" and len(partesMensaje) >= 2:
            ws_id = partesMensaje[1]
            database.actualizarEstado(ws_id, "LEAK")
            print(f"[CENTRAL] Fuga detectada en la estación {ws_id}. Estado -> LEAK")
            return f"ACK_LEAK{protocol.SEP}{ws_id}", ws_id
        elif comando == "OK" and len(partesMensaje) >= 2:
            ws_id = partesMensaje[1]
            database.actualizarEstado(ws_id, "AVAILABLE")
            print(f"[CENTRAL] Avería resuelta en la estación {ws_id}. Estado -> AVAILABLE")
            return f"ACK_OK{protocol.SEP}{ws_id}", ws_id
        else:
            print(f"[CENTRAL] Comando desconocido o incompleto: {mensaje}")
            return "ERROR#COMANDO_DESCONOCIDO", None

    def configurar_productor(self):
        try:
            self.producer = Producer({'bootstrap.servers': self.broker_address})
            print(f"[CENTRAL] Productor Kafka conectado a {self.broker_address}")
        except Exception as e:
            print(f"[CENTRAL] Error conectando Productor Kafka: {e}")
            sys.exit(1)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def consumir_peticiones(self):
        consumer = Consumer({
            'bootstrap.servers': self.broker_address,
            'group.id': 'central-requests-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        consumer.subscribe(['wm-requests'])
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
                    accion = datos.get("action", "REQUEST")
                    ws_id = datos.get("ws_id")
                    operator_id = datos.get("operator_id")
                    
                    if accion == "REQUEST":
                        duracion = datos.get("duration", 60)
                        print(f"\n[CENTRAL] Petición recibida: {operator_id} -> {ws_id} ({duracion}s)")
                        self.manejar_peticion_riego(operator_id, ws_id, duracion)
                        
                    elif accion == "STOP":
                        volumen = datos.get("accumulated_volume", 0)
                        database.finalizarRiegoEstacion(ws_id) 
                        print(f"\n[CENTRAL] Riego finalizado en {ws_id}. Volumen total: {volumen}L")
                        
                        resumen = {
                            "operator_id": operator_id,
                            "ws_id": ws_id,
                            "status": "FINISHED",
                            "msg": f"Riego completado. Volumen consumido: {volumen}L"
                        }
                        if self.producer:
                            self.producer.produce('wm-responses', value=json.dumps(resumen).encode('utf-8'), callback=self.delivery_report)
                            self.producer.poll(0)
        finally:
            consumer.close()

    def consumir_telemetria(self):
        consumer = Consumer({
            'bootstrap.servers': self.broker_address,
            'group.id': 'central-requests-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        })
        consumer.subscribe(['wm-telemetry'])
        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition reached
                        print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    datos = json.loads(msg.value().decode('utf-8'))
                    ws_id = datos.get("ws_id")
                    flow = datos.get("flow")
                    accumulated_volume = datos.get("accumulated_volume")
                    operator_id = datos.get("operator_id")
                    database.actualizarTelemetria(ws_id, flow, accumulated_volume, operator_id)
        finally:
            consumer.close()

    def manejar_registro(self, ws_id: str, location: str):
        database.registrarEstacion(ws_id, location)
        print(f"[CENTRAL] Estación '{ws_id}' ({location}) registrada y disponible.")
        return f"REG_OK{protocol.SEP}{ws_id}"

    def manejar_peticion_riego(self, operator_id: str, ws_id: str, duracion: int):
        estacion = database.obtenerEstacion(ws_id)
        respuesta_fo = {"operator_id": operator_id, "ws_id": ws_id, "status": "DENIED", "msg": ""}
        if not estacion:
            motivo = f"Estación {ws_id} no encontrada."
            respuesta_fo["msg"] = motivo
        else:
            estado_actual = estacion[2]    
            if estado_actual != "AVAILABLE":
                motivo = f"La estación '{ws_id}' no está disponible. Estado actual: {estado_actual}"
                respuesta_fo["msg"] = motivo
            else:
                database.iniciarRiegoEstacion(ws_id, operator_id)
                respuesta_fo["status"] = "APPROVED"
                motivo = f"Riego autorizado para {operator_id} en {ws_id} durante {duracion} segundos."
                respuesta_fo["msg"] = motivo
                self.enviar_orden_a_estacion(ws_id, "START", duration=duracion, operator_id=operator_id)

        print(f"[CENTRAL -> FO] Respuesta a {operator_id}: {respuesta_fo['status']} - {respuesta_fo['msg']}")

        if self.producer:
            self.producer.produce('wm-responses', value=json.dumps(respuesta_fo).encode('utf-8'), callback=self.delivery_report)
            self.producer.poll(0)

    def enviar_orden_a_estacion(self, ws_id: str, orden: str, **kafkaargs):
        orden = {"ws_id": ws_id, "order": orden}
        orden.update(kafkaargs)
        if self.producer:
            self.producer.produce('wm-orders', value=json.dumps(orden).encode('utf-8'), callback=self.delivery_report)
            self.producer.poll(0)
            print(f"[CENTRAL -> KAFKA] Orden enviada a {ws_id}: {orden}")

    def forzar_inicio_riego(self, ws_id: str, operator_id: str, duracion: int = 60):
        print(f"[CENTRAL] Orden administrativa: Forzar inicio de riego en {ws_id}")
        self.manejar_peticion_riego("CENTRAL_ADMIN", ws_id, duracion)

    def forzar_bloqueo_estacion(self, ws_id: str):
        database.finalizarRiegoEstacion(ws_id, status="OUT_OF_SERVICE")
        print(f"[CENTRAL] Estación {ws_id} bloqueada (Estado -> 'OUT_OF_SERVICE'.)")
        self.enviar_orden_a_estacion(ws_id, "STOP")

    def forzar_activación_estacion(self, ws_id: str):
        database.actualizarEstado(ws_id, "AVAILABLE")
        print(f"[CENTRAL] Estación {ws_id} reactivada (Estado -> 'AVAILABLE')")
        self.enviar_orden_a_estacion(ws_id, "ACTIVATE")

def main():
        if len(sys.argv) < 3:
            print("Uso: python wm_central.py <puerto_sockets> <ip:puerto_broker>")
            sys.exit(1)

        socket_port = int(sys.argv[1])
        broker_address = sys.argv[2]

        central = WM_Central(socket_port, broker_address)
        central.iniciar()

if __name__ == "__main__":
    main()