import sys
import socket
import threading
import time
import database
import protocol

class WM_Central:
    def __init__(self, socket_port, broker_address):
        self.socket_port = socket_port
        self.broker_address = broker_address
        self.server_socket = None
        self.running = False

    def iniciar(self):
        self.running = True
        database.iniciarBD()
        print(f"[CENTRAL] Base de datos inicializada")
        socket_thread = threading.Thread(target=self.escuchar_sockets, daemon=True)
        socket_thread.start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[CENTRAL] Apagando servidor central...")
            self.running = False
        finally:
            if self.server_socket:
                self.server_socket.close()

    def escuchar_sockets(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('', self.socket_port))
        self.server_socket.listen(20)
        print(f"[CENTRAL] Escuchando conexiones en el puerto {self.socket_port}...")
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                print(f"[CENTRAL] Conexión entrante desde {addr}")
                client_thread = threading.Thread(target=self.manejar_estacion, args=(client_socket, addr), daemon=True)
                client_thread.start()
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

        except ConnectionResetError:
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
            return f"ACK_LEAK#{ws_id}", ws_id
        elif comando == "OK" and len(partesMensaje) >= 2:
            ws_id = partesMensaje[1]
            database.actualizarEstado(ws_id, "AVAILABLE")
            print(f"[CENTRAL] Avería resuelta en la estación {ws_id}. Estado -> AVAILABLE")
            return f"ACK_OK#{ws_id}", ws_id
        else:
            print(f"[CENTRAL] Comando desconocido o incompleto: {mensaje}")
            return "ERROR#COMANDO_DESCONOCIDO", None

    def manejar_registro(self, ws_id: str, location: str):
        database.registrarEstacion(ws_id, location)
        print(f"[CENTRAL] Estación '{ws_id}' ({location}) registrada y disponible.")
        return f"REG_OK#{ws_id}"

    def manejar_peticion_riego(self):
        pass

    def enviar_orden_a_estacion(self):
        pass

    def forzar_inicio_riego(self):
        pass

    def forzar_bloqueo_estacion(self):
        pass

    def forzar_activación_estacion(self):
        pass

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