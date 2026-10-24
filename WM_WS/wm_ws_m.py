import sys
import socket
import time
import threading
import protocol

class WM_WS_M:
    def __init__(self, engine_port, central_address, ws_id, location):
        self.engine_port = engine_port
        self.central_address = central_address
        self.ws_id = ws_id
        self.location = location
        self.central_socket = None
        self.running = False
        self.server_socket = None
        self.estado_anterior = "OK"

    def iniciar(self):
        self.running = True
        if self.conectar_central():
            print(f"[MONITOR] Estación activa. Manteniendo conexión con la central...")
            client_thread = threading.Thread(target=self.escuchar_sockets_engine, daemon=True)
            client_thread.start()
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("[CENTRAL] Apagando servidor central...")
                self.running = False
            finally:
                if self.server_socket:
                    self.server_socket.close()
    

    def conectar_central(self):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(self.central_address)
            self.central_socket = client


            msg = f"REG#{self.ws_id}#{self.location}"
            if self.enviar_a_central(msg):
                print(f"[MONITOR] Registrado con éxito en CENTRAL {self.central_address}")
                return True
            return False

        except ConnectionRefusedError:
            print(f"[MONITOR] Error: Central no disponible")

    def enviar_a_central(self, msg: str) -> bool:
        try:
            trama = protocol.empaquetarCadena(msg)
            self.central_socket.sendall(trama)

            datos = self.central_socket.recv(1024)
            if not datos or datos[0:1] == protocol.NACK:
                print("[MONITOR] CENTRAL rechazó el mensaje.")
                return False

            
            if datos.startswith(protocol.ACK):
                datos = datos[1:]

            if len(datos) > 0:
                respuesta = protocol.extraerMensaje(datos)
                print(f"[MONITOR] Respuesta de CENTRAL: {respuesta}")
            else:
                datos_trama = self.central_socket.recv(1024)
                respuesta = protocol.extraerMensaje(datos_trama)
                print(f"[MONITOR] Respuesta de CENTRAL: {respuesta}")

            return True
        
        except Exception as e:
            print(f"[MONITOR] Error comunicando con CENTRAL: {e}")
            return False

    def escuchar_sockets_engine(self):
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind(('', self.engine_port))
            self.server_socket.listen(20)
            print(f"[MONITOR] Escuchando conexiones en el puerto {self.engine_port}...")
            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    print(f"[MONITOR] Conexión entrante desde {addr}")
                    self.manejar_engine(client_socket, addr)
                except Exception as e:
                    print(f"[MONITOR] Error al aceptar conexión: {e}")
                
    def manejar_engine(self, client_socket, addr):
        client_socket.settimeout(1.0)
        try:
            while self.running:
                msg = protocol.empaquetarCadena("CHECK_HEALTH")
                client_socket.sendall(msg)

                try:
                    datos = client_socket.recv(1024)
                except socket.timeout:
                    print(f"[MONITOR] Timeout esperando al engine {addr}.")
                    break

                if not datos:
                    print(f"[MONITOR] El engine {addr} se ha desconectado.")
                    break
        
                respuesta = protocol.extraerMensaje(datos)
                if respuesta is None:
                    print(f"[MONITOR] Mensaje corrupto (LRC inválido) desde {addr}. Enviando NACK.")
                    client_socket.sendall(protocol.NACK)
                    continue

                if respuesta == "KO" and self.estado_anterior != "KO":
                    print("[MONITOR] Avería (KO) detectada. Notificando a CENTRAL...")
                    self.enviar_a_central(f"LEAK#{self.ws_id}")
                    self.estado_anterior = "KO"
                elif respuesta == "OK" and self.estado_anterior == "KO":
                    print("[MONITOR] Avería resuelta. Notificando a CENTRAL...")
                    self.enviar_a_central(f"OK#{self.ws_id}")
                    self.estado_anterior = "OK"              
        
                time.sleep(1)
        
        except ConnectionResetError:
            print(f"[MONITOR] Conexión perdida con {addr}.")
        except Exception as e:
            print(f"[MONITOR] Error con el engine {addr}: {e}")
        finally:
            client_socket.close()
            print("[MONITOR] Conexión cerrada. Notificando fuga a CENTRAL...")
            self.enviar_a_central(f"LEAK#{self.ws_id}")
            self.estado_anterior = "KO"


  

    
    
def main():
    if  (len(sys.argv) == 5):
        engine_port = int(sys.argv[1])
        central_address = sys.argv[2]
        ws_id = sys.argv[3]

        central_ip, central_port = central_address.split(":")
        central_port = int(central_port)
        central_address = (central_ip, central_port)

        location = sys.argv[4]
        
        monitor = WM_WS_M(engine_port, central_address, ws_id, location)
        monitor.iniciar()

    else:
        print("Uso: python wm_ws_m.py <puerto_engine> <ip:puerto_central> <ws_id> <location>")
        sys.exit(1)

if __name__ == "__main__": 
    main()