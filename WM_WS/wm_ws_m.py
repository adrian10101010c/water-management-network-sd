import sys
import socket
import time
import protocol

class WM_WS_M:
    def __init__(self, engine_port, central_address, ws_id, location):
        self.engine_port = engine_port
        self.central_address = central_address
        self.ws_id = ws_id
        self.location = location

        self.central_socket = None

        self.running = False

    def iniciar(self):
        self.running = True
        if self.conectar_central():
            pass
    

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
            respuesta = protocol.extraerMensaje(datos)
            print(f"[MONITOR] Respuesta de CENTRAL: {respuesta}")
            return True

        except Exception as e:
            print(f"[MONITOR] Error comunicando con CENTRAL: {e}")
            return False
    
def main():
    if  (len(sys.argv) == 5):
        engine_port = int(sys.argv[1])
        central_address = sys.argv[2]
        ws_id = sys.argv[3]

        central_ip, central_port = central_address.split(":")
        central_port = int(central_port)
        central_address = (central_ip, central_port)

        location = sys.argv[5]
        
        monitor = WM_WS_M(engine_port, central_address, ws_id, location)
        monitor.iniciar()

    else:
        print("Uso: python wm_ws_m.py <puerto_engine> <ip:puerto_central> <ws_id> <location>")
        sys.exit(1)

if __name__ == "__main__": 
    main()