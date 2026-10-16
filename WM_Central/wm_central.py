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
                client_thread = threading.Thread(target=self.manejar_cliente, args=(client_socket, addr), daemon=True)
                client_thread.start()
            except Exception as e:
                print(f"[CENTRAL] Error al aceptar conexión: {e}")
                break

    def manejar_estacion(self):
        pass

    def procesar_mensaje(self):
        pass

    def manejar_registro(self):
        pass

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