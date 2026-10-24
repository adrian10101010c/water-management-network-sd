import sys
import socket
import time
import threading
import protocol

class WM_WS_E:
    def __init__(self, broker_address, monitor_address):
        self.broker_address = broker_address
        self.monitor_address = monitor_address
        self.client_socket = None
        self.running = False
        self.estado = "OK"
    def iniciar(self):
        self.running = True
        if self.conectar_monitor():
            print("[ENGINE] Conectado al Monitor. Esperando chequeos de salud...")
            
            threading.Thread(target=self.escuchar_monitor, daemon=True).start()
            threading.Thread(target=self.capturar_teclado, daemon=True).start()
            
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n[ENGINE] Apagando Engine...")
                self.running = False
            finally:
                if self.client_socket:
                    self.client_socket.close()
     
    def conectar_monitor(self):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(self.monitor_address)
            self.client_socket = client
            return True
        except ConnectionRefusedError:
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
        except Exception as e:
            if self.running:
                print(f"[ENGINE] Error comunicando con Monitor: {e}")

    def capturar_teclado(self):
        while self.running:
            try:
                input() 
                if self.estado == "OK":
                    self.estado = "KO"
                    print("\n[ENGINE] ¡Avería detectada!")
                else:
                    self.estado = "OK"
                    print("\n[ENGINE] Avería reparada")
            except EOFError:
                break
def main():
    if  (len(sys.argv) == 3):
        broker_address = sys.argv[1]
        monitor_address = sys.argv[2]

        broker_ip, broker_port = broker_address.split(":")
        broker_port = int(broker_port)
        broker_address = (broker_ip, broker_port)

        monitor_ip, monitor_port = monitor_address.split(":")
        monitor_port = int(monitor_port)
        monitor_address = (monitor_ip, monitor_port)

    
        
        engine = WM_WS_E(broker_address, monitor_address)
        engine.iniciar()

    else:
        print("Uso: python wm_ws_e.py <ip:puerto_broker> <ip:puerto_monitor>")
        sys.exit(1)

if __name__ == "__main__": 
    main()