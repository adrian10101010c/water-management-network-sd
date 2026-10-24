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

    def iniciar(self):
        self.running = True
        self.conectar_monitor()
        while self.running:
            time.sleep(1)
        
    def conectar_monitor(self):
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(self.monitor_address)
            self.client_socket = client
            return True
        except ConnectionRefusedError:
            print(f"[ENGINE] Error: Monitor no disponible en {self.monitor_address}")
            return False


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
        print("Uso: python wm_ws_m.py <puerto_engine> <ip:puerto_central> <ws_id> <location>")
        sys.exit(1)

if __name__ == "__main__": 
    main()