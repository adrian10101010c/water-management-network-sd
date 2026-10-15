import sys
import protocol

class vm_central:
    def __init__(self, socket_port, broker_ip, broker_port):
        self.socket_port = socket_port
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.running = False

    def iniciar(self):
        if not self.running:
            try:
                self.running = True
                while self.running:
                    pass
            except:
                self.running = False
            finally:
                pass

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
        ip_broker, puerto_broker = sys.argv[2].split(":", maxsplit=1)

        central = vm_central(socket_port, ip_broker, puerto_broker)
        central.iniciar()
