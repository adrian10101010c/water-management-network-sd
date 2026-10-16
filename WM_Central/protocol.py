STX = b"\x02"
ETX = b"\x03"
ACK = b"\x06"
NACK = b"\x15"
SEP = "#"

def calcularLRC(data: bytes) -> bytes:
	lrc = 0
	for byte in data:
		lrc ^= byte
	return bytes([lrc])

def empaquetarCadena(cadena: str) -> bytes:
	data = cadena.encode("utf-8")
	LRC = calcularLRC(data)
	return STX + data + ETX + LRC

def extraerMensaje(message: bytes) -> str | None:
	if len(message) < 4 or message[0:1] != STX:
		return None
	try:
		etx_index = message.rindex(ETX)
	except ValueError:
		return None
	data = message[1:etx_index]
	LRC = message[etx_index+1:etx_index+2]
	if calcularLRC(data) != LRC:
		return None
	try:
		return data.decode("utf-8")
	except UnicodeDecodeError:
		return None