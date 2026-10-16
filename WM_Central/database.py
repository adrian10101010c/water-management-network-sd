import sqlite3
DB_FILE="water_management.db"

def iniciarBD():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watering_stations (
            id TEXT PRIMARY KEY,
            location TEXT NOT NULL,
            status TEXT NOT NULL,
            flow REAL DEFAULT 0.0,
            accumulated_volume INTEGER DEFAULT 0,
            operator TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operators (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL
        )
    ''')

    cursor.execute("UPDATE watering_stations SET status = 'DISCONNECTED'")
    conn.commit()
    conn.close()

def registrarEstacion(ws_id: str, location: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO watering_stations (id, location, status, flow, accumulated_volume)
        VALUES (?, ?, 'AVAILABLE', 0.0, 0)
        ON CONFLICT(id) DO UPDATE SET location=excluded.location, status='AVAILABLE', flow=0.0
    ''', (ws_id, location))
    conn.commit()
    conn.close()

def actualizarEstado(ws_id: str, status: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE watering_stations
        SET status = ?
        WHERE id = ?
    ''', (status, ws_id))
    conn.commit()
    conn.close()

def actualizarTelemetria(ws_id: str, flow: float, accumulated_volume: int, operator_id: str = None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE watering_stations
        SET flow = ?, accumulated_volume = ?, operator = ?
        WHERE id = ?
    ''', (flow, accumulated_volume, operator_id, ws_id))
    conn.commit()
    conn.close()

def obtenerEstaciones():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM watering_stations')
    estaciones = cursor.fetchall()
    conn.close()
    return estaciones