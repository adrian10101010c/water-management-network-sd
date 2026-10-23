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
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO watering_stations (id, location, status, flow, accumulated_volume)
            VALUES (?, ?, 'AVAILABLE', 0.0, 0)
            ON CONFLICT(id) DO UPDATE SET location=excluded.location, status='AVAILABLE', flow=0.0
        ''', (ws_id, location))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DATABASE] Error al registrar la estación: {e}")
    finally:
        if conn:
            conn.close()

def actualizarEstado(ws_id: str, status: str):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            
        UPDATE watering_stations
        SET status = ?
        WHERE id = ?
        ''', (status, ws_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DATABASE] Error al actualizar el estado de la estación: {e}")
    finally:
        if conn:
            conn.close()


def actualizarTelemetria(ws_id: str, flow: float, accumulated_volume: int, operator_id: str = None):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE watering_stations
            SET flow = ?, accumulated_volume = ?, operator = ?
            WHERE id = ?
        ''', (flow, accumulated_volume, operator_id, ws_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DATABASE] Error al actualizar la telemetría de la estación: {e}")
    finally:
        if conn:
            conn.close()

def obtenerEstaciones():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM watering_stations')
        estaciones = cursor.fetchall()
        conn.close()
        return estaciones
    except Exception as e:
        print(f"[DATABASE] Error al obtener las estaciones: {e}")
        return []
    finally:
        if conn:
            conn.close()

def obtenerEstacion(ws_id: str):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM watering_stations WHERE id = ?', (ws_id,))
        estacion = cursor.fetchone()
        conn.close()
        return estacion
    except Exception as e:
        print(f"[DATABASE] Error al obtener la estación: {e}")
        return None
    finally:
        if conn:
            conn.close()

def iniciarRiegoEstacion(ws_id: str, operator_id: str, initial_flow: float = 10.0):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE watering_stations
            SET status = 'WATERING', current_operator = ?, flow = ?
            WHERE id = ?
        ''', (operator_id, initial_flow, ws_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DATABASE] Error al iniciar el riego de la estación: {e}")
    finally:
        if conn:
            conn.close()

def finalizarRiegoEstacion(ws_id: str, status: str = "AVAILABLE"):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE watering_stations
            SET status = ?, flow = 0.0, current_operator = NULL
            WHERE id = ?
        ''', (status, ws_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DATABASE] Error al finalizar el riego de la estación: {e}")
    finally:
        if conn:
            conn.close()