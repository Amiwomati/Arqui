import os
import mysql.connector
import datetime

# Obtener credenciales de la base de datos desde las variables de entorno
dbname = os.environ.get('DBNAME')
user = os.environ.get('DBUSER')
password = os.environ.get('DBPASSWORD')
host = os.environ.get('DBHOST')
port = os.environ.get('DBPORT')

# Funci贸n para calcular la edad a partir de la fecha de nacimiento
def calcular_edad(fecha_nacimiento):
    hoy = datetime.date.today()
    edad = hoy.year - fecha_nacimiento.year - ((hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
    return edad

try:
    # Conexi贸n a la base de datos
    conn = mysql.connector.connect(
            database=dbname,
            user=user,
            password=password,
            host=host,
            port=port
    )
    print("Conexi贸n establecida.")

    # Crear una nueva tabla llamada 'persona'
    create_table_query = """
    CREATE TABLE IF NOT EXISTS persona (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre VARCHAR(255),
        apellido VARCHAR(255),
        edad INT
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
        print("Tabla 'persona' creada.")

    # Consulta para obtener los datos de la tabla 'raw_data'
    query = "SELECT nombre, apellido, edad FROM raw_data"

    # Ejecutar la consulta
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

        # Calcular las edades y insertarlas en la nueva tabla 'persona'
        insert_query = "INSERT INTO persona (nombre, apellido, edad) VALUES (%s, %s, %s)"
        for row in rows:
            nombre, apellido, fecha_nacimiento = row
            edad_calculada = calcular_edad(fecha_nacimiento)
            cursor.execute(insert_query, (nombre, apellido, edad_calculada))
            print(f"Nombre: {nombre}, Apellido: {apellido}, Edad calculada: {edad_calculada}")

        conn.commit()
        print("Datos insertados en la tabla 'persona'.")

except mysql.connector.Error as err:
    print(f"Error al conectar a la base de datos: {err}")

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Conexi贸n cerrada.")