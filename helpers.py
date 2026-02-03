# importar librerias 
from dbfread2 import DBF
import psycopg2
import psycopg2.extras
import time
import csv
from io import StringIO
from datetime import datetime
from typing import Iterable, List, Dict, Callable, Optional, Any
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
import unicodedata
import re

load_dotenv()

## constantes 
DATA_PATH = Path("DATA")
OBJECT_STORAGE_PATH = Path("OBJECT-STORAGE")
SCRIPTS = Path("SQL-SCRIPTS")
RESTORE = Path("0 - RESTORE")
PATRIMONIO_ESQUEMA = "bytsscom_bytsig"
AMBIENTES = Path("AMBIENTES")
BIENES = Path("BIENES")
BIENES_INFO = SCRIPTS/BIENES/"2 - INFO"
## bienes 

## function para conectarse a la base de datos
def connect_to_db(coneccion_tipo='local'):
    if coneccion_tipo == 'local':
        conn = psycopg2.connect(
            dbname= os.environ.get("DB_NAME_LOCAL"),
            user= os.environ.get("DB_USER_LOCAL"),
            password= os.environ.get("DB_PASSWORD_LOCAL"),
            host= os.environ.get("DB_HOST_LOCAL"),
            port= os.environ.get("DB_PORT_LOCAL", 5432)
        )
    if coneccion_tipo == 'prod':
        conn = psycopg2.connect(
            dbname= os.environ.get("DB_NAME"),
            user= os.environ.get("DB_USER"),
            password= os.environ.get("DB_PASSWORD"),
            host= os.environ.get("DB_HOST"),
            port= os.environ.get("DB_PORT", 5432)
        )
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cur

#funcion que verifica si existe un archivo y si existe lo elimina
def remove_file_if_exists(file_path: str) -> None:
    if os.path.exists(file_path):
        os.remove(file_path)

#funcion que crea un archivo sql con las sentencias sql
def generate_sql_file(file_path: str, sql_statements: Iterable[str]) -> None:
    remove_file_if_exists(file_path)
    # Si es string → convertir a lista con un solo elemento
    if isinstance(sql_statements, str):
        sql_statements = [sql_statements]
    with open(file_path, 'w', encoding='utf-8') as f:
        for sql in sql_statements:
            f.write(sql + '\n')



def buscar_item_por_numero_de_documento(numero:str,cursor) -> Optional[int]:
    numero =   numero.strip()
    query = "SELECT * FROM bytsscom_bytsig.item WHERE cod_item = %s "
    cursor.execute(query, (numero,))
    result = cursor.fetchone()
    print(result['id_item']) if result else print("No se encontró ningún item con ese numero.")
    return result['id_item'] if result else None

    

def ejecutar_sql_con_cursor(cursor, ruta_archivo_sql):
    """
    Ejecuta un script SQL desde un archivo utilizando un cursor de base de datos 
    ya existente.

    Esta función NO maneja la conexión ni las transacciones (commit/rollback).
    Eso debe hacerse fuera de la función.

    Args:
        cursor: Un objeto cursor de una conexión de base de datos activa (ej. psycopg2).
        ruta_archivo_sql (str): La ruta completa al archivo .sql a ejecutar.

    Returns:
        bool: True si la ejecución fue exitosa, False si ocurrió un error.
    """
    try:
        # 1. Leer el archivo SQL
        print(f"📄 Leyendo el script: {ruta_archivo_sql}")
        with open(ruta_archivo_sql, 'r', encoding='utf-8') as archivo:
            script_sql = archivo.read()
        
        # Valida que el script no esté vacío para evitar ejecuciones innecesarias
        if not script_sql.strip():
            print("⚠️ Advertencia: El archivo SQL está vacío. No se ejecutó nada.")
            return True

        # 2. Ejecutar el script con el cursor proporcionado
        print("🚀 Ejecutando script...")
        cursor.execute(script_sql)
        print(f"✅ Script {ruta_archivo_sql} ejecutado en la transacción actual.")
        
        return True

    except Exception as error:
        # 3. Informar del error (sin hacer rollback aquí)
        print(f"❌ Error al ejecutar el script '{ruta_archivo_sql}': {error}")
        return False
    


def generar_insert_sql(df, tabla, mapping):
    """
    Genera un INSERT multi-rows a partir de un DataFrame, usando un mapping
    entre nombres de columnas del DF y la tabla SQL.

    :param df: DataFrame con los datos
    :param tabla: Nombre de la tabla SQL destino
    :param mapping: Dict con {df_col: sql_col}
    :return: String con el SQL generado
    """
    sql_cols = []
    values_list = []

    # Las columnas destino en el orden del mapping
    for df_col, sql_col in mapping.items():
        sql_cols.append(sql_col)

    for _, row in df.iterrows():
        valores = []
        for df_col in mapping.keys():
            val = row[df_col]

            if pd.isna(val):
                valores.append("NULL")
                continue

            # Fecha → formato SQL estándar
            if hasattr(val, "strftime"):
                val = val.strftime("%Y-%m-%d")

            # Escape simple de comillas
            val = str(val).replace("'", "''")
            valores.append(f"'{val}'")

        values_list.append(f"({', '.join(valores)})")

    sql = (
        f"INSERT INTO {tabla} ({', '.join(sql_cols)}) VALUES\n"
        + ",\n".join(values_list)
        + ";"
    )

    return sql

def quitar_tildes_puntos(texto: str) -> str:
    if pd.isna(texto):
        return ""
    texto_sin_tildes = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto_limpio = re.sub(r'[^\w\s]', '', texto_sin_tildes)
    return re.sub(r'\s+', ' ', texto_limpio).strip()

def create_uuid() -> str:
    import uuid
    return str(uuid.uuid4())

def export_copy_sql(filename: str, table_name: str, df, columna_serial:Optional[str]):
    cols = ",".join(df.columns)
    if columna_serial:
        serial_reboot = f"""SELECT setval(
            pg_get_serial_sequence('bytsscom_bytsig.{table_name}', '{columna_serial}'),
            (SELECT COALESCE(MAX({columna_serial}), 1) FROM bytsscom_bytsig.{table_name}),
            TRUE
        );
        """
    
    with open(filename, 'w', encoding='utf-8') as f:
        # BEGIN opcional
        f.write("BEGIN;\n\n")

        if columna_serial:
            f.write("\n")
            f.write(serial_reboot)
            f.write("\n")

        # COPY header
        f.write(f"COPY {PATRIMONIO_ESQUEMA}.{table_name} ({cols}) FROM STDIN WITH CSV HEADER;\n")

        # Escribir encabezado CSV
        f.write(",".join(df.columns) + "\n")

        # Escribir filas
        for _, row in df.iterrows():
            # Convertir valores a texto CSV
            values = ["" if pd.isna(v) else str(v) for v in row.tolist()]
            f.write(",".join(values) + "\n")

        # Marca fin de datos COPY
        f.write("\\.\n\n")

        # Commit
        f.write("COMMIT;\n")

        if columna_serial:
            f.write("\n")
            f.write(serial_reboot)
            f.write("\n")

    print(f"✅ Archivo SQL generado: {filename}")

