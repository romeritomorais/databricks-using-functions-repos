from json import loads
from pyspark.sql.types import StructType as struct

def schema(line: str):
    schema = f"schemas/{line}"
    with open(schema, "r") as file:
        get = file.read()
        estrutura = struct.fromJson(loads(get))
    return estrutura


def read(arquivo: str, schema: str, formato: str, separador: str, engine: object):
    dataframe = (
        engine.read.format(formato)
        .schema(schema)
        .option("header", "true")
        .option("delimiter", f"{separador}")
        .option("inferSchema", "false")
        .option("path", f"{arquivo}")
        .load()
    )
    return dataframe
