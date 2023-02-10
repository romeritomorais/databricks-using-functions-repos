def view(dataframe: object, descricao: str):
    return dataframe.createOrReplaceTempView(descricao)


def query(dataframe: object, describe: str, file: str, engine: object):
    view(dataframe, describe)
    with open(f"{file}") as load:
        query = load.read()
        queryObject = engine.sql(query)
    return queryObject
