# Databricks notebook source
# DBTITLE 1,Adiciona Caminho de Módulos Python 
import sys
import os
sys.path.append('/Workspace/Repos/romeritomorais@outlook.com.br/delta-live-tables/functions/ingestion')
sys.path.append('/Workspace/Repos/romeritomorais@outlook.com.br/delta-live-tables/functions/transformer')

# COMMAND ----------

# DBTITLE 1,Importação de Módulos
from loads import schema
from loads import read
from processing import view
from processing import query

# COMMAND ----------

Data = "/FileStore/tables/licitacoes_2022.csv"
FileSQL = "/Workspace/Repos/romeritomorais@outlook.com.br/delta-live-tables/sql/agrupado-por-tipo.sql"

# COMMAND ----------

# DBTITLE 1,Carrega schema de arquivo
SchLicitacao = schema("licitacoes.json")

# COMMAND ----------

# DBTITLE 1,Carrega dados
Licitacao = read(Data, SchLicitacao, "csv", ";", spark)

# COMMAND ----------

display(Licitacao)

# COMMAND ----------

# DBTITLE 1,Query que faz agrupamento pelo Tipo
ValorPorTipo = query(Licitacao,"TabelaLicitacao",FileSQL, spark)
display(ValorPorTipo)
