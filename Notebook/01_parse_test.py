# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Prueba de ai_parse_document
# MAGIC
# MAGIC Comprueba si las funciones de IA están disponibles en Databricks Free Edition con un solo PDF del piloto.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT path, ai_parse_document(content) AS parsed
# MAGIC FROM read_files(
# MAGIC   '/Volumes/workspace/invoice_extraction_benchmark/docile_pdfs/f7b33ee579c24727a2a8923f.pdf',
# MAGIC   format => 'binaryFile'
# MAGIC )