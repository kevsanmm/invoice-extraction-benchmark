# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · M2 v2: método de producción (esquema + validación matemática)
# MAGIC
# MAGIC Esquema genérico del revisor de facturas: cabecera con total del PDF y líneas con cantidad, costo unitario e importe numéricos.
# MAGIC **Regla:** se desarrolla y ajusta solo con `dev/` (partición train). `val_pilot/` y la validación completa se evalúan una vez, al final.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 1: parsear dev (20 docs de entrenamiento)
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_parsed_dev AS
# MAGIC SELECT
# MAGIC   regexp_extract(path, '([^/]+)\.pdf$', 1) AS docid,
# MAGIC   path,
# MAGIC   ai_parse_document(content, map('version', '2.0')) AS parsed,
# MAGIC   current_timestamp() AS parsed_at
# MAGIC FROM read_files('/Volumes/workspace/invoice_extraction_benchmark/docile_pdfs/dev/', format => 'binaryFile');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 2: extraer dev
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_extracted_dev AS
# MAGIC SELECT
# MAGIC   docid,
# MAGIC   current_timestamp() AS extracted_at,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{"invoice": {"type": "string", "description": "Numero de factura o documento del emisor"}, "invoice_date": {"type": "string", "description": "Fecha de emision tal como aparece"}, "due_date": {"type": "string", "description": "Fecha de vencimiento tal como aparece, si existe"}, "vendor_name": {"type": "string", "description": "Nombre del emisor del documento"}, "moneda": {"type": "string", "description": "Moneda del documento (codigo o simbolo)"}, "total_factura_pdf": {"type": "number", "description": "Total final declarado en el documento"}, "line_items": {"type": "array", "description": "Lineas de detalle del documento", "items": {"type": "object", "properties": {"item": {"type": "string", "description": "Codigo del producto o servicio, si existe"}, "descripcion": {"type": "string", "description": "Nombre/descripcion del producto o servicio"}, "cantidad": {"type": "number", "description": "Cantidad de unidades"}, "costo_unitario": {"type": "number", "description": "Precio unitario"}, "importe": {"type": "number", "description": "Importe total de la linea"}}}}}',
# MAGIC     map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', 'Documentos comerciales: facturas, ordenes y notas. Extrae cada linea de detalle de la tabla.')
# MAGIC   ) AS extracted
# MAGIC FROM workspace.invoice_extraction_benchmark.m2v2_parsed_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluación final (M2 v2 congelado el 08-nov-2026)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 3: parsear val (500 docs, evaluación final, una sola vez)
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_parsed_val AS
# MAGIC SELECT
# MAGIC   regexp_extract(path, '([^/]+)\.pdf$', 1) AS docid,
# MAGIC   path,
# MAGIC   ai_parse_document(content, map('version', '2.0')) AS parsed,
# MAGIC   current_timestamp() AS parsed_at
# MAGIC FROM read_files('/Volumes/workspace/invoice_extraction_benchmark/docile_pdfs/val/', format => 'binaryFile');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 4: extraer val (M2 v2 congelado)
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_extracted_val AS
# MAGIC SELECT
# MAGIC   docid,
# MAGIC   current_timestamp() AS extracted_at,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{"invoice": {"type": "string", "description": "Numero de factura o documento del emisor"}, "invoice_date": {"type": "string", "description": "Fecha de emision tal como aparece"}, "due_date": {"type": "string", "description": "Fecha de vencimiento tal como aparece, si existe"}, "vendor_name": {"type": "string", "description": "Nombre del emisor del documento"}, "moneda": {"type": "string", "description": "Moneda del documento (codigo o simbolo)"}, "total_factura_pdf": {"type": "number", "description": "Total final declarado en el documento"}, "line_items": {"type": "array", "description": "Lineas de detalle del documento", "items": {"type": "object", "properties": {"item": {"type": "string", "description": "Codigo del producto o servicio, si existe"}, "descripcion": {"type": "string", "description": "Nombre/descripcion del producto o servicio"}, "cantidad": {"type": "number", "description": "Cantidad de unidades"}, "costo_unitario": {"type": "number", "description": "Precio unitario"}, "importe": {"type": "number", "description": "Importe total de la linea"}}}}}',
# MAGIC     map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', 'Documentos comerciales: facturas, ordenes y notas. Extrae cada linea de detalle de la tabla.')
# MAGIC   ) AS extracted
# MAGIC FROM workspace.invoice_extraction_benchmark.m2v2_parsed_val;