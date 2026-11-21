# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · M2: ai_parse_document + ai_extract
# MAGIC
# MAGIC Esquema = nombres de campo de DocILE (21 de información clave + 12 de ítem de línea), sin ejemplos (zero-shot).
# MAGIC Celdas en orden. **Celda 2 procesa 1 documento** para revisar la salida antes de correr los 20.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 1: parsear los 20 PDF del piloto (una vez)
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2_parsed AS
# MAGIC SELECT
# MAGIC   regexp_extract(path, '([^/]+)\.pdf$', 1) AS docid,
# MAGIC   path,
# MAGIC   ai_parse_document(content, map('version', '2.0')) AS parsed,
# MAGIC   current_timestamp() AS parsed_at
# MAGIC FROM read_files('/Volumes/workspace/invoice_extraction_benchmark/docile_pdfs/', format => 'binaryFile');
# MAGIC
# MAGIC SELECT count(*) AS documentos FROM workspace.invoice_extraction_benchmark.m2_parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 2: extraer 1 documento (prueba)
# MAGIC SELECT
# MAGIC   docid,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{"document_id": {"type": "string", "description": "Invoice/document number"}, "date_issue": {"type": "string", "description": "Issue date"}, "date_due": {"type": "string", "description": "Due date"}, "order_id": {"type": "string", "description": "Order number referenced by the document"}, "customer_id": {"type": "string", "description": "Customer/account number"}, "customer_order_id": {"type": "string", "description": "Customer purchase order number"}, "vendor_name": {"type": "string", "description": "Issuer/vendor name"}, "vendor_address": {"type": "string", "description": "Issuer/vendor address"}, "vendor_tax_id": {"type": "string", "description": "Vendor tax ID"}, "vendor_email": {"type": "string", "description": "Vendor email"}, "customer_billing_name": {"type": "string", "description": "Bill-to name"}, "customer_billing_address": {"type": "string", "description": "Bill-to address"}, "customer_delivery_name": {"type": "string", "description": "Ship-to name"}, "customer_delivery_address": {"type": "string", "description": "Ship-to address"}, "amount_total_net": {"type": "string", "description": "Total before tax"}, "amount_total_tax": {"type": "string", "description": "Total tax"}, "amount_total_gross": {"type": "string", "description": "Total including tax"}, "amount_due": {"type": "string", "description": "Amount due / balance"}, "amount_paid": {"type": "string", "description": "Amount already paid"}, "currency_code_amount_due": {"type": "string", "description": "Currency of the amount due"}, "payment_terms": {"type": "string", "description": "Payment terms"}, "line_items": {"type": "array", "description": "One object per table row", "items": {"type": "object", "properties": {"line_item_position": {"type": "string", "description": "Row number"}, "line_item_date": {"type": "string", "description": "Date of the item/service"}, "line_item_code": {"type": "string", "description": "Item code/SKU"}, "line_item_description": {"type": "string", "description": "Item description"}, "line_item_quantity": {"type": "string", "description": "Quantity"}, "line_item_units_of_measure": {"type": "string", "description": "Unit of measure"}, "line_item_unit_price_net": {"type": "string", "description": "Unit price before tax"}, "line_item_unit_price_gross": {"type": "string", "description": "Unit price"}, "line_item_amount_net": {"type": "string", "description": "Line amount before tax"}, "line_item_amount_gross": {"type": "string", "description": "Line amount"}, "line_item_currency": {"type": "string", "description": "Currency"}, "line_item_person_name": {"type": "string", "description": "Person name in the row"}}}}}',
# MAGIC     map('version', '2.1',
# MAGIC         'enableCitations', 'true',
# MAGIC         'enableConfidenceScores', 'true',
# MAGIC         'instructions', 'Extract each value exactly as printed in the document, without reformatting dates or numbers. Omit fields that are not present.')
# MAGIC   ) AS extracted
# MAGIC FROM workspace.invoice_extraction_benchmark.m2_parsed
# MAGIC WHERE docid = 'f7b33ee579c24727a2a8923f';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 3: extraer los 20 documentos y guardar
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2_extracted AS
# MAGIC SELECT
# MAGIC   docid,
# MAGIC   current_timestamp() AS extracted_at,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{"document_id": {"type": "string", "description": "Invoice/document number"}, "date_issue": {"type": "string", "description": "Issue date"}, "date_due": {"type": "string", "description": "Due date"}, "order_id": {"type": "string", "description": "Order number referenced by the document"}, "customer_id": {"type": "string", "description": "Customer/account number"}, "customer_order_id": {"type": "string", "description": "Customer purchase order number"}, "vendor_name": {"type": "string", "description": "Issuer/vendor name"}, "vendor_address": {"type": "string", "description": "Issuer/vendor address"}, "vendor_tax_id": {"type": "string", "description": "Vendor tax ID"}, "vendor_email": {"type": "string", "description": "Vendor email"}, "customer_billing_name": {"type": "string", "description": "Bill-to name"}, "customer_billing_address": {"type": "string", "description": "Bill-to address"}, "customer_delivery_name": {"type": "string", "description": "Ship-to name"}, "customer_delivery_address": {"type": "string", "description": "Ship-to address"}, "amount_total_net": {"type": "string", "description": "Total before tax"}, "amount_total_tax": {"type": "string", "description": "Total tax"}, "amount_total_gross": {"type": "string", "description": "Total including tax"}, "amount_due": {"type": "string", "description": "Amount due / balance"}, "amount_paid": {"type": "string", "description": "Amount already paid"}, "currency_code_amount_due": {"type": "string", "description": "Currency of the amount due"}, "payment_terms": {"type": "string", "description": "Payment terms"}, "line_items": {"type": "array", "description": "One object per table row", "items": {"type": "object", "properties": {"line_item_position": {"type": "string", "description": "Row number"}, "line_item_date": {"type": "string", "description": "Date of the item/service"}, "line_item_code": {"type": "string", "description": "Item code/SKU"}, "line_item_description": {"type": "string", "description": "Item description"}, "line_item_quantity": {"type": "string", "description": "Quantity"}, "line_item_units_of_measure": {"type": "string", "description": "Unit of measure"}, "line_item_unit_price_net": {"type": "string", "description": "Unit price before tax"}, "line_item_unit_price_gross": {"type": "string", "description": "Unit price"}, "line_item_amount_net": {"type": "string", "description": "Line amount before tax"}, "line_item_amount_gross": {"type": "string", "description": "Line amount"}, "line_item_currency": {"type": "string", "description": "Currency"}, "line_item_person_name": {"type": "string", "description": "Person name in the row"}}}}}',
# MAGIC     map('version', '2.1',
# MAGIC         'enableCitations', 'true',
# MAGIC         'enableConfidenceScores', 'true',
# MAGIC         'instructions', 'Extract each value exactly as printed in the document, without reformatting dates or numbers. Omit fields that are not present.')
# MAGIC   ) AS extracted
# MAGIC FROM workspace.invoice_extraction_benchmark.m2_parsed;