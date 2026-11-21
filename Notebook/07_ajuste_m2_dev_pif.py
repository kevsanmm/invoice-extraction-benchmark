# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Ajuste de M2 v2 con `dev_pif` (20 documentos PIF de entrenamiento)
# MAGIC
# MAGIC Corre M2 v2 **sin cambios** (mismo esquema e instrucción que el cuaderno 03) sobre `dev_pif` y lo evalúa con la regla de la tesis:
# MAGIC cantidad exacta y todos los montos con |Δ| < 0,01. Solo usa la partición de entrenamiento.
# MAGIC
# MAGIC | Salida | Contenido |
# MAGIC |---|---|
# MAGIC | `m2v2_parsed_dev_pif`, `m2v2_extracted_dev_pif` | Lectura y extracción de los 20 PDF |
# MAGIC | `ieb_devpif_resumen` | Aciertos por campo de cabecera y por valor de línea (cifras agregadas) |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 1: leer dev_pif
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_parsed_dev_pif AS
# MAGIC SELECT
# MAGIC   regexp_extract(path, '([^/]+)\.pdf$', 1) AS docid,
# MAGIC   path,
# MAGIC   ai_parse_document(content, map('version', '2.0')) AS parsed,
# MAGIC   current_timestamp() AS parsed_at
# MAGIC FROM read_files('/Volumes/workspace/invoice_extraction_benchmark/docile_pdfs/dev_pif/', format => 'binaryFile');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Celda 2: extraer dev_pif (M2 v2 sin cambios)
# MAGIC CREATE OR REPLACE TABLE workspace.invoice_extraction_benchmark.m2v2_extracted_dev_pif AS
# MAGIC SELECT
# MAGIC   docid,
# MAGIC   current_timestamp() AS extracted_at,
# MAGIC   ai_extract(
# MAGIC     parsed,
# MAGIC     '{"invoice": {"type": "string", "description": "Numero de factura o documento del emisor"}, "invoice_date": {"type": "string", "description": "Fecha de emision tal como aparece"}, "due_date": {"type": "string", "description": "Fecha de vencimiento tal como aparece, si existe"}, "vendor_name": {"type": "string", "description": "Nombre del emisor del documento"}, "moneda": {"type": "string", "description": "Moneda del documento (codigo o simbolo)"}, "total_factura_pdf": {"type": "number", "description": "Total final declarado en el documento"}, "line_items": {"type": "array", "description": "Lineas de detalle del documento", "items": {"type": "object", "properties": {"item": {"type": "string", "description": "Codigo del producto o servicio, si existe"}, "descripcion": {"type": "string", "description": "Nombre/descripcion del producto o servicio"}, "cantidad": {"type": "number", "description": "Cantidad de unidades"}, "costo_unitario": {"type": "number", "description": "Precio unitario"}, "importe": {"type": "number", "description": "Importe total de la linea"}}}}}',
# MAGIC     map('version', '2.1', 'enableConfidenceScores', 'true', 'instructions', 'Documentos comerciales: facturas, ordenes y notas. Extrae cada linea de detalle de la tabla.')
# MAGIC   ) AS extracted
# MAGIC FROM workspace.invoice_extraction_benchmark.m2v2_parsed_dev_pif;

# COMMAND ----------

# Celda 3: evaluar con la regla de la tesis (montos |Δ| < 0,01)
import json, os, sys
sys.path.insert(0, os.path.abspath("../tools"))
import eval_valores as EV

EV.TOL["importe"] = 0.01
_same = EV.same
def same_tesis(kind, pred, refs):
    if kind == "num":
        p = EV.num(pred)
        return p is not None and any(EV.num(r) is not None and abs(p - EV.num(r)) < 0.01 for r in refs)
    return _same(kind, pred, refs)
EV.same = same_tesis

V = "/Volumes/workspace/invoice_extraction_benchmark/eval_inputs"
S = "workspace.invoice_extraction_benchmark"
ids = [d["docid"] for d in json.load(open(f"{V}/dev_pif.json", encoding="utf-8"))]
m2 = {r.docid: json.loads(r.j) for r in spark.sql(
    f"SELECT docid, to_json(extracted) AS j FROM {S}.m2v2_extracted_dev_pif").collect()}
assert set(ids) == set(m2), (len(ids), len(m2))
R = {d: EV.evaluate(json.load(open(f"{V}/annotations_train/{d}.json", encoding="utf-8")), m2[d]["response"] or {}) for d in ids}

rows = []
for k in EV.HEADER:
    n = sum(r["head"][k]["ref"] for r in R.values()); ok = sum(r["head"][k]["ok"] for r in R.values() if r["head"][k]["ref"])
    rows.append(("cabecera", k, ok, n))
L = [l for r in R.values() for l in r["lines"]]
rows.append(("lineas", "anotadas", sum(r["n_ref_lines"] for r in R.values()), None))
rows.append(("lineas", "extraidas", sum(r["n_pred_lines"] for r in R.values()), None))
rows.append(("lineas", "emparejadas", len(L), None))
for k in ("cantidad", "costo_unitario", "importe", "completa"):
    v = [l[k] for l in L if l[k] is not None]
    rows.append(("lineas", k, sum(v), len(v)))
rows.append(("documentos", "sin_salida", sum(1 for d in ids if not m2[d].get("response")), len(ids)))
df = spark.createDataFrame([{"seccion": a, "indicador": b, "aciertos": int(c), "n": None if n is None else int(n),
                             "proporcion": None if not n else round(c / n, 4)} for a, b, c, n in rows],
                           "seccion string, indicador string, aciertos long, n long, proporcion double")
df.write.mode("overwrite").saveAsTable(f"{S}.ieb_devpif_resumen")
display(df)
