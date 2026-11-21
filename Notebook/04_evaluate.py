# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Evaluación M1 vs M2 v2 sobre `val` (500)
# MAGIC
# MAGIC Compara, documento por documento, lo que extrae cada método con la anotación de DocILE.
# MAGIC
# MAGIC | Entrada | Origen |
# MAGIC |---|---|
# MAGIC | Anotaciones (referencia) | `/Volumes/workspace/invoice_extraction_benchmark/eval_inputs/annotations_val/` (500 JSON de DocILE, licencia solo investigación) |
# MAGIC | M1 · LayoutLMv3 | `/Volumes/workspace/invoice_extraction_benchmark/eval_inputs/m1_val_schema.json` (predicciones publicadas, adaptadas con `tools/m1_to_schema.py`) |
# MAGIC | M2 v2 · ai_parse_document + ai_extract | tabla `workspace.invoice_extraction_benchmark.m2v2_extracted_val` (notebook 03, celdas 3 y 4) |
# MAGIC | Grupo de diseño (0-shot, 1-3, 4+) | `/Volumes/workspace/invoice_extraction_benchmark/eval_inputs/val_grupos.json` |
# MAGIC
# MAGIC **Métrica por valor:** cantidad exacta, costo unitario |Δ| < 0.01, importe |Δ| < 0.50 (código en `tools/eval_valores.py`).
# MAGIC **Pruebas:** Shapiro-Wilk → Wilcoxon o t pareada (HE2, HE3), McNemar exacto por campo (HE1), IC 95 % bootstrap (`tools/comparar.py`).
# MAGIC
# MAGIC **Salida** (se recalcula completa en cada corrida; misma entrada, mismo resultado):
# MAGIC
# MAGIC | Tabla | Llave (`clave`) | Contenido |
# MAGIC |---|---|---|
# MAGIC | `ieb_eval_resumen` | `seccion` + `indicador` | cifras y pruebas de hipótesis |
# MAGIC | `ieb_eval_por_doc` | `docid` | indicadores por documento, sin texto |
# MAGIC | `ieb_eval_detalle` | `modelo` + `docid` + `linea` + `campo` | valor anotado vs. extraído (texto de facturas: privada) |
# MAGIC
# MAGIC En `clave` las partes van separadas por barra vertical. `linea` = 0 es la cabecera.

# COMMAND ----------

# Celda 1: código de evaluación del repositorio
import json, os, sys
sys.path.insert(0, os.path.abspath("../tools"))
from eval_valores import HEADER
from comparar import comparar, doc_score

# COMMAND ----------

# Celda 2: entradas
V = "/Volumes/workspace/invoice_extraction_benchmark/eval_inputs"
m1 = json.load(open(f"{V}/m1_val_schema.json", encoding="utf-8"))
G = {d["docid"]: d["grupo"] for d in json.load(open(f"{V}/val_grupos.json", encoding="utf-8"))}
m2 = {r.docid: json.loads(r.j) for r in spark.sql(
    "SELECT docid, to_json(extracted) AS j FROM workspace.invoice_extraction_benchmark.m2v2_extracted_val").collect()}
print(f"docs: grupos={len(G)}  M1={len(m1)}  M2={len(m2)}")
assert set(G) <= set(m1) and set(G) <= set(m2)

# COMMAND ----------

# Celda 3: evaluar y probar hipótesis
res, R1, R2 = comparar(f"{V}/annotations_val", m1, m2, G)
print(json.dumps(res, ensure_ascii=False, indent=1, default=lambda x: round(x, 4) if isinstance(x, float) else str(x)))

# COMMAND ----------

# Celda 4: resumen (cifras agregadas, publicable). clave = seccion|indicador
r4 = lambda x: None if x is None else round(float(x), 4)
rows = []
for k in ("cantidad", "costo_unitario", "importe", "completa"):
    rows.append(("lineas_emparejadas", k, r4(res["agregado_M1"][k]), r4(res["agregado_M2"][k]), None, None, None, None, None))
for k in ("lineas_anotadas", "lineas_extraidas", "emparejadas"):
    rows.append(("lineas_conteo", k, res["agregado_M1"][k], res["agregado_M2"][k], None, None, None, None, None))
for g, t in res["lineas"].items():
    rows.append(("HE2_HE3_lineas_por_documento", g, r4(t["media_M1"]), r4(t["media_M2"]), t["n"],
                 t["prueba"], r4(t["p"]), r4(t["ic95_dif"][0]), r4(t["ic95_dif"][1])))
for k, t in res["cabecera"].items():
    rows.append(("HE1_cabecera", k, r4(t["M1"]), r4(t["M2"]), t["n"], "McNemar exacto", r4(t["p"]), None, None))
for k in ("facturas_con_error", "señaladas", "falsas_alarmas"):
    rows.append(("HE5_alertas", k, res["alertas"]["M1"][k], res["alertas"]["M2"][k], None, None, None, None, None))
cols = "seccion indicador M1 M2 n prueba p ic95_inf ic95_sup".split()
resumen = spark.createDataFrame([{"clave": f"{r[0]}|{r[1]}", **dict(zip(cols, r))} for r in rows],
    "clave string, seccion string, indicador string, M1 double, M2 double, n int, prueba string, p double, ic95_inf double, ic95_sup double")
resumen.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.invoice_extraction_benchmark.ieb_eval_resumen")
display(resumen)

# COMMAND ----------

# Celda 5: por documento (solo indicadores, sin texto de facturas, publicable). clave = docid
pd_rows = []
for d in sorted(G):
    a, b = R1[d], R2[d]
    pd_rows.append({"clave": d, "docid": d, "grupo": G[d], "lineas_anotadas": a["n_ref_lines"],
        "M1_lineas_extraidas": a["n_pred_lines"], "M2_lineas_extraidas": b["n_pred_lines"],
        "M1_lineas_completas": r4(doc_score(a)), "M2_lineas_completas": r4(doc_score(b)),
        "M1_cabecera_ok": sum(h["ok"] for h in a["head"].values()), "M2_cabecera_ok": sum(h["ok"] for h in b["head"].values()),
        "cabecera_anotada": sum(h["ref"] for h in a["head"].values()),
        "M1_error": a["error"], "M1_alerta": a["alerta"], "M2_error": b["error"], "M2_alerta": b["alerta"]})
por_doc = spark.createDataFrame(pd_rows,
    "clave string, docid string, grupo string, lineas_anotadas int, M1_lineas_extraidas int, M2_lineas_extraidas int, "
    "M1_lineas_completas double, M2_lineas_completas double, M1_cabecera_ok int, M2_cabecera_ok int, "
    "cabecera_anotada int, M1_error boolean, M1_alerta boolean, M2_error boolean, M2_alerta boolean")
por_doc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.invoice_extraction_benchmark.ieb_eval_por_doc")
display(por_doc)

# COMMAND ----------

# Celda 6: detalle valor por valor (CONTIENE texto de facturas: solo hoja restringida)
# clave = modelo|docid|linea|campo  (linea 0 = cabecera; 1..n = líneas en el orden del emparejamiento)
s = lambda x: None if x is None else str(x)
ok = lambda x: None if x is None else ("sí" if x else "no")
det = []
for nm, R in (("M1", R1), ("M2", R2)):
    for d in sorted(G):
        for k, h in R[d]["head"].items():
            if h["ref"] or h["pred"]:
                det.append((f"{nm}|{d}|0|{k}", nm, d, G[d], 0, "cabecera", k, s(h["ref_txt"]), s(h["pred_txt"]),
                            ok(h["ok"]) if h["ref"] else "sin anotación"))
        for i, x in enumerate(R[d]["detail"], 1):
            a, b = x["ref"] or {}, x["pred"] or {}
            for k in ("descripcion", "cantidad", "costo_unitario", "importe"):
                det.append((f"{nm}|{d}|{i}|{k}", nm, d, G[d], i, "linea " + x["estado"], k, s(a.get(k)), s(b.get(k)),
                            ok(x.get(k)) if k != "descripcion" else None))
detalle = spark.createDataFrame(det, "clave string, modelo string, docid string, grupo string, linea int, tipo string, "
                                     "campo string, anotado string, extraido string, acierto string")
assert detalle.select("clave").distinct().count() == detalle.count(), "clave duplicada"
detalle.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.invoice_extraction_benchmark.ieb_eval_detalle")
print(detalle.count(), "filas de detalle")