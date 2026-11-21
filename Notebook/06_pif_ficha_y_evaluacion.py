# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Muestra PIF (275): ficha de datos y evaluación de la tesis
# MAGIC
# MAGIC | Parte | Qué hace | Cuándo se ejecuta |
# MAGIC |---|---|---|
# MAGIC | A · Ficha | Mide la muestra PIF desde las anotaciones (sin modelos) → `ieb_pif_ficha` | Siempre |
# MAGIC | B · Evaluación | M1 vs M2 v2 sobre los 275 PIF, montos con tolerancia 0,01, Holm → `ieb_pif_*` | Solo con `ejecutar_evaluacion = si`, **después** de las 3 constancias de expertos |
# MAGIC
# MAGIC No modifica `tools/` ni las tablas `ieb_eval_*` (evaluación exploratoria de 500 con tolerancia 0,50).

# COMMAND ----------

dbutils.widgets.dropdown("ejecutar_evaluacion", "no", ["no", "si"])
EJECUTAR_B = dbutils.widgets.get("ejecutar_evaluacion") == "si"

import json, os, re, sys, collections
V = "/Volumes/workspace/invoice_extraction_benchmark/eval_inputs"
S = "workspace.invoice_extraction_benchmark"
ANN = f"{V}/annotations_val"
ann = {f[:-5]: json.load(open(f"{ANN}/{f}", encoding="utf-8")) for f in os.listdir(ANN) if f.endswith(".json")}
G_all = {d["docid"]: d["grupo"] for d in json.load(open(f"{V}/val_grupos.json", encoding="utf-8"))}
PIF = sorted(d for d, a in ann.items() if a["metadata"].get("source") == "pif")
print("anotaciones:", len(ann), "| PIF:", len(PIF))

# COMMAND ----------

# Parte A · ficha de datos de la muestra PIF (solo anotaciones)
alnum = lambda s: re.sub(r"[^0-9a-z]", "", str(s).lower())
kile = [f for d in PIF for f in ann[d]["field_extractions"]]
lir = [f for d in PIF for f in ann[d]["line_item_extractions"]]
items = sum(len({f["line_item_id"] for f in ann[d]["line_item_extractions"]}) for d in PIF)
vend = {}
for d in PIF:
    v = next((f["text"] for f in ann[d]["field_extractions"] if f["fieldtype"] == "vendor_name"), None)
    vend[d] = alnum(v) if v else None
cnt = collections.Counter(v for v in vend.values() if v)
tipos = collections.Counter(ann[d]["metadata"]["document_type"] for d in PIF)
grupos = collections.Counter(G_all[d] for d in PIF)
ficha = [
    ("documentos", len(PIF)),
    ("paginas", sum(ann[d]["metadata"]["page_count"] for d in PIF)),
    ("disenos_cluster_id", len({ann[d]["metadata"]["cluster_id"] for d in PIF})),
    ("campos_clave", len(kile)), ("tipos_campos_clave", len({f["fieldtype"] for f in kile})),
    ("campos_lineas", len(lir)), ("tipos_campos_lineas", len({f["fieldtype"] for f in lir})),
    ("items_de_linea", items),
    ("emisores_distintos", len(cnt)), ("emisores_con_1_documento", sum(1 for c in cnt.values() if c == 1)),
    ("documentos_sin_emisor", sum(1 for v in vend.values() if not v)),
] + [(f"grupo_{k}", v) for k, v in sorted(grupos.items())] + [(f"tipo_{k}", v) for k, v in sorted(tipos.items())]
df = spark.createDataFrame([{"indicador": k, "valor": int(v)} for k, v in ficha], "indicador string, valor long")
df.write.mode("overwrite").saveAsTable(f"{S}.ieb_pif_ficha")
display(df)

# COMMAND ----------

# Parte B · evaluación de la tesis (apagada hasta tener las 3 constancias de expertos)
if not EJECUTAR_B:
    dbutils.notebook.exit("Parte A lista. Parte B no ejecutada (ejecutar_evaluacion = no).")

sys.path.insert(0, os.path.abspath("../tools"))
import eval_valores as EV
from comparar import comparar, doc_score, paired, boot_ci
from scipy.stats import shapiro

# Tolerancia de la tesis: todos los montos con |Δ| < 0,01 (no se edita tools/)
EV.TOL["importe"] = 0.01
_same = EV.same
def same_tesis(kind, pred, refs):
    if kind == "num":
        p = EV.num(pred)
        return p is not None and any(EV.num(r) is not None and abs(p - EV.num(r)) < 0.01 for r in refs)
    return _same(kind, pred, refs)
EV.same = same_tesis

m1 = json.load(open(f"{V}/m1_val_schema.json", encoding="utf-8"))
m2 = {r.docid: json.loads(r.j) for r in spark.sql(
    f"SELECT docid, to_json(extracted) AS j FROM {S}.m2v2_extracted_val").collect()}
G = {d: G_all[d] for d in PIF}
res, R1, R2 = comparar(ANN, m1, m2, G)

def holm(ps):
    o = sorted(range(len(ps)), key=lambda i: ps[i]); adj = [0.0] * len(ps); run = 0.0
    for r, i in enumerate(o):
        run = max(run, min(1.0, (len(ps) - r) * ps[i])); adj[i] = run
    return adj

def doc_total(r):
    n = sum(h["ref"] for h in r["head"].values()) + r["n_ref_lines"]
    ok = sum(h["ok"] for h in r["head"].values() if h["ref"]) + sum(l["completa"] for l in r["lines"])
    return ok / n if n else None

def doc_head(r):
    n = sum(h["ref"] for h in r["head"].values())
    return sum(h["ok"] for h in r["head"].values() if h["ref"]) / n if n else None

def prueba(f, ds):
    a = [f(R1[d]) for d in ds]; b = [f(R2[d]) for d in ds]
    out = paired(a, b)
    dif = [y - x for x, y in zip(a, b)]
    out["shapiro_W"] = shapiro(dif).statistic if len(set(dif)) > 2 else None
    return out

con_lineas = [d for d in PIF if R1[d]["n_ref_lines"]]
H = {
    "HG": prueba(doc_total, [d for d in PIF if doc_total(R1[d]) is not None]),
    "HE1": prueba(doc_head, [d for d in PIF if doc_head(R1[d]) is not None]),
    "HE2": prueba(doc_score, con_lineas),
    "HE3": prueba(doc_total, [d for d in PIF if G[d] == "0-shot" and doc_total(R1[d]) is not None]),
}
adj = holm([H[k]["p"] for k in H])
r4 = lambda x: None if x is None else round(float(x), 4)
hip = [{"hipotesis": k, "n": t["n"], "media_M1": r4(t["media_M1"]), "media_M2": r4(t["media_M2"]),
        "dif": r4(t["dif_media"]), "ic95_inf": r4(t["ic95_dif"][0]), "ic95_sup": r4(t["ic95_dif"][1]),
        "shapiro_W": r4(t["shapiro_W"]), "shapiro_p": r4(t["shapiro_p"]), "prueba": t["prueba"],
        "p": r4(t["p"]), "p_holm": r4(a), "decision": "se acepta" if a < 0.05 else "no se acepta"}
       for (k, t), a in zip(H.items(), adj)]
spark.createDataFrame(hip).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.ieb_pif_hipotesis")
display(spark.table(f"{S}.ieb_pif_hipotesis"))

# COMMAND ----------

# Cabecera por campo (McNemar + Holm) y descriptivos por grupo
cab = res["cabecera"]; adjc = holm([cab[k]["p"] for k in cab])
filas = [{"campo": k, "n": t["n"], "M1": r4(t["M1"]), "M2": r4(t["M2"]), "solo_M1": t["solo_M1"],
          "solo_M2": t["solo_M2"], "p": r4(t["p"]), "p_holm": r4(a)} for (k, t), a in zip(cab.items(), adjc)]
spark.createDataFrame(filas).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.ieb_pif_cabecera")

desc = []
for nm, R in (("M1", R1), ("M2", R2)):
    ag = res[f"agregado_{nm}"]
    desc += [{"modelo": nm, "grupo": "todos", "indicador": k, "valor": r4(ag[k])} for k in ag]
    for g in ("0-shot", "1-3", "4+"):
        ds = [d for d in PIF if G[d] == g]
        hs = [doc_head(R[d]) for d in ds if doc_head(R[d]) is not None]
        ls = [doc_score(R[d]) for d in ds if R[d]["n_ref_lines"]]
        desc += [{"modelo": nm, "grupo": g, "indicador": "documentos", "valor": float(len(ds))},
                 {"modelo": nm, "grupo": g, "indicador": "cabecera_media", "valor": r4(sum(hs) / len(hs))},
                 {"modelo": nm, "grupo": g, "indicador": "lineas_completas_media", "valor": r4(sum(ls) / len(ls))}]
    desc.append({"modelo": nm, "grupo": "todos", "indicador": "documentos_sin_salida",
                 "valor": float(sum(1 for d in PIF if not (m1 if nm == "M1" else m2)[d].get("response")))})
spark.createDataFrame(desc, "modelo string, grupo string, indicador string, valor double") \
    .write.mode("overwrite").saveAsTable(f"{S}.ieb_pif_descriptivos")
display(spark.table(f"{S}.ieb_pif_cabecera"))

# COMMAND ----------

# Alerta por anomalías matemáticas (solo descriptivo, sin prueba de hipótesis):
# línea con |cantidad × costo − importe| > 1 o |Σ importes − total| > 1 (tools/eval_valores.py)
al = []
for nm, R in (("M1", R1), ("M2", R2)):
    err = [d for d in PIF if R[d]["error"]]
    al.append({"modelo": nm, "documentos": len(PIF), "con_error": len(err),
               "con_error_alertados": sum(R[d]["alerta"] for d in err),
               "sin_error_alertados": sum(R[d]["alerta"] and not R[d]["error"] for d in PIF),
               "alertados": sum(R[d]["alerta"] for d in PIF)})
spark.createDataFrame(al).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.ieb_pif_alertas")
display(spark.table(f"{S}.ieb_pif_alertas"))
