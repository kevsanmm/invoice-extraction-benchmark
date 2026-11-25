# Databricks notebook source
# 08 · Resultados completos para el Cap. IV (complementa al 06; mismas funciones, misma semilla)
import json, os, sys, random
TOOLS = os.path.abspath("../tools")
sys.path.insert(0, TOOLS)
import eval_valores as EV
from comparar import comparar, doc_score, paired, boot_ci
from scipy.stats import shapiro, ttest_rel, wilcoxon

V = "/Volumes/workspace/invoice_extraction_benchmark/eval_inputs"
S = "workspace.invoice_extraction_benchmark"
ANN = f"{V}/annotations_val"
ann = {f[:-5]: json.load(open(f"{ANN}/{f}", encoding="utf-8")) for f in os.listdir(ANN) if f.endswith(".json")}
G_all = {d["docid"]: d["grupo"] for d in json.load(open(f"{V}/val_grupos.json", encoding="utf-8"))}
PIF = sorted(d for d, a in ann.items() if a["metadata"].get("source") == "pif")

# misma regla que el 06: todos los montos con |Δ| < 0,01
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

def doc_total(r):
    n = sum(h["ref"] for h in r["head"].values()) + r["n_ref_lines"]
    ok = sum(h["ok"] for h in r["head"].values() if h["ref"]) + sum(l["completa"] for l in r["lines"])
    return ok / n if n else None

def doc_head(r):
    n = sum(h["ref"] for h in r["head"].values())
    return sum(h["ok"] for h in r["head"].values() if h["ref"]) / n if n else None

def holm(ps):
    o = sorted(range(len(ps)), key=lambda i: ps[i]); adj = [0.0] * len(ps); run = 0.0
    for r, i in enumerate(o):
        run = max(run, min(1.0, (len(ps) - r) * ps[i])); adj[i] = run
    return adj

def prueba(f, ds):
    a = [f(R1[d]) for d in ds]; b = [f(R2[d]) for d in ds]
    out = paired(a, b, random.Random(42))
    dif = [y - x for x, y in zip(a, b)]
    if len(set(dif)) > 2:
        sw = shapiro(dif); out["shapiro_W"] = sw.statistic
    out["estadistico"] = (ttest_rel(b, a).statistic if out["prueba"] == "t pareada"
                          else (wilcoxon(b, a).statistic if any(dif) else None))
    return out

out = {"n_pif": len(PIF), "grupos": {g: sum(1 for d in PIF if G[d] == g) for g in ("0-shot", "1-3", "4+")}}

# hipótesis (misma definición que el 06)
H = {"HG": prueba(doc_total, [d for d in PIF if doc_total(R1[d]) is not None]),
     "HE1": prueba(doc_head, [d for d in PIF if doc_head(R1[d]) is not None]),
     "HE2": prueba(doc_score, [d for d in PIF if R1[d]["n_ref_lines"]]),
     "HE3": prueba(doc_total, [d for d in PIF if G[d] == "0-shot" and doc_total(R1[d]) is not None])}
for (k, t), a in zip(H.items(), holm([H[k]["p"] for k in H])):
    t["p_holm"] = a
out["hipotesis"] = H

# cabecera por campo: proporciones, McNemar + Holm, IC bootstrap de la diferencia
cab = res["cabecera"]
for (k, t), a in zip(cab.items(), holm([cab[k]["p"] for k in cab])):
    ds = [d for d in PIF if R1[d]["head"][k]["ref"]]
    t["p_holm"] = a
    t["ic95_dif"] = boot_ci([float(R1[d]["head"][k]["ok"]) for d in ds], [float(R2[d]["head"][k]["ok"]) for d in ds], random.Random(42))
out["cabecera"] = cab

# líneas: proporciones agregadas y IC bootstrap de documentos de la diferencia M2 - M1
def agreg(R, ds, k):
    if k == "cobertura":
        num = sum(len(R[d]["lines"]) for d in ds); den = sum(R[d]["n_ref_lines"] for d in ds)
    else:
        L = [l for d in ds for l in R[d]["lines"] if l[k] is not None]
        num = sum(l[k] for l in L); den = len(L)
    return num / den if den else None

lin = {}
docs_l = [d for d in PIF if R1[d]["n_ref_lines"]]
for k in ("cantidad", "costo_unitario", "importe", "completa", "cobertura"):
    rng = random.Random(42); diffs = []
    for _ in range(10000):
        s = [rng.choice(docs_l) for _ in docs_l]
        x1, x2 = agreg(R1, s, k), agreg(R2, s, k)
        if x1 is not None and x2 is not None:
            diffs.append(x2 - x1)
    diffs.sort()
    lin[k] = {"M1": agreg(R1, PIF, k), "M2": agreg(R2, PIF, k),
              "ic95_dif": (diffs[int(0.025 * len(diffs))], diffs[int(0.975 * len(diffs))])}
out["lineas"] = lin
out["agregado"] = {nm: res[f"agregado_{nm}"] for nm in ("M1", "M2")}

# por grupo de diseño
grp = {}
for nm, R in (("M1", R1), ("M2", R2)):
    for g in ("0-shot", "1-3", "4+"):
        ds = [d for d in PIF if G[d] == g]
        hs = [doc_head(R[d]) for d in ds if doc_head(R[d]) is not None]
        ls = [doc_score(R[d]) for d in ds if R[d]["n_ref_lines"]]
        grp.setdefault(g, {})[nm] = {"docs": len(ds), "cabecera_media": sum(hs) / len(hs),
                                     "lineas_completas_media": sum(ls) / len(ls) if ls else None}
out["grupos_res"] = grp
out["sin_salida"] = {nm: sum(1 for d in PIF if not (m1 if nm == "M1" else m2)[d].get("response")) for nm in ("M1", "M2")}
out["alertas"] = res["alertas"]
out["docs_con_lineas"] = len(docs_l)

# extracto para el Anexo 7: 10 filas, sin texto de documentos
ext = []
for d in random.Random(42).sample(PIF, 10):
    for nm, R in (("M1", R1), ("M2", R2)):
        r = R[d]
        ext.append({"docid": d, "grupo": G[d], "metodo": nm,
                    "cabecera_ok": sum(h["ok"] for h in r["head"].values() if h["ref"]),
                    "cabecera_anotada": sum(h["ref"] for h in r["head"].values()),
                    "lineas_anotadas": r["n_ref_lines"], "lineas_extraidas": r["n_pred_lines"],
                    "lineas_completas": sum(l["completa"] for l in r["lines"])})
out["extracto"] = ext

txt = json.dumps(out, ensure_ascii=False, indent=1, default=lambda x: float(x) if hasattr(x, "__float__") else str(x))
with open(f"{V}/resultados_tesis.json", "w", encoding="utf-8") as f:
    f.write(txt)
print(txt[:3000])
