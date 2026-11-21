# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exportar resultados a Google Sheets
# MAGIC
# MAGIC Lee las tablas del notebook 04 y las sincroniza con dos hojas del Drive personal mediante una cuenta de servicio de Google.
# MAGIC
# MAGIC | Hoja | Acceso | Pestañas |
# MAGIC |---|---|---|
# MAGIC | `ieb_resultados_publicos` | cualquiera con el enlace (lector) | `leeme`, `cargas`, `resumen`, `por_documento` (cifras, sin texto de facturas) |
# MAGIC | `ieb_detalle_restringido` | solo el autor | `cargas`, `detalle` (valor anotado vs. extraído; licencia DocILE solo investigación) |
# MAGIC
# MAGIC **Llave única:** columna `clave`, primera de cada pestaña (partes separadas por barra vertical).
# MAGIC
# MAGIC | Pestaña | clave |
# MAGIC |---|---|
# MAGIC | `resumen` | `seccion` + `indicador` |
# MAGIC | `por_documento` | `docid` |
# MAGIC | `detalle` | `modelo` + `docid` + `linea` + `campo` (linea 0 = cabecera) |
# MAGIC
# MAGIC **Columnas de carga** (al final de cada fila): `hash_fila` (huella del contenido), `id_carga` (AAAAMMDD_HHMMSS) y `cargado_en` (hora de Lima).
# MAGIC
# MAGIC **Parámetro `modo`:**
# MAGIC - `incremental`: compara por `clave` y `hash_fila`. Inserta las filas nuevas, reemplaza las que cambiaron y deja las iguales con su carga original. Conserva las filas que ya no vienen en la fuente.
# MAGIC - `completo`: reescribe la pestaña entera; todas las filas quedan con la carga actual.
# MAGIC
# MAGIC Repetir la corrida en `incremental` sin cambios en la fuente no modifica ninguna fila (idempotente).
# MAGIC La pestaña `cargas` registra cada ejecución: filas nuevas, actualizadas, sin cambio, conservadas y total.
# MAGIC Para ver lo cargado en la última ejecución: filtrar `id_carga` por el último valor de `cargas`.
# MAGIC
# MAGIC **Credencial:** el JSON de `ieb-sheets-writer@invoice-extraction-benchmark.iam.gserviceaccount.com` está en el secret
# MAGIC `invoice-extraction-benchmark / gsheets_sa_json`. No aparece en el código ni en el repositorio.

# COMMAND ----------

# MAGIC %pip install -q gspread==6.1.4

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Celda 1: parámetros, marca de carga y conexión
import hashlib, json, math
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
from gspread.utils import ValueRenderOption

dbutils.widgets.dropdown("modo", "incremental", ["incremental", "completo"])
dbutils.widgets.text("hoja_publica", "1ol8jgRnQ9HZgfhB_4J-Iwp6cpdBMv7odHrgCKMRk4dc")
dbutils.widgets.text("hoja_restringida", "1EKJngJ5CcD_wFrw0g2W6msIYxlyW3VckRtC3TkBe7pg")
MODO = dbutils.widgets.get("modo")
ahora = datetime.now(ZoneInfo("America/Lima"))
ID_CARGA, CARGADO_EN = ahora.strftime("%Y%m%d_%H%M%S"), ahora.strftime("%Y-%m-%d %H:%M:%S")

gc = gspread.service_account_from_dict(json.loads(dbutils.secrets.get("invoice-extraction-benchmark", "gsheets_sa_json")))
pub = gc.open_by_key(dbutils.widgets.get("hoja_publica"))
res = gc.open_by_key(dbutils.widgets.get("hoja_restringida"))
print(f"modo={MODO}  id_carga={ID_CARGA}  cargado_en={CARGADO_EN} (Lima)")
print("conectado:", pub.title, "|", res.title)

# COMMAND ----------

# Celda 2: funciones de sincronización por llave
CARGA = ["hash_fila", "id_carga", "cargado_en"]

def limpio(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return v.item() if hasattr(v, "item") else v      # numpy -> python

def hoja(libro, nombre, filas, cols):
    try:
        return libro.worksheet(nombre)
    except gspread.WorksheetNotFound:
        ws = libro.add_worksheet(nombre, rows=max(filas, 2), cols=cols)
        for x in libro.worksheets():                    # quita la pestaña vacía por defecto
            if x.title in ("Hoja 1", "Sheet1"):
                libro.del_worksheet(x)
        return ws

def escribir(ws, filas):
    ws.clear()
    ws.resize(rows=max(len(filas), 2), cols=len(filas[0]))
    ws.update(values=filas, range_name="A1", value_input_option="RAW")
    ws.freeze(rows=1)

def sincronizar(libro, nombre, df):
    cols = df.columns
    assert cols[0] == "clave"
    cabecera = cols + CARGA
    fuente = {}
    for r in df.toPandas().itertuples(index=False):
        v = [limpio(x) for x in r]
        fuente[v[0]] = (v, hashlib.sha1(json.dumps(v, default=str).encode()).hexdigest()[:16])
    ws = hoja(libro, nombre, len(fuente) + 1, len(cabecera))
    previas = {}
    if MODO == "incremental":
        actual = ws.get_values(value_render_option=ValueRenderOption.unformatted)
        if actual and actual[0] == cabecera:
            previas = {str(r[0]): r + [""] * (len(cabecera) - len(r)) for r in actual[1:] if r and r[0] != ""}
        elif actual and any(actual[0]):
            print(f"  {nombre}: las columnas cambiaron; se carga completo")
    salida, n = {}, {"nuevas": 0, "actualizadas": 0, "sin_cambio": 0, "conservadas": 0}
    for k, (v, h) in fuente.items():
        p = previas.get(k)
        if p and p[len(cols)] == h:
            salida[k] = p; n["sin_cambio"] += 1
        else:
            salida[k] = v + [h, ID_CARGA, CARGADO_EN]; n["actualizadas" if p else "nuevas"] += 1
    for k, p in previas.items():
        if k not in salida:
            salida[k] = p; n["conservadas"] += 1
    if n["nuevas"] or n["actualizadas"] or MODO == "completo" or not previas:
        escribir(ws, [cabecera] + [salida[k] for k in sorted(salida)])
    print(f"  {libro.title} / {nombre}: {n}  total={len(salida)}")
    return {"hoja": nombre, **n, "total": len(salida)}

def registrar(libro, resultados):
    cab = ["id_carga", "cargado_en", "modo", "pestaña", "nuevas", "actualizadas", "sin_cambio", "conservadas", "total"]
    ws = hoja(libro, "cargas", 2, len(cab))
    if ws.row_values(1) != cab:
        escribir(ws, [cab])
    ws.append_rows([[ID_CARGA, CARGADO_EN, MODO, r["hoja"], r["nuevas"], r["actualizadas"], r["sin_cambio"],
                     r["conservadas"], r["total"]] for r in resultados], value_input_option="RAW")

# COMMAND ----------

# Celda 3: sincronizar
leeme = [["campo", "valor"],
    ["Proyecto", "Extracción de datos de facturas: M1 (LayoutLMv3, predicciones publicadas de DocILE) vs M2 v2 (ai_parse_document + ai_extract, Databricks)"],
    ["Datos", "DocILE (Šimsa et al., ICDAR 2023), partición val: 500 documentos"],
    ["Métrica", "Por valor: cantidad exacta, costo unitario |Δ|<0.01, importe |Δ|<0.50; emparejamiento húngaro de líneas"],
    ["Pruebas", "Shapiro-Wilk → Wilcoxon / t pareada; McNemar exacto por campo; IC 95 % bootstrap (10 000)"],
    ["Llaves (columna clave)", "resumen: seccion|indicador · por_documento: docid · detalle: modelo|docid|linea|campo (linea 0 = cabecera)"],
    ["Columnas de carga", "hash_fila = huella del contenido · id_carga = AAAAMMDD_HHMMSS · cargado_en = hora de Lima"],
    ["Última carga", f"{ID_CARGA} ({CARGADO_EN}, modo {MODO}). Filtrar id_carga con ese valor para ver lo cargado"],
    ["Código", "github.com/datavisionary-consulting/databricks/tree/main/invoice-extraction-benchmark"]]
escribir(hoja(pub, "leeme", len(leeme), 2), leeme)

registrar(pub, [sincronizar(pub, "resumen", spark.table("workspace.invoice_extraction_benchmark.ieb_eval_resumen")),
                sincronizar(pub, "por_documento", spark.table("workspace.invoice_extraction_benchmark.ieb_eval_por_doc"))])
registrar(res, [sincronizar(res, "detalle", spark.table("workspace.invoice_extraction_benchmark.ieb_eval_detalle"))])
print(pub.url); print(res.url)