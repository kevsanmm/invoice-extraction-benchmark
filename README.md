# Invoice extraction benchmark: radiodifusoras de EE. UU.

Repositorio de trazabilidad de la tesis *Evaluación comparativa de modelos de lenguaje multimodales y modelos especializados en la extracción de información de facturas de radiodifusoras estadounidenses, 2026* (Maestría en Inteligencia Artificial, Escuela de Posgrado Newman).

## Qué compara

| Método | Descripción |
|---|---|
| M1 | LayoutLMv3 (`layoutlmv3_ours`), predicciones publicadas por los autores de DocILE para la partición de validación |
| M2 | Modelo de lenguaje multimodal sin ejemplos: `ai_parse_document` + `ai_extract` en Databricks Free Edition |

- Muestra: 275 documentos de radiodifusoras (fuente PIF) de la partición de validación de DocILE.
- Indicadores: exactitud por valor en cabecera, líneas de detalle y diseños no vistos.
- Pruebas: Shapiro-Wilk, t pareada o Wilcoxon, McNemar, bootstrap y corrección de Holm (α = 0,05).

## Estructura

| Carpeta | Contenido |
|---|---|
| `Notebook/` | Cuadernos de Databricks en orden (01 a 06). `06_pif_ficha_y_evaluacion` es la evaluación de la tesis (275 PIF) |
| `tools/` | Normalización, evaluación por valor y emparejamiento de líneas (`eval_valores.py`), pruebas (`comparar.py`) |
| `BITACORA.md` | Registro de cada ejecución: datos, comando, resultado y decisión |

Las tablas de resultados viven en `workspace.invoice_extraction_benchmark` (Databricks Free Edition): `ieb_pif_*` para la tesis e `ieb_eval_*` para la evaluación exploratoria de 500 documentos.

## Datos

El corpus DocILE (Šimsa et al., 2023) se descarga con token desde https://docile.rossum.ai/. Su licencia solo permite el uso en investigación; por eso este repositorio no incluye documentos, anotaciones ni texto extraído.

## Estado

En construcción. Se publica el código a medida que se ejecuta cada etapa.
