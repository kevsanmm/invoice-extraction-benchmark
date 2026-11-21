"""Selecciona los 20 documentos PIF de entrenamiento para ajustar M2 (dev_pif).

Muestreo aleatorio simple, semilla 42, sobre los documentos de train.json con
metadata.source == "pif" (ordenados por docid para que el resultado sea reproducible).

Uso: python select_dev_pif.py <dataset_path> <salida.json>
"""
import json
import random
import sys
from pathlib import Path

N, SEED = 20, 42


def main(dataset, out):
    D = Path(dataset)
    train = json.loads((D / "train.json").read_text(encoding="utf-8"))
    pif = []
    for d in sorted(train):
        meta = json.loads((D / "annotations" / f"{d}.json").read_text(encoding="utf-8"))["metadata"]
        if meta.get("source") == "pif":
            pif.append({"docid": d, "tipo": meta.get("document_type"), "cluster_id": meta.get("cluster_id"),
                        "paginas": meta.get("page_count")})
    sel = random.Random(SEED).sample(pif, N)
    Path(out).write_text(json.dumps(sel, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"PIF en entrenamiento: {len(pif)} | seleccionados: {len(sel)} (semilla {SEED})")
    for s in sel:
        print(s["docid"], s["tipo"], s["paginas"])


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1], sys.argv[2])
