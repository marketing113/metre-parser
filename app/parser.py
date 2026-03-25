import os
import re
import tempfile
from typing import Any

import httpx
from odf.opendocument import load
from odf.table import Table, TableRow, TableCell
from odf.text import P


LOT_RE = re.compile(r"^\d{2}$")
SOUS_LOT_RE = re.compile(r"^\d{2}\.\d{2}$")
PRODUIT_RE = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")


def get_cell_text(cell: TableCell) -> str:
    texts = []
    for p in cell.getElementsByType(P):
        if p.firstChild:
            texts.append(str(p.firstChild.data))
    return " ".join(texts).strip()


def normalize_number(value: str):
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None

    # Nettoyage format français : "1 234,56" -> 1234.56
    value = value.replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def classify_code(code: str) -> str:
    if not code:
        return "other"
    code = code.strip()
    if LOT_RE.match(code):
        return "lot"
    if SOUS_LOT_RE.match(code):
        return "sous_lot"
    if PRODUIT_RE.match(code):
        return "produit"
    return "other"


def extract_rows_from_result_sheet(file_path: str):
    doc = load(file_path)
    sheets = doc.spreadsheet.getElementsByType(Table)

    result_sheet = None
    for sheet in sheets:
        name = sheet.getAttribute("name")
        if name == "Result":
            result_sheet = sheet
            break

    if not result_sheet:
        raise ValueError("Feuille 'Result' introuvable.")

    all_rows = []
    for row_index, row in enumerate(result_sheet.getElementsByType(TableRow), start=1):
        row_values = []
        for cell in row.getElementsByType(TableCell):
            repeat = cell.getAttribute("numbercolumnsrepeated")
            repeat = int(repeat) if repeat else 1
            cell_text = get_cell_text(cell)
            for _ in range(repeat):
                row_values.append(cell_text)
        all_rows.append({
            "row_index": row_index,
            "values": row_values
        })

    return all_rows


def find_header_row(rows):
    for row in rows:
        vals = [v.strip() for v in row["values"]]
        joined = " | ".join(vals).lower()
        if "code" in joined and "désignation" in joined and "quantité" in joined:
            return row["row_index"]
    raise ValueError("Ligne d'en-tête introuvable dans la feuille 'Result'.")


def parse_rows(rows, header_index):
    parsed_rows = []
    warnings = []

    current_lot_code = None
    current_lot_name = None

    for row in rows:
        if row["row_index"] <= header_index:
            continue

        values = row["values"]
        if len(values) < 6:
            values = values + [""] * (6 - len(values))

        code = values[0].strip()
        designation = values[1].strip()
        unite = values[2].strip() or None
        quantite = normalize_number(values[3])
        pu = normalize_number(values[4])
        total = normalize_number(values[5])

        joined_text = " ".join(values).strip().upper()

        if not joined_text:
            continue

        if "TOTAL HT" in joined_text or joined_text.startswith("TOTAL"):
            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code or None,
                "type_ligne": "total",
                "code_lot": current_lot_code,
                "designation": designation or "TOTAL HT",
                "unite": unite,
                "quantite": quantite,
                "pu": pu,
                "total": total,
                "commentaire": None
            })
            continue

        if "TVA" in joined_text:
            continue

        type_ligne = classify_code(code)

        if type_ligne == "lot":
            current_lot_code = code
            current_lot_name = designation
            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code,
                "type_ligne": "lot",
                "code_lot": code,
                "designation": designation,
                "unite": None,
                "quantite": None,
                "pu": None,
                "total": None,
                "commentaire": None
            })
            continue

        if type_ligne == "sous_lot":
            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code,
                "type_ligne": "sous_lot",
                "code_lot": current_lot_code,
                "designation": designation,
                "unite": None,
                "quantite": None,
                "pu": None,
                "total": None,
                "commentaire": None
            })
            continue

        if type_ligne == "produit":
            code_lot = code[:2]
            if not current_lot_code:
                warnings.append(f"Ligne produit sans lot courant détectée à la ligne {row['row_index']}")
            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code,
                "type_ligne": "produit",
                "code_lot": code_lot,
                "designation": designation,
                "unite": unite,
                "quantite": quantite,
                "pu": pu,
                "total": total,
                "commentaire": None
            })
            continue

        # Lignes parasites ignorées
        continue

    return parsed_rows, warnings


def aggregate_lots(parsed_rows):
    lots = {}

    for row in parsed_rows:
        if row["type_ligne"] == "lot":
            code_lot = row["code_lot"]
            lots[code_lot] = {
                "code_lot": code_lot,
                "nom_lot": row["designation"],
                "nb_lignes_produit": 0,
                "total_calcule": 0.0,
                "total_importe": None,
                "delta_total": None
            }

    for row in parsed_rows:
        code_lot = row.get("code_lot")
        if not code_lot or code_lot not in lots:
            continue

        if row["type_ligne"] == "produit":
            lots[code_lot]["nb_lignes_produit"] += 1
            lots[code_lot]["total_calcule"] += row["total"] or 0.0

        if row["type_ligne"] == "total":
            lots[code_lot]["total_importe"] = row["total"]

    for code_lot, lot in lots.items():
        if lot["total_importe"] is not None:
            lot["delta_total"] = round(lot["total_calcule"] - lot["total_importe"], 2)
        else:
            lot["delta_total"] = None

        lot["total_calcule"] = round(lot["total_calcule"], 2)

    return list(lots.values())


def parse_ods_from_url(file_url: str, chantier_id: str = None, metre_type: str = None, version_index: int = None):
    if not file_url:
        return {
            "status": "error",
            "errors": ["file_url manquant"],
            "warnings": [],
            "rows": [],
            "lots": []
        }

    tmp_path = None

    try:
        with tempfile.NamedTemporaryFile(suffix=".ods", delete=False) as tmp:
            tmp_path = tmp.name

        with httpx.Client(timeout=60.0, follow_redirects=True) as client:
            response = client.get(file_url)
            response.raise_for_status()
            with open(tmp_path, "wb") as f:
                f.write(response.content)

        rows = extract_rows_from_result_sheet(tmp_path)
        header_index = find_header_row(rows)
        parsed_rows, warnings = parse_rows(rows, header_index)
        lots = aggregate_lots(parsed_rows)

        errors = []
        if not lots:
            errors.append("Aucun lot exploitable détecté.")

        return {
            "status": "success" if not errors else "error",
            "file_name": os.path.basename(file_url),
            "sheet_name": "Result",
            "chantier_id": chantier_id,
            "type": metre_type,
            "version_index": version_index,
            "errors": errors,
            "warnings": warnings,
            "rows": parsed_rows,
            "lots": lots
        }

    except Exception as e:
        return {
            "status": "error",
            "file_name": os.path.basename(file_url) if file_url else None,
            "sheet_name": "Result",
            "chantier_id": chantier_id,
            "type": metre_type,
            "version_index": version_index,
            "errors": [str(e)],
            "warnings": [],
            "rows": [],
            "lots": []
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
