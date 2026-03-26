import os
import re
import tempfile
from typing import Optional

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
        parts = []
        for node in p.childNodes:
            if hasattr(node, "data") and node.data:
                parts.append(str(node.data))
        txt = "".join(parts).strip()
        if txt:
            texts.append(txt)
    return " ".join(texts).strip()


def normalize_number(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None

    value = str(value).strip()
    if value == "":
        return None

    value = value.replace("\xa0", " ")
    value = value.replace(" ", "")
    value = value.replace(",", ".")

    try:
        return round(float(value), 2)
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
        vals = [str(v).strip() for v in row["values"]]
        joined = " | ".join(vals).lower()

        if "code" in joined and ("désignation" in joined or "designation" in joined) and "quantité" in joined:
            return row["row_index"]

    raise ValueError("Ligne d'en-tête introuvable dans la feuille 'Result'.")


def get_last_numeric_value(values) -> Optional[float]:
    """
    Récupère la dernière valeur numérique non vide de la ligne.
    Utile pour les lignes TOTAL HT où la valeur n'est pas forcément dans la colonne fixe.
    """
    numeric_values = []
    for v in values:
        n = normalize_number(v)
        if n is not None:
            numeric_values.append(n)

    if not numeric_values:
        return None

    return numeric_values[-1]


def parse_rows(rows, header_index):
    parsed_rows = []
    warnings = []
    suspect_rows = []

    current_lot_code = None

    for row in rows:
        if row["row_index"] <= header_index:
            continue

        values = row["values"]
        if len(values) < 8:
            values = values + [""] * (8 - len(values))

        code = values[0].strip() if len(values) > 0 else ""
        designation = values[1].strip() if len(values) > 1 else ""
        unite = values[2].strip() if len(values) > 2 else ""
        quantite = normalize_number(values[3] if len(values) > 3 else "")
        pu = normalize_number(values[4] if len(values) > 4 else "")
        total = normalize_number(values[5] if len(values) > 5 else "")
        non_compris = str(values[7]).strip().upper() == "X" if len(values) > 7 else False

        joined_text = " ".join([str(v) for v in values]).strip().upper()

        if not joined_text:
            continue

        # Ignore TVA
        if "TVA" in joined_text:
            continue

        # Lignes TOTAL
        if "TOTAL HT" in joined_text or joined_text.startswith("TOTAL"):
            total_value = get_last_numeric_value(values)

            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code or None,
                "type_ligne": "total",
                "code_lot": current_lot_code,
                "designation": "TOTAL HT",
                "unite": None,
                "quantite": None,
                "pu": None,
                "total": total_value,
                "non_compris": False,
                "commentaire": None
            })
            continue

        # Ligne suspecte : quantité présente mais aucun code
        if not code and quantite is not None:
            suspect_rows.append({
                "row_index": row["row_index"],
                "raw_values": values,
                "reason": "code_absent_with_quantity"
            })
            continue

        type_ligne = classify_code(code)

        if code and type_ligne == "other":
            suspect_rows.append({
                "row_index": row["row_index"],
                "raw_values": values,
                "reason": "unknown_code_format"
            })
            continue

        if type_ligne == "lot":
            current_lot_code = code

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
                "non_compris": False,
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
                "non_compris": False,
                "commentaire": None
            })
            continue

        if type_ligne == "produit":
            code_lot = code[:2]

            if not current_lot_code:
                warnings.append(f"Ligne produit sans lot courant détectée à la ligne {row['row_index']}")
                suspect_rows.append({
                    "row_index": row["row_index"],
                    "raw_values": values,
                    "reason": "produit_without_current_lot"
                })

            if quantite is None and pu is None and total is None:
                suspect_rows.append({
                    "row_index": row["row_index"],
                    "raw_values": values,
                    "reason": "produit_missing_numeric_values"
                })

            if total is None:
                if quantite is not None and pu is not None:
                    total = round(quantite * pu, 2)
                else:
                    suspect_rows.append({
                        "row_index": row["row_index"],
                        "raw_values": values,
                        "reason": "produit_total_missing_and_cannot_recalculate"
                    })

            parsed_rows.append({
                "row_index": row["row_index"],
                "code": code,
                "type_ligne": "produit",
                "code_lot": code_lot,
                "designation": designation,
                "unite": unite or None,
                "quantite": quantite,
                "pu": pu,
                "total": total,
                "non_compris": non_compris,
                "commentaire": None
            })
            continue

        continue

    return parsed_rows, warnings, suspect_rows


def aggregate_lots(parsed_rows):
    lots = {}
    warnings = []

    # Initialisation des lots
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

    # Agrégation
    for row in parsed_rows:
        code_lot = row.get("code_lot")
        if not code_lot or code_lot not in lots:
            continue

        if row["type_ligne"] == "produit":
            lots[code_lot]["nb_lignes_produit"] += 1

            if not row.get("non_compris", False):
                lots[code_lot]["total_calcule"] += row["total"] or 0.0

        # On garde UNIQUEMENT le premier TOTAL HT rencontré
        if row["type_ligne"] == "total" and lots[code_lot]["total_importe"] is None:
            lots[code_lot]["total_importe"] = row["total"]

    # Finalisation + warnings
    for code_lot, lot in lots.items():
        lot["total_calcule"] = round(lot["total_calcule"], 2)

        if lot["total_importe"] is not None:
            lot["total_importe"] = round(lot["total_importe"], 2)
            lot["delta_total"] = round(lot["total_calcule"] - lot["total_importe"], 2)
        else:
            lot["delta_total"] = None
            warnings.append(f"Lot {code_lot} : total importé introuvable.")

        if lot["nb_lignes_produit"] == 0:
            warnings.append(f"Lot {code_lot} : aucun produit détecté.")

    return list(lots.values()), warnings


def parse_ods_from_url(
    file_url: str,
    chantier_id: Optional[str] = None,
    metre_type: Optional[str] = None,
    version_index: Optional[int] = None
):
    if not file_url:
        return {
            "status": "error",
            "errors": ["file_url manquant"],
            "warnings": [],
            "suspect_rows": [],
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
        parsed_rows, parse_warnings, suspect_rows = parse_rows(rows, header_index)
        lots, aggregate_warnings = aggregate_lots(parsed_rows)

        warnings = parse_warnings + aggregate_warnings
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
            "suspect_rows": suspect_rows,
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
            "suspect_rows": [],
            "rows": [],
            "lots": []
        }

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
