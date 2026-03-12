"""
Excel Schema Extractor — XML-aware vs Pandas comparison
Extracts rich structural metadata from semi-structured financial Excel sheets.

Key signals recovered from XML that pandas loses:
  1. Bold formatting  → section headers / subtotals
  2. Indent level     → hierarchy depth of line items
  3. Merged cells     → multi-column header spans
  4. Font size        → title vs header vs body distinction
  5. Row grouping     → outline levels (collapsed sections)
"""

import zipfile
import json
import re
from collections import defaultdict
import xml.etree.ElementTree as ET

import pandas as pd
import openpyxl

NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
XLSX_PATH = "1773278180536_260225-4q-2025-data-pack-excel.xlsx"


# ──────────────────────────────────────────────
# 1.  PANDAS APPROACH (baseline)
# ──────────────────────────────────────────────

def pandas_schema(xlsx_path: str, sheet_name: str) -> dict:
    """Current approach: read with pandas, infer schema from values only."""
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)

    # Heuristic: first non-null row with >1 non-null values = header
    header_row = None
    for i, row in df.iterrows():
        non_null = row.dropna()
        if len(non_null) > 1:
            header_row = i
            break

    columns = []
    if header_row is not None:
        for col_idx, val in df.iloc[header_row].items():
            if pd.notna(val):
                columns.append({"col_index": col_idx, "header": str(val)})

    row_labels = []
    for i, row in df.iterrows():
        first = row.iloc[0] if len(row) > 0 else None
        if pd.notna(first) and isinstance(first, str):
            row_labels.append(first.strip())

    return {
        "sheet": sheet_name,
        "approach": "pandas",
        "shape": list(df.shape),
        "inferred_headers": columns,
        "row_labels": row_labels,
    }


# ──────────────────────────────────────────────
# 2.  XML-AWARE APPROACH
# ──────────────────────────────────────────────

def _load_shared_strings(z: zipfile.ZipFile) -> list[str]:
    with z.open("xl/sharedStrings.xml") as f:
        root = ET.parse(f).getroot()
    result = []
    for si in root.findall(f"{{{NS}}}si"):
        texts = [t.text or "" for t in si.findall(f".//{{{NS}}}t")]
        result.append("".join(texts))
    return result


def _load_styles(z: zipfile.ZipFile) -> tuple[list, list]:
    with z.open("xl/styles.xml") as f:
        root = ET.parse(f).getroot()

    fonts = []
    for font in root.findall(f".//{{{NS}}}fonts/{{{NS}}}font"):
        bold = font.find(f"{{{NS}}}b") is not None
        sz_el = font.find(f"{{{NS}}}sz")
        size = float(sz_el.get("val", 10)) if sz_el is not None else 10.0
        fonts.append({"bold": bold, "size": size})

    xfs = []
    for xf in root.findall(f".//{{{NS}}}cellXfs/{{{NS}}}xf"):
        align = xf.find(f"{{{NS}}}alignment")
        indent = int(align.get("indent", 0)) if align is not None else 0
        font_id = int(xf.get("fontId", 0))
        xfs.append({"font": fonts[font_id], "indent": indent})

    return fonts, xfs


def _sheet_id_map(z: zipfile.ZipFile) -> dict[str, str]:
    """Return {sheet_name: sheet_filename}."""
    with z.open("xl/workbook.xml") as f:
        root = ET.parse(f).getroot()
    with z.open("xl/_rels/workbook.xml.rels") as f:
        rels_root = ET.parse(f).getroot()

    RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
    rid_to_target = {
        r.get("Id"): r.get("Target")
        for r in rels_root.findall(f"{{{RELS_NS}}}Relationship")
    }

    result = {}
    for sheet in root.findall(f".//{{{NS}}}sheet"):
        name = sheet.get("name")
        rid = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        target = rid_to_target.get(rid, "")
        filename = target if target.startswith("xl/") else f"xl/{target}"
        result[name] = filename
    return result


def _col_letter_to_index(col_str: str) -> int:
    """'A'->0, 'B'->1, 'AA'->26 ..."""
    idx = 0
    for ch in col_str:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def _parse_cell_ref(ref: str) -> tuple[int, int]:
    """'B5' -> (row=4, col=1) zero-based."""
    match = re.match(r"([A-Z]+)(\d+)", ref)
    col_str, row_str = match.group(1), match.group(2)
    return int(row_str) - 1, _col_letter_to_index(col_str)


def xml_schema(xlsx_path: str, sheet_name: str) -> dict:
    """
    XML-aware extraction capturing:
      - bold (section headers, subtotals)
      - font size (title hierarchy)
      - indent level (item depth)
      - merged cells (multi-column header spans)
      - row outline level (collapsible groups)
    Returns a rich schema dict ready to feed an LLM.
    """
    with zipfile.ZipFile(xlsx_path) as z:
        shared_strings = _load_shared_strings(z)
        fonts, xfs = _load_styles(z)
        sheet_map = _sheet_id_map(z)

        sheet_file = sheet_map[sheet_name]
        with z.open(sheet_file) as f:
            sheet_root = ET.parse(f).getroot()

    # ── Merged cells ──
    merged_ranges = []
    merge_el = sheet_root.find(f"{{{NS}}}mergeCells")
    if merge_el is not None:
        for mc in merge_el.findall(f"{{{NS}}}mergeCell"):
            merged_ranges.append(mc.get("ref"))

    # ── Parse every cell ──
    grid: dict[tuple[int, int], dict] = {}

    for row_el in sheet_root.findall(f".//{{{NS}}}row"):
        outline_level = int(row_el.get("outlineLevel", 0))
        for cell_el in row_el.findall(f"{{{NS}}}c"):
            ref = cell_el.get("r")
            row_i, col_i = _parse_cell_ref(ref)

            s_idx = int(cell_el.get("s", 0))
            xf = xfs[s_idx] if s_idx < len(xfs) else {"font": {"bold": False, "size": 10}, "indent": 0}

            v_el = cell_el.find(f"{{{NS}}}v")
            raw_val = None
            val_type = "empty"
            if v_el is not None and v_el.text is not None:
                if cell_el.get("t") == "s":
                    raw_val = shared_strings[int(v_el.text)]
                    val_type = "string"
                else:
                    try:
                        raw_val = float(v_el.text)
                        val_type = "number"
                    except ValueError:
                        raw_val = v_el.text
                        val_type = "string"

            grid[(row_i, col_i)] = {
                "ref": ref,
                "value": raw_val,
                "type": val_type,
                "bold": xf["font"]["bold"],
                "font_size": xf["font"]["size"],
                "indent": xf["indent"],
                "outline_level": outline_level,
            }

    if not grid:
        return {"sheet": sheet_name, "approach": "xml", "error": "empty sheet"}

    max_row = max(r for r, c in grid) + 1
    max_col = max(c for r, c in grid) + 1

    # ── Identify header rows (multi-col text, bold, large font) ──
    header_rows: list[dict] = []
    data_start_row = 0

    for row_i in range(min(15, max_row)):
        row_cells = {c: grid[(row_i, c)] for (r, c) in grid if r == row_i}
        text_cells = [v for v in row_cells.values() if v["type"] == "string" and v["value"]]
        num_cells = [v for v in row_cells.values() if v["type"] == "number"]
        if not text_cells:
            continue
        avg_size = sum(v["font_size"] for v in text_cells) / len(text_cells)
        bold_ratio = sum(1 for v in text_cells if v["bold"]) / len(text_cells)
        if not num_cells and (bold_ratio > 0.5 or avg_size >= 12):
            labels = [v["value"] for v in sorted(text_cells, key=lambda x: x["ref"]) if v["value"]]
            header_rows.append({
                "row_index": row_i,
                "labels": labels,
                "bold": bold_ratio > 0.5,
                "avg_font_size": round(avg_size, 1),
            })
            data_start_row = row_i + 1

    # ── Column headers (last header row is usually the column label row) ──
    col_headers: dict[int, str] = {}
    for hrow in header_rows:
        for r_i, c_i in grid:
            if r_i == hrow["row_index"]:
                cell = grid[(r_i, c_i)]
                if cell["type"] == "string" and cell["value"]:
                    col_headers[c_i] = str(cell["value"])

    # ── Section structure (bold rows = section headers / subtotals) ──
    sections: list[dict] = []
    current_section: dict | None = None

    for row_i in range(data_start_row, max_row):
        row_cells = {c: grid.get((row_i, c)) for c in range(max_col) if (row_i, c) in grid}
        first_cell = grid.get((row_i, 0))
        if not first_cell or first_cell["type"] != "string" or not first_cell["value"]:
            continue

        label = str(first_cell["value"]).strip()
        is_bold = first_cell["bold"]
        indent = first_cell["indent"]
        outline = first_cell["outline_level"]

        num_vals = {
            col_headers.get(c_i, f"col_{c_i}"): cell["value"]
            for c_i, cell in row_cells.items()
            if cell and cell["type"] == "number" and cell["value"] is not None
        }

        row_schema = {
            "label": label,
            "bold": is_bold,
            "indent": indent,
            "outline_level": outline,
            "role": "section_header" if (is_bold and not num_vals) else
                    "subtotal" if (is_bold and num_vals) else
                    "line_item",
            "sample_values": dict(list(num_vals.items())[:3]),  # first 3 cols as sample
        }

        if is_bold and not num_vals:
            current_section = {"header": label, "items": []}
            sections.append(current_section)
        elif current_section is not None:
            current_section["items"].append(row_schema)
        else:
            sections.append({"header": None, "items": [row_schema]})

    return {
        "sheet": sheet_name,
        "approach": "xml",
        "dimensions": {"rows": max_row, "cols": max_col},
        "merged_ranges": merged_ranges,
        "header_rows": header_rows,
        "column_headers": col_headers,
        "sections": sections,
    }


# ──────────────────────────────────────────────
# 3.  COMPARE & PRINT
# ──────────────────────────────────────────────

def compare(xlsx_path: str, sheet_name: str):
    print("=" * 70)
    print(f"SHEET: {sheet_name}")
    print("=" * 70)

    # Pandas
    ps = pandas_schema(xlsx_path, sheet_name)
    print("\n── PANDAS SCHEMA ──")
    print(f"  Shape      : {ps['shape']}")
    print(f"  Headers    : {ps['inferred_headers'][:4]}...")
    print(f"  Row labels : {ps['row_labels'][:8]}...")

    # XML
    xs = xml_schema(xlsx_path, sheet_name)
    print("\n── XML-AWARE SCHEMA ──")
    print(f"  Dimensions    : {xs.get('dimensions')}")
    print(f"  Merged ranges : {xs.get('merged_ranges')}")
    print(f"  Column headers: {xs.get('column_headers')}")
    print(f"\n  Document hierarchy:")
    for sec in xs.get("sections", []):
        hdr = sec.get("header")
        items = sec.get("items", [])
        if hdr:
            print(f"\n  [SECTION] {hdr}")
        for item in items:
            prefix = "    " + ("  " * item["indent"])
            role_tag = f"[{item['role']}]" if item["role"] != "line_item" else ""
            vals = ", ".join(f"{k}={v}" for k, v in item["sample_values"].items())
            print(f"  {prefix}{role_tag} {item['label']}")
            if vals:
                print(f"  {prefix}    → {vals}")

    return ps, xs


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else XLSX_PATH
    sheet = sys.argv[2] if len(sys.argv) > 2 else "Group income statement"
    pandas_out, xml_out = compare(path, sheet)

    # Also dump full XML schema as JSON for LLM consumption
    out_file = f"schema_{sheet.replace(' ', '_')}.json"
    with open(out_file, "w") as f:
        json.dump(xml_out, f, indent=2, default=str)
    print(f"\n  Full XML schema saved to: {out_file}")
