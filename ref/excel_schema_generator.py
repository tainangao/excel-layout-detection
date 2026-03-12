"""
excel_schema_generator.py
─────────────────────────
Generates rich, LLM-ready schemas for every sheet in an Excel workbook.

Handles both:
  • Structured sheets   – clear tabular data (uses pandas fast-path)
  • Semi-structured sheets – financial reports, pivot-style layouts
                            (uses XML-aware extraction for bold / indent /
                             merged-cell hierarchy)

Usage
─────
    python excel_schema_generator.py workbook.xlsx [--sheets Sheet1 Sheet2]
                                                   [--output schema.json]
                                                   [--format json|text]
                                                   [--mode auto|xml|pandas]

Programmatic
────────────
    from excel_schema_generator import generate_workbook_schema

    schema = generate_workbook_schema("workbook.xlsx")
    # schema["sheets"] is a list of per-sheet schema dicts
"""

from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

import pandas as pd
import openpyxl

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────

NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

# Heuristics for auto-detecting sheet type
STRUCTURED_MAX_HEADER_ROW = 5      # header must appear in first N rows
STRUCTURED_MIN_FILL_RATIO = 0.55   # ≥55 % of cells in body must be non-null
SEMI_STRUCTURED_BOLD_RATIO = 0.08  # ≥8 % bold cells → likely semi-structured


# ─────────────────────────────────────────────────────────────
# Low-level XML helpers
# ─────────────────────────────────────────────────────────────

def _load_shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    with z.open("xl/sharedStrings.xml") as f:
        root = ET.parse(f).getroot()
    out: list[str] = []
    for si in root.findall(f"{{{NS}}}si"):
        texts = [t.text or "" for t in si.findall(f".//{{{NS}}}t")]
        out.append("".join(texts))
    return out


def _load_styles(z: zipfile.ZipFile) -> tuple[list[dict], list[dict]]:
    """Return (fonts, xfs) where xfs[style_id] = {bold, size, indent}."""
    with z.open("xl/styles.xml") as f:
        root = ET.parse(f).getroot()

    fonts: list[dict] = []
    for font in root.findall(f".//{{{NS}}}fonts/{{{NS}}}font"):
        bold = font.find(f"{{{NS}}}b") is not None
        sz_el = font.find(f"{{{NS}}}sz")
        size = float(sz_el.get("val", 10)) if sz_el is not None else 10.0
        color_el = font.find(f"{{{NS}}}color")
        color = color_el.get("rgb", "") if color_el is not None else ""
        fonts.append({"bold": bold, "size": size, "color": color})

    xfs: list[dict] = []
    for xf in root.findall(f".//{{{NS}}}cellXfs/{{{NS}}}xf"):
        align = xf.find(f"{{{NS}}}alignment")
        indent = int(align.get("indent", 0)) if align is not None else 0
        h_align = align.get("horizontal", "") if align is not None else ""
        font_id = int(xf.get("fontId", 0))
        num_fmt_id = int(xf.get("numFmtId", 0))
        xfs.append({
            "font": fonts[font_id] if font_id < len(fonts) else {"bold": False, "size": 10, "color": ""},
            "indent": indent,
            "h_align": h_align,
            "num_fmt_id": num_fmt_id,
        })
    return fonts, xfs


def _sheet_file_map(z: zipfile.ZipFile) -> dict[str, str]:
    """Return {sheet_name: path_inside_zip}."""
    with z.open("xl/workbook.xml") as f:
        wb_root = ET.parse(f).getroot()
    with z.open("xl/_rels/workbook.xml.rels") as f:
        rels_root = ET.parse(f).getroot()

    rid_to_target = {
        r.get("Id"): r.get("Target")
        for r in rels_root.findall(f"{{{RELS_NS}}}Relationship")
    }
    result: dict[str, str] = {}
    for sheet in wb_root.findall(f".//{{{NS}}}sheet"):
        name = sheet.get("name", "")
        rid = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
        target = rid_to_target.get(rid, "")
        path = target if target.startswith("xl/") else f"xl/{target}"
        result[name] = path
    return result


def _col_to_index(col_str: str) -> int:
    idx = 0
    for ch in col_str:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def _parse_ref(ref: str) -> tuple[int, int]:
    m = re.match(r"([A-Z]+)(\d+)", ref)
    return int(m.group(2)) - 1, _col_to_index(m.group(1))


def _num_fmt_category(num_fmt_id: int) -> str:
    """Rough mapping of built-in numFmtId → human category."""
    if num_fmt_id == 0:
        return "general"
    if num_fmt_id in range(1, 9):
        return "integer" if num_fmt_id < 2 else "decimal"
    if num_fmt_id in range(9, 12):
        return "percent"
    if num_fmt_id in (14, 15, 16, 17, 18, 19, 20, 21, 22):
        return "date"
    if num_fmt_id in (45, 46, 47):
        return "time"
    if num_fmt_id == 49:
        return "text"
    return "number"


# ─────────────────────────────────────────────────────────────
# Cell grid builder (shared by both extraction paths)
# ─────────────────────────────────────────────────────────────

def _build_grid(
    sheet_root: ET.Element,
    shared_strings: list[str],
    xfs: list[dict],
) -> dict[tuple[int, int], dict]:
    grid: dict[tuple[int, int], dict] = {}
    for row_el in sheet_root.findall(f".//{{{NS}}}row"):
        outline_level = int(row_el.get("outlineLevel", 0))
        collapsed = row_el.get("collapsed", "0") == "1"
        for cell_el in row_el.findall(f"{{{NS}}}c"):
            ref = cell_el.get("r", "")
            if not ref:
                continue
            row_i, col_i = _parse_ref(ref)
            s_idx = int(cell_el.get("s", 0))
            xf = xfs[s_idx] if s_idx < len(xfs) else {
                "font": {"bold": False, "size": 10, "color": ""},
                "indent": 0, "h_align": "", "num_fmt_id": 0,
            }
            v_el = cell_el.find(f"{{{NS}}}v")
            raw_val: Any = None
            val_type = "empty"
            if v_el is not None and v_el.text is not None:
                if cell_el.get("t") == "s":
                    try:
                        raw_val = shared_strings[int(v_el.text)]
                        val_type = "string"
                    except (ValueError, IndexError):
                        raw_val = v_el.text
                        val_type = "string"
                elif cell_el.get("t") == "b":
                    raw_val = v_el.text == "1"
                    val_type = "boolean"
                else:
                    try:
                        raw_val = float(v_el.text)
                        val_type = _num_fmt_category(xf["num_fmt_id"])
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
                "collapsed": collapsed,
            }
    return grid


# ─────────────────────────────────────────────────────────────
# Sheet-type detector
# ─────────────────────────────────────────────────────────────

def _detect_sheet_type(grid: dict, max_row: int, max_col: int) -> str:
    """
    Returns 'structured' or 'semi_structured'.

    Structured  → dense, regular table with a clear header row.
    Semi-struct → financial reports, multi-section layouts, indented rows.
    """
    if not grid:
        return "structured"

    # Count bold cells ratio
    total = len(grid)
    bold_count = sum(1 for c in grid.values() if c["bold"])
    bold_ratio = bold_count / total if total else 0

    # Count indent usage
    indent_count = sum(1 for c in grid.values() if c["indent"] > 0)
    has_indent = indent_count > 0

    # Check header row density: find first row with ≥2 string cells
    header_row = None
    for row_i in range(min(STRUCTURED_MAX_HEADER_ROW, max_row)):
        row_cells = [grid.get((row_i, c)) for c in range(max_col)]
        str_cells = [c for c in row_cells if c and c["type"] == "string"]
        if len(str_cells) >= 2:
            header_row = row_i
            break

    if header_row is None:
        return "semi_structured"

    # Body fill ratio below header
    body_cells = [(r, c) for (r, c) in grid if r > header_row]
    non_empty = sum(1 for (r, c) in body_cells if grid[(r, c)]["type"] != "empty")
    fill_ratio = non_empty / len(body_cells) if body_cells else 0

    if bold_ratio >= SEMI_STRUCTURED_BOLD_RATIO or has_indent:
        return "semi_structured"
    if fill_ratio >= STRUCTURED_MIN_FILL_RATIO:
        return "structured"
    return "semi_structured"


# ─────────────────────────────────────────────────────────────
# Structured sheet schema  (pandas fast-path)
# ─────────────────────────────────────────────────────────────

def _structured_schema(
    xlsx_path: str,
    sheet_name: str,
    grid: dict,
    max_row: int,
    max_col: int,
    merged_ranges: list[str],
) -> dict:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)

    # Find header row
    header_row_idx = None
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        if row.notna().sum() >= 2:
            header_row_idx = i
            break

    columns: list[dict] = []
    sample_rows: list[dict] = []

    if header_row_idx is not None:
        raw_headers = df.iloc[header_row_idx].tolist()
        data_df = df.iloc[header_row_idx + 1:].reset_index(drop=True)
        data_df.columns = [
            str(h) if pd.notna(h) else f"col_{i}"
            for i, h in enumerate(raw_headers)
        ]
        for col in data_df.columns:
            series = data_df[col].dropna()
            inferred = "unknown"
            if len(series):
                sample = series.iloc[0]
                if isinstance(sample, (int, float)):
                    inferred = "number"
                elif hasattr(sample, "year"):
                    inferred = "date"
                else:
                    inferred = "string"
            columns.append({
                "name": col,
                "inferred_type": inferred,
                "non_null_count": int(series.count()),
                "sample_values": [str(v) for v in series.head(3).tolist()],
            })
        for _, row in data_df.head(3).iterrows():
            sample_rows.append({k: str(v) for k, v in row.items() if pd.notna(v)})
    else:
        data_df = df

    return {
        "sheet_type": "structured",
        "dimensions": {"rows": max_row, "cols": max_col},
        "merged_ranges": merged_ranges,
        "header_row_index": header_row_idx,
        "columns": columns,
        "sample_rows": sample_rows,
        "notes": [],
    }


# ─────────────────────────────────────────────────────────────
# Semi-structured sheet schema  (XML-aware)
# ─────────────────────────────────────────────────────────────

def _resolve_column_headers(
    grid: dict, max_row: int, max_col: int
) -> tuple[dict[int, str], int]:
    """
    Scan top rows to build col_index → header_label map.
    Returns (col_headers, data_start_row).
    Multi-row headers are concatenated with ' / '.
    """
    header_rows: list[int] = []
    for row_i in range(min(12, max_row)):
        row_cells = [grid.get((row_i, c)) for c in range(max_col)]
        text = [c for c in row_cells if c and c["type"] == "string" and c["value"]]
        nums = [c for c in row_cells if c and c["type"] not in ("string", "empty")]
        # A header row has text, no numbers, and some bold or large font
        if text and not nums:
            header_rows.append(row_i)

    col_headers: dict[int, list[str]] = {}
    for row_i in header_rows:
        for col_i in range(max_col):
            cell = grid.get((row_i, col_i))
            if cell and cell["type"] == "string" and cell["value"]:
                col_headers.setdefault(col_i, []).append(str(cell["value"]).strip())

    data_start = (max(header_rows) + 1) if header_rows else 0
    return {k: " / ".join(v) for k, v in col_headers.items()}, data_start


def _build_section_tree(
    grid: dict,
    col_headers: dict[int, str],
    data_start: int,
    max_row: int,
    max_col: int,
) -> list[dict]:
    """
    Walk rows top-to-bottom and build a nested section tree using:
      - bold + no numbers  → section_header
      - bold + numbers     → subtotal
      - indent > 0         → child line item
      - else               → line item
    """
    sections: list[dict] = []
    section_stack: list[dict] = []   # stack of open section dicts

    def _current_section() -> dict | None:
        return section_stack[-1] if section_stack else None

    def _sample_values(row_i: int, n: int = 4) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for col_i in range(max_col):
            cell = grid.get((row_i, col_i))
            if cell and cell["type"] not in ("string", "empty") and cell["value"] is not None:
                label = col_headers.get(col_i, f"col_{col_i}")
                out[label] = cell["value"]
                if len(out) >= n:
                    break
        return out

    for row_i in range(data_start, max_row):
        first = grid.get((row_i, 0))
        if not first or first["type"] != "string" or not first["value"]:
            continue

        label = str(first["value"]).strip()
        if not label:
            continue

        is_bold = first["bold"]
        indent = first["indent"]
        font_size = first["font_size"]
        outline = first["outline_level"]
        sample = _sample_values(row_i)

        if is_bold and not sample:
            # Pure section header — open a new section
            node: dict = {
                "label": label,
                "role": "section_header",
                "bold": True,
                "font_size": font_size,
                "indent": indent,
                "outline_level": outline,
                "children": [],
            }
            # Pop stack until we find a parent with lower indent / font_size
            while section_stack and section_stack[-1]["indent"] >= indent:
                section_stack.pop()
            if section_stack:
                section_stack[-1]["children"].append(node)
            else:
                sections.append(node)
            section_stack.append(node)

        else:
            role = "subtotal" if (is_bold and sample) else "line_item"
            item: dict = {
                "label": label,
                "role": role,
                "bold": is_bold,
                "indent": indent,
                "outline_level": outline,
                "sample_values": sample,
            }
            cur = _current_section()
            if cur is not None:
                cur["children"].append(item)
            else:
                # Top-level orphan item
                if not sections or "children" not in sections[-1]:
                    placeholder: dict = {
                        "label": "",
                        "role": "section_header",
                        "bold": False,
                        "indent": 0,
                        "outline_level": 0,
                        "children": [],
                    }
                    sections.append(placeholder)
                    section_stack = [placeholder]
                section_stack[-1]["children"].append(item)

    return sections


def _semi_structured_schema(
    grid: dict,
    max_row: int,
    max_col: int,
    merged_ranges: list[str],
) -> dict:
    col_headers, data_start = _resolve_column_headers(grid, max_row, max_col)
    sections = _build_section_tree(grid, col_headers, data_start, max_row, max_col)

    # Collect footnotes (long strings in bottom rows, no numbers)
    footnotes: list[str] = []
    for row_i in range(max(0, max_row - 5), max_row):
        for col_i in range(max_col):
            cell = grid.get((row_i, col_i))
            if cell and cell["type"] == "string" and cell["value"]:
                val = str(cell["value"]).strip()
                if len(val) > 60:
                    footnotes.append(val)

    return {
        "sheet_type": "semi_structured",
        "dimensions": {"rows": max_row, "cols": max_col},
        "merged_ranges": merged_ranges,
        "column_headers": col_headers,
        "data_start_row": data_start,
        "sections": sections,
        "footnotes": footnotes,
    }


# ─────────────────────────────────────────────────────────────
# Per-sheet dispatcher
# ─────────────────────────────────────────────────────────────

def extract_sheet_schema(
    xlsx_path: str,
    sheet_name: str,
    shared_strings: list[str],
    xfs: list[dict],
    sheet_file_map: dict[str, str],
    mode: str = "auto",          # "auto" | "xml" | "pandas"
) -> dict:
    """Extract schema for a single sheet."""
    sheet_file = sheet_file_map.get(sheet_name)
    if not sheet_file:
        return {"sheet": sheet_name, "error": "sheet file not found"}

    with zipfile.ZipFile(xlsx_path) as z:
        with z.open(sheet_file) as f:
            sheet_root = ET.parse(f).getroot()

    # Merged cells
    merged_ranges: list[str] = []
    merge_el = sheet_root.find(f"{{{NS}}}mergeCells")
    if merge_el is not None:
        merged_ranges = [mc.get("ref", "") for mc in merge_el]

    grid = _build_grid(sheet_root, shared_strings, xfs)

    if not grid:
        return {
            "sheet": sheet_name,
            "sheet_type": "empty",
            "dimensions": {"rows": 0, "cols": 0},
        }

    max_row = max(r for r, c in grid) + 1
    max_col = max(c for r, c in grid) + 1

    # Decide extraction mode
    if mode == "auto":
        sheet_type = _detect_sheet_type(grid, max_row, max_col)
    elif mode == "xml":
        sheet_type = "semi_structured"
    else:
        sheet_type = "structured"

    if sheet_type == "structured":
        schema = _structured_schema(xlsx_path, sheet_name, grid, max_row, max_col, merged_ranges)
    else:
        schema = _semi_structured_schema(grid, max_row, max_col, merged_ranges)

    schema["sheet"] = sheet_name
    return schema


# ─────────────────────────────────────────────────────────────
# Workbook-level entry point
# ─────────────────────────────────────────────────────────────

def generate_workbook_schema(
    xlsx_path: str,
    sheet_names: list[str] | None = None,
    mode: str = "auto",
) -> dict:
    """
    Generate schemas for all (or selected) sheets in a workbook.

    Parameters
    ----------
    xlsx_path   : path to the .xlsx file
    sheet_names : list of sheet names to process; None = all sheets
    mode        : "auto" | "xml" | "pandas"
                  auto   – detect per sheet (recommended)
                  xml    – force XML-aware for all sheets
                  pandas – force pandas for all sheets

    Returns
    -------
    {
      "workbook": "<filename>",
      "total_sheets": N,
      "processed_sheets": M,
      "sheets": [ <sheet_schema>, ... ]
    }
    """
    xlsx_path = str(xlsx_path)

    with zipfile.ZipFile(xlsx_path) as z:
        shared_strings = _load_shared_strings(z)
        _, xfs = _load_styles(z)
        sheet_file_map = _sheet_file_map(z)

    all_sheet_names = list(sheet_file_map.keys())
    target_sheets = sheet_names if sheet_names else all_sheet_names

    schemas: list[dict] = []
    for name in target_sheets:
        if name not in sheet_file_map:
            schemas.append({"sheet": name, "error": "sheet not found"})
            continue
        try:
            s = extract_sheet_schema(
                xlsx_path, name, shared_strings, xfs, sheet_file_map, mode
            )
        except Exception as exc:
            s = {"sheet": name, "error": str(exc)}
        schemas.append(s)

    return {
        "workbook": Path(xlsx_path).name,
        "total_sheets": len(all_sheet_names),
        "processed_sheets": len(schemas),
        "all_sheet_names": all_sheet_names,
        "sheets": schemas,
    }


# ─────────────────────────────────────────────────────────────
# Text renderer (compact LLM prompt-ready view)
# ─────────────────────────────────────────────────────────────

def _render_section_tree(sections: list[dict], depth: int = 0) -> list[str]:
    lines: list[str] = []
    pad = "  " * depth
    for node in sections:
        role = node.get("role", "")
        label = node.get("label", "")
        bold_tag = "[bold]" if node.get("bold") else ""
        role_tag = f"[{role}]" if role != "line_item" else ""
        indent_tag = f"(indent={node['indent']})" if node.get("indent", 0) > 0 else ""

        tags = " ".join(t for t in [bold_tag, role_tag, indent_tag] if t)
        sample = node.get("sample_values", {})
        sample_str = ("  →  " + ", ".join(f"{k}={v}" for k, v in list(sample.items())[:3])) if sample else ""

        if label:
            lines.append(f"{pad}{tags} {label}{sample_str}")

        children = node.get("children", [])
        if children:
            lines.extend(_render_section_tree(children, depth + 1))
    return lines


def render_text(workbook_schema: dict) -> str:
    lines: list[str] = []
    lines.append(f"WORKBOOK: {workbook_schema['workbook']}")
    lines.append(f"Sheets ({workbook_schema['total_sheets']} total): "
                 f"{', '.join(workbook_schema['all_sheet_names'])}")
    lines.append("")

    for s in workbook_schema["sheets"]:
        sheet = s.get("sheet", "?")
        stype = s.get("sheet_type", "error")
        lines.append("─" * 60)
        lines.append(f"SHEET: {sheet}  [{stype.upper()}]")

        if "error" in s:
            lines.append(f"  ERROR: {s['error']}")
            continue

        dim = s.get("dimensions", {})
        lines.append(f"  Dimensions : {dim.get('rows')} rows × {dim.get('cols')} cols")

        merged = s.get("merged_ranges", [])
        if merged:
            lines.append(f"  Merged cells: {', '.join(merged)}")

        if stype == "structured":
            cols = s.get("columns", [])
            lines.append(f"  Header row : {s.get('header_row_index')}")
            lines.append(f"  Columns ({len(cols)}):")
            for col in cols:
                lines.append(
                    f"    • {col['name']}  [{col['inferred_type']}]"
                    f"  non-null={col['non_null_count']}"
                    f"  samples={col['sample_values']}"
                )
            samples = s.get("sample_rows", [])
            if samples:
                lines.append(f"  Sample rows:")
                for row in samples:
                    lines.append(f"    {row}")

        elif stype == "semi_structured":
            col_hdrs = s.get("column_headers", {})
            if col_hdrs:
                lines.append(f"  Column headers:")
                for ci, label in sorted(col_hdrs.items()):
                    lines.append(f"    col_{ci}: {label}")
            sections = s.get("sections", [])
            if sections:
                lines.append(f"  Section hierarchy:")
                lines.extend(_render_section_tree(sections, depth=2))
            notes = s.get("footnotes", [])
            if notes:
                lines.append(f"  Footnotes:")
                for n in notes:
                    lines.append(f"    ¹ {n[:120]}")

        lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate rich schemas for Excel workbooks (structured + semi-structured)."
    )
    parser.add_argument("xlsx", help="Path to the .xlsx file")
    parser.add_argument(
        "--sheets", nargs="*", metavar="SHEET",
        help="Sheet names to process (default: all)"
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output file path (.json or .txt). Prints to stdout if omitted."
    )
    parser.add_argument(
        "--format", choices=["json", "text"], default="text",
        help="Output format (default: text)"
    )
    parser.add_argument(
        "--mode", choices=["auto", "xml", "pandas"], default="auto",
        help="Extraction mode (default: auto)"
    )
    args = parser.parse_args()

    schema = generate_workbook_schema(args.xlsx, args.sheets, args.mode)

    if args.format == "json":
        output = json.dumps(schema, indent=2, default=str)
    else:
        output = render_text(schema)

    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        print(f"Schema written to: {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
