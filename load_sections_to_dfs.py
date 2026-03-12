from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


def normalize_label(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value)
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text.casefold()


def unique_column_names(names: list[str]) -> list[str]:
    counts: dict[str, int] = defaultdict(int)
    output: list[str] = []
    for name in names:
        counts[name] += 1
        if counts[name] == 1:
            output.append(name)
        else:
            output.append(f"{name}__{counts[name]}")
    return output


def find_next_row(first_col: pd.Series, label: str, start_row: int) -> int | None:
    target = normalize_label(label)
    if not target:
        return None

    for row_idx in range(start_row, len(first_col)):
        if normalize_label(first_col.iloc[row_idx]) == target:
            return row_idx
    return None


def parse_column_headers(raw_headers: dict[Any, Any]) -> dict[int, str]:
    parsed: dict[int, str] = {}
    for key, value in raw_headers.items():
        try:
            col_idx = int(key)
        except (TypeError, ValueError):
            continue
        parsed[col_idx] = str(value)
    return parsed


def build_column_plan(
    sheet_df: pd.DataFrame,
    col_headers: dict[int, str],
) -> tuple[list[int], dict[int, str]]:
    if col_headers:
        selected_cols = sorted(c for c in col_headers if c in sheet_df.columns)
    else:
        selected_cols = list(sheet_df.columns)

    if 0 in sheet_df.columns and 0 not in selected_cols:
        selected_cols = [0, *selected_cols]

    names: list[str] = []
    for col_idx in selected_cols:
        if col_idx == 0:
            names.append("label")
        else:
            names.append(col_headers.get(col_idx, f"col_{col_idx}"))

    unique_names = unique_column_names(names)
    rename_map = {col_idx: unique_names[i] for i, col_idx in enumerate(selected_cols)}
    return selected_cols, rename_map


def section_key(label: str, index: int, counts: dict[str, int]) -> str:
    base = label.strip() if label.strip() else f"Section {index:02d}"
    counts[base] += 1
    if counts[base] == 1:
        return base
    return f"{base} ({counts[base]})"


def table_headers_by_id(sheet_meta: dict[str, Any]) -> dict[int, dict[int, str]]:
    table_map: dict[int, dict[int, str]] = {}
    for table in sheet_meta.get("tables", []):
        table_id = table.get("table_id")
        if table_id is None:
            continue
        headers = parse_column_headers(table.get("column_headers", {}))
        if headers:
            table_map[int(table_id)] = headers
    return table_map


def headers_for_section(
    sheet_meta: dict[str, Any],
    section: dict[str, Any],
    fallback_headers: dict[int, str],
    table_map: dict[int, dict[int, str]],
) -> tuple[dict[int, str], int | None]:
    section_table_id = section.get("table_id")
    if section_table_id is not None:
        headers = table_map.get(int(section_table_id))
        if headers:
            return headers, int(section_table_id)

    child_table_ids = {
        int(child["table_id"])
        for child in section.get("children", [])
        if child.get("table_id") is not None
    }
    if len(child_table_ids) == 1:
        table_id = next(iter(child_table_ids))
        headers = table_map.get(table_id)
        if headers:
            return headers, table_id

    return fallback_headers, int(
        section_table_id
    ) if section_table_id is not None else None


def extract_sections_from_sheet(
    sheet_df: pd.DataFrame, sheet_meta: dict[str, Any]
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    first_col = sheet_df.iloc[:, 0]
    fallback_headers = parse_column_headers(sheet_meta.get("column_headers", {}))
    table_map = table_headers_by_id(sheet_meta)

    start_row = int(sheet_meta.get("data_start_row", 0))
    warnings: list[str] = []
    output: dict[str, pd.DataFrame] = {}
    key_counts: dict[str, int] = defaultdict(int)

    for idx, section in enumerate(sheet_meta.get("sections", []), start=1):
        label = str(section.get("label", "") or "")
        key = section_key(label, idx, key_counts)

        section_headers, section_table_id = headers_for_section(
            sheet_meta,
            section,
            fallback_headers,
            table_map,
        )
        selected_cols, rename_map = build_column_plan(sheet_df, section_headers)

        if label.strip():
            header_row = find_next_row(first_col, label, start_row)
            if header_row is None:
                warnings.append(f"Section header not found: {label}")
            else:
                start_row = header_row + 1

        row_matches: list[tuple[int, dict[str, Any]]] = []
        for child in section.get("children", []):
            child_label = str(child.get("label", "") or "")
            row_idx = find_next_row(first_col, child_label, start_row)
            if row_idx is None:
                warnings.append(f"Row not found in section '{key}': {child_label}")
                continue
            row_matches.append((row_idx, child))
            start_row = row_idx + 1

        if not row_matches:
            output[key] = pd.DataFrame(
                columns=[
                    "excel_row",
                    "table_id",
                    "role",
                    "indent",
                    "outline_level",
                    *rename_map.values(),
                ]
            )
            continue

        row_indices = [row_idx for row_idx, _ in row_matches]
        values = sheet_df.loc[row_indices, selected_cols].copy().reset_index(drop=True)
        values.rename(columns=rename_map, inplace=True)

        meta = pd.DataFrame(
            {
                "excel_row": [row_idx + 1 for row_idx, _ in row_matches],
                "table_id": [
                    child.get("table_id", section_table_id) for _, child in row_matches
                ],
                "role": [child.get("role", "line_item") for _, child in row_matches],
                "indent": [child.get("indent", 0) for _, child in row_matches],
                "outline_level": [
                    child.get("outline_level", 0) for _, child in row_matches
                ],
            }
        )

        output[key] = pd.concat([meta, values], axis=1)

    return output, warnings


def infer_excel_path(metadata_path: Path, workbook_name: str) -> Path:
    candidates = [
        metadata_path.parent / workbook_name,
        metadata_path.parent / "data" / workbook_name,
        metadata_path.parent.parent / "data" / workbook_name,
        Path.cwd() / workbook_name,
        Path.cwd() / "data" / workbook_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Excel file not found. Checked: {', '.join(str(c) for c in candidates)}"
    )


def load_sections_from_metadata(
    metadata: dict[str, Any],
    excel_path: str | Path,
    target_sheets: list[str] | None = None,
) -> tuple[dict[str, dict[str, pd.DataFrame]], list[str]]:
    excel_path = Path(excel_path)
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    target_set = set(target_sheets) if target_sheets else None

    all_sheets: dict[str, dict[str, pd.DataFrame]] = {}
    all_warnings: list[str] = []

    for sheet_meta in metadata.get("sheets", []):
        sheet_name = sheet_meta.get("sheet", "")
        if target_set and sheet_name not in target_set:
            continue

        sheet_df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
        section_dfs, warnings = extract_sections_from_sheet(sheet_df, sheet_meta)
        all_sheets[sheet_name] = section_dfs
        all_warnings.extend([f"[{sheet_name}] {w}" for w in warnings])

    return all_sheets, all_warnings


def load_sections_as_dataframes(
    metadata_path: str | Path,
    excel_path: str | Path | None = None,
    target_sheets: list[str] | None = None,
) -> tuple[dict[str, dict[str, pd.DataFrame]], list[str]]:
    metadata_path = Path(metadata_path)
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    if excel_path is None:
        workbook_name = metadata.get("workbook")
        if not workbook_name:
            raise ValueError(
                "Missing 'workbook' in metadata and no --excel path provided."
            )
        excel_path = infer_excel_path(metadata_path, workbook_name)

    return load_sections_from_metadata(
        metadata, excel_path, target_sheets=target_sheets
    )


def safe_slug(text: str) -> str:
    text = text.strip().lower()
    text = text.encode("ascii", errors="ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9._-]+", "_", text).strip("_")
    return text or "section"


def export_sections_to_csv(
    sheets: dict[str, dict[str, pd.DataFrame]],
    export_dir: str | Path,
) -> list[Path]:
    export_path = Path(export_dir)
    export_path.mkdir(parents=True, exist_ok=True)

    output_files: list[Path] = []
    for sheet_name, section_map in sheets.items():
        sheet_dir = export_path / safe_slug(sheet_name)
        sheet_dir.mkdir(parents=True, exist_ok=True)

        for section_name, df in section_map.items():
            out_file = sheet_dir / f"{safe_slug(section_name)}.csv"
            df.to_csv(out_file, index=False)
            output_files.append(out_file)

    return output_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load metadata sections from Excel into pandas DataFrames."
    )
    parser.add_argument(
        "--metadata",
        default="data/op_schema/test-generator.json",
        help="Path to metadata JSON.",
    )
    parser.add_argument(
        "--excel",
        default=None,
        help="Path to source XLSX file. If omitted, infer from metadata.",
    )
    parser.add_argument(
        "--sheet",
        default=None,
        help="Optional single worksheet name to load.",
    )
    parser.add_argument(
        "--export-dir",
        default=None,
        help="Optional directory to export each section DataFrame as CSV.",
    )
    args = parser.parse_args()

    target_sheets = [args.sheet] if args.sheet else None
    sheets, warnings = load_sections_as_dataframes(
        args.metadata,
        args.excel,
        target_sheets=target_sheets,
    )

    print("Loaded section DataFrames:")
    for sheet_name, section_map in sheets.items():
        print(f"- {sheet_name}: {len(section_map)} sections")
        for section_name, df in section_map.items():
            print(f"    {section_name}: shape={df.shape}")

    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"- {warning}")

    if args.export_dir:
        output_files = export_sections_to_csv(sheets, args.export_dir)
        print(f"\nExported {len(output_files)} CSV files to: {args.export_dir}")


if __name__ == "__main__":
    main()
