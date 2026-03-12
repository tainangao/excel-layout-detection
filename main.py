from __future__ import annotations

import json
from pathlib import Path

from load_sections_to_dfs import export_sections_to_csv, load_sections_from_metadata
from excel_schema_generator import generate_workbook_schema


def run_pipeline(
    workbook_path: str | Path,
    worksheet_name: str,
    schema_output_path: str | Path,
    csv_output_dir: str | Path,
    schema_mode: str = "auto",
) -> tuple[dict, list[str], list[Path]]:
    workbook_path = Path(workbook_path)
    schema_output_path = Path(schema_output_path)
    csv_output_dir = Path(csv_output_dir)

    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    print("[1/3] Generating schema...")
    schema = generate_workbook_schema(
        str(workbook_path),
        sheet_names=[worksheet_name],
        mode=schema_mode,
    )

    schema_output_path.parent.mkdir(parents=True, exist_ok=True)
    schema_output_path.write_text(
        json.dumps(schema, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"      Schema saved: {schema_output_path}")

    print("[2/3] Loading worksheet sections into DataFrames...")
    sheets, warnings = load_sections_from_metadata(
        schema,
        workbook_path,
        target_sheets=[worksheet_name],
    )
    if worksheet_name not in sheets:
        raise ValueError(f"Worksheet not found in generated schema: {worksheet_name}")

    print("[3/3] Exporting section DataFrames to CSV...")
    exported_files = export_sections_to_csv(
        {worksheet_name: sheets[worksheet_name]},
        csv_output_dir,
    )
    print(f"      Exported {len(exported_files)} CSV files to: {csv_output_dir}")

    return schema, warnings, exported_files


def main() -> None:
    # -----------------------------------------------------------------
    # PLACEHOLDERS: replace these values with your hard-coded arguments.
    # -----------------------------------------------------------------
    DATA_DIR = Path("data")
    WORKBOOK_PATH = Path("data/260225-4q-2025-data-pack-excel.xlsx")
    TARGET_WORKSHEET = "Credit risk"
    SCHEMA_OUTPUT_PATH = DATA_DIR / 'op_schema' / Path(f"{'-'.join(TARGET_WORKSHEET.lower().split())}.json")
    CSV_OUTPUT_DIR = DATA_DIR / 'op_csv'
    SCHEMA_MODE = "xml"  # auto | xml | pandas

    placeholders = [
        str(WORKBOOK_PATH),
        TARGET_WORKSHEET,
        str(SCHEMA_OUTPUT_PATH),
        str(CSV_OUTPUT_DIR),
    ]
    if any("REPLACE_WITH" in value for value in placeholders):
        raise ValueError(
            "Please update WORKBOOK_PATH, TARGET_WORKSHEET, SCHEMA_OUTPUT_PATH, "
            "and CSV_OUTPUT_DIR in run_schema_sections_pipeline.py."
        )

    _, warnings, _ = run_pipeline(
        workbook_path=WORKBOOK_PATH,
        worksheet_name=TARGET_WORKSHEET,
        schema_output_path=SCHEMA_OUTPUT_PATH,
        csv_output_dir=CSV_OUTPUT_DIR,
        schema_mode=SCHEMA_MODE,
    )

    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"- {warning}")


if __name__ == "__main__":
    main()
