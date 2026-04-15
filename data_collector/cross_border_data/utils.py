from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import csv
import json
from typing import Iterable

from .schemas import FIELDNAMES_BY_TABLE, ordered_row


def ensure_parent(path: str | Path) -> Path:
    resolved = Path(path).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def write_rows(table_name: str, rows: Iterable[dict], output_path: str | Path) -> Path:
    output_file = ensure_parent(output_path)
    rows = [ordered_row(table_name, row) for row in rows]
    fieldnames = list(FIELDNAMES_BY_TABLE[table_name])

    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with output_file.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_file


def write_json(payload: dict | list, output_path: str | Path) -> Path:
    output_file = ensure_parent(output_path)
    with output_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return output_file


def read_csv_rows(input_path: str | Path) -> list[dict]:
    with Path(input_path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def utc_now_text() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat(sep=" ")


def iso_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def as_float(value: object) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def as_int(value: object) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None
