#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRODUCT_DIR="${RAW_PRODUCTS_DIR:-$ROOT_DIR/data_platform/storage/raw/json/products}"
RETENTION_DAYS="${RAW_PRODUCTS_RETENTION_DAYS:-14}"
MODE="${1:---dry-run}"

usage() {
    echo "Usage: bash scripts/cleanup_raw_products.sh [--dry-run|--apply]"
}

case "$MODE" in
    --dry-run|dry-run)
        APPLY=false
        ;;
    --apply|apply)
        APPLY=true
        ;;
    *)
        usage
        exit 1
        ;;
esac

if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]] || [[ "$RETENTION_DAYS" -lt 1 ]]; then
    echo "RAW_PRODUCTS_RETENTION_DAYS must be a positive integer"
    exit 1
fi

if [[ ! -d "$PRODUCT_DIR" ]]; then
    echo "raw products directory not found: $PRODUCT_DIR"
    exit 1
fi

mtime_threshold=$((RETENTION_DAYS - 1))

tmp_list="$(mktemp)"
trap 'rm -f "$tmp_list"' EXIT

find "$PRODUCT_DIR" -type f \( -name "*.json" -o -name "*.json.gz" \) ! -name "*.meta.json" -mtime +"$mtime_threshold" -print > "$tmp_list"

tmp_payloads="$(mktemp)"
trap 'rm -f "$tmp_list" "$tmp_payloads"' EXIT
cp "$tmp_list" "$tmp_payloads"

while IFS= read -r payload; do
    [[ -z "$payload" ]] && continue
    if [[ "$payload" == *.json.gz ]]; then
        meta_path="${payload%.json.gz}.meta.json"
    else
        meta_path="${payload%.json}.meta.json"
    fi
    if [[ -f "$meta_path" ]]; then
        printf '%s\n' "$meta_path" >> "$tmp_list"
    fi
done < "$tmp_payloads"

find "$PRODUCT_DIR" -type f -name "*.meta.json" -mtime +"$mtime_threshold" -print >> "$tmp_list"

file_count="$(sort -u "$tmp_list" | sed '/^$/d' | wc -l | tr -d ' ')"

echo "raw products dir: $PRODUCT_DIR"
echo "retention days: $RETENTION_DAYS"
echo "mode: $MODE"
echo "matched files: $file_count"

if [[ "$file_count" == "0" ]]; then
    exit 0
fi

echo "sample files:"
sort -u "$tmp_list" | sed '/^$/d' | head -n 10

if [[ "$APPLY" != true ]]; then
    echo "dry-run only; rerun with --apply to delete matched files"
    exit 0
fi

sort -u "$tmp_list" | sed '/^$/d' | while IFS= read -r target; do
    rm -f "$target"
done

echo "deleted matched files"