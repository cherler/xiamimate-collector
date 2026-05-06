#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib import error, request

LEVEL_LABELS = {
    "info": "[INFO]",
    "success": "[SUCCESS]",
    "warning": "[WARNING]",
    "error": "[ERROR]",
}


def parse_summary(output: str) -> tuple[int, int]:
    errors = 0
    warnings = 0
    for line in output.splitlines():
        if line.startswith("=== summary:"):
            parts = line.replace("===", "").replace(":", " ").split()
            for part in parts:
                if part.startswith("errors="):
                    errors = int(part.split("=", 1)[1])
                elif part.startswith("warnings="):
                    warnings = int(part.split("=", 1)[1])
    return errors, warnings


def issue_lines(output: str, limit: int = 30) -> list[str]:
    lines = []
    for line in output.splitlines():
        if line.startswith("ERROR:") or line.startswith("WARN:"):
            lines.append(line)
    return lines[:limit]


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_time(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def build_feishu_payload(*, title: str, level: str, message: str, lines: list[str], facts: list[str]) -> dict:
    content_lines = [f"{LEVEL_LABELS.get(level, '[INFO]')} {title}", message]
    content_lines.extend(line for line in lines if line)
    for fact in facts:
        if not fact:
            continue
        if "=" in fact:
            key, value = fact.split("=", 1)
            content_lines.append(f"{key.strip()}: {value.strip()}")
        else:
            content_lines.append(fact)
    content_lines.append(f"sent_at: {datetime.now().astimezone().isoformat(timespec='seconds')}")
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [[{"tag": "text", "text": line}] for line in content_lines],
                }
            }
        },
    }


def send_webhook(webhook_url: str, payload: dict, timeout_seconds: float) -> dict:
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=payload_bytes,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {"ok": True, "http_status": getattr(response, "status", 200), "response": body}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"webhook http error {exc.code}: {body}") from exc
    except Exception as exc:
        raise RuntimeError(f"webhook request failed: {exc}") from exc


def should_notify(state: dict, status: str, signature: str, cooldown_seconds: int) -> bool:
    previous_signature = state.get("last_signature")
    last_notified_at = parse_time(state.get("last_notified_at"))
    now = datetime.now(timezone.utc)
    if signature != previous_signature:
        return True
    if last_notified_at is None:
        return True
    return (now - last_notified_at).total_seconds() >= cooldown_seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Notify Feishu for ECS2 collector healthcheck warnings or errors.")
    parser.add_argument("--healthcheck-output-file", required=True)
    parser.add_argument("--healthcheck-exit-code", type=int, required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--webhook-url", default=os.environ.get("FEISHU_WEBHOOK_URL"))
    parser.add_argument("--cooldown-seconds", type=int, default=3600)
    parser.add_argument("--notify-recovery", action="store_true")
    parser.add_argument("--host", default=os.uname().nodename)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.healthcheck_output_file)
    state_path = Path(args.state_file)
    output = output_path.read_text(encoding="utf-8", errors="replace")
    errors, warnings = parse_summary(output)
    lines = issue_lines(output)

    if errors > 0 or args.healthcheck_exit_code != 0:
        status = "error"
        level = "error"
    elif warnings > 0:
        status = "warning"
        level = "warning"
    else:
        status = "ok"
        level = "success"

    state = load_state(state_path)
    previous_status = state.get("last_status")
    signature_source = "\n".join([status, str(errors), str(warnings), *lines])
    signature = hashlib.sha256(signature_source.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc)

    if not args.webhook_url:
        print("healthcheck notifier: FEISHU_WEBHOOK_URL not configured; skip notification")
        state.update({"last_status": status, "last_signature": signature})
        save_state(state_path, state)
        return 0

    if status in {"warning", "error"}:
        if should_notify(state, status, signature, args.cooldown_seconds):
            title = f"ECS2 Collector Healthcheck {status.upper()}"
            payload = build_feishu_payload(
                title=title,
                level=level,
                message="ECS2 collector healthcheck detected warnings or errors.",
                lines=lines or ["No WARN/ERROR lines captured; inspect healthcheck log."],
                facts=[
                    f"host={args.host}",
                    f"errors={errors}",
                    f"warnings={warnings}",
                    f"exit_code={args.healthcheck_exit_code}",
                    "log=/data/xiamimate/collector/logs/collector_healthcheck.timer.log",
                ],
            )
            result = send_webhook(args.webhook_url, payload, args.timeout_seconds)
            print(json.dumps(result, ensure_ascii=False))
            state["last_notified_at"] = now.isoformat()
            state["last_notification_kind"] = "alert"
        else:
            print("healthcheck notifier: alert suppressed by signature/cooldown")
    elif args.notify_recovery and previous_status in {"warning", "error"}:
        payload = build_feishu_payload(
            title="ECS2 Collector Healthcheck RECOVERED",
            level="success",
            message="ECS2 collector healthcheck is back to OK.",
            lines=[],
            facts=[f"host={args.host}", "errors=0", "warnings=0"],
        )
        result = send_webhook(args.webhook_url, payload, args.timeout_seconds)
        print(json.dumps(result, ensure_ascii=False))
        state["last_notified_at"] = now.isoformat()
        state["last_notification_kind"] = "recovery"

    state["last_status"] = status
    state["last_signature"] = signature
    state["updated_at"] = now.isoformat()
    save_state(state_path, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
