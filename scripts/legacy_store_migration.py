"""Reviewed JSONL receipts -> repeatable v1 backfill. Dry-run is the default.

Inputs are sensitive. Reports never contain receipts, tokens, provider errors,
account UUIDs, or raw transaction IDs. Grant and binding approval are separate.
"""
from __future__ import annotations
import argparse
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'api'))
from fastapi import HTTPException
from app.platform.config import get_settings
from app.platform.legacy_migration import approve_binding, process_legacy, reconcile
from app.platform.legacy_verifier import verify_legacy


def run_batch(settings, rows, *, mode='grant', apply=False):
    for number, line in enumerate(rows, 1):
        try:
            if len(line) > 3*1024*1024:
                raise ValueError()
            row = json.loads(line)
            verification = dict(platform=row['platform'], product_id=row['productId'],
                verification_data=row['verificationData'], transaction_id=row.get('transactionId'))
            if mode == 'reconcile':
                result = reconcile(settings, purchase=verify_legacy(settings, **verification), apply=apply)
            else:
                user_id = str(uuid.UUID(row['userId']))
                if mode == 'bind':
                    result = approve_binding(settings, user_id=user_id, evidence_ref=row['bindingEvidenceRef'], apply=apply, **verification)
                else:
                    result = process_legacy(settings, user_id=user_id, apply=apply, **verification)
            yield {'line': number, 'mode': mode, 'apply': apply, **{
                k: v for k, v in result.items() if k in ('version', 'status', 'creditsGranted', 'debited', 'reconciliationCredits')}}
        except HTTPException as exc:
            # Coarse codes only. Do not interpolate input or exception text.
            yield {'line': number, 'status': 'rejected', 'httpStatus': exc.status_code,
                   'retryable': exc.status_code >= 500}
        except (ValueError, KeyError, TypeError):
            yield {'line': number, 'status': 'invalid_input', 'retryable': False}
        except Exception:
            yield {'line': number, 'status': 'failed', 'retryable': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True, help='restricted local JSONL file')
    parser.add_argument('--report', type=Path, required=True, help='new redacted JSONL report (never overwritten)')
    parser.add_argument('--mode', choices=('grant', 'bind', 'reconcile'), default='grant')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    failed = False
    with args.input.open(encoding='utf-8') as source, args.report.open('x', encoding='utf-8') as target:
        for result in run_batch(get_settings(), source, mode=args.mode, apply=args.apply):
            failed |= result['status'] in ('rejected', 'invalid_input', 'failed')
            target.write(json.dumps(result, ensure_ascii=False) + '\n')
            target.flush()
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
