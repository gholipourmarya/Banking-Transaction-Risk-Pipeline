"""Small dependency-free ETL example. Input is synthetic demonstration data."""
import csv
import json
import sqlite3
from contextlib import closing
from math import isfinite
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DOMAIN = "banking"

def transform(row):
    """Return normalized record or raise ValueError for invalid source rows."""
    out = dict(row)
    try:
        if DOMAIN == 'retail':
            qty, price, stock = int(row['quantity']), float(row['unit_price']), int(row['stock'])
            if min(qty, price, stock) < 0: raise ValueError('negative retail value')
            out['value'] = round(qty * price, 2)
            out['flag'] = int(stock < 15)
        elif DOMAIN == 'iot':
            temp, vibration, pressure = float(row['temperature_c']), float(row['vibration_mm_s']), float(row['pressure_bar'])
            if pressure < 0: raise ValueError('negative pressure')
            out['value'] = 1.0
            out['flag'] = int(temp > 85 or vibration > 6)
        elif DOMAIN == 'banking':
            account_id = row.get('account_id')
            if not isinstance(account_id, str) or not account_id.strip():
                raise ValueError('account_id is required')
            amount = float(row['amount'])
            if not isfinite(amount) or amount < 0:
                raise ValueError('amount must be finite and nonnegative')
            hour = int(row['hour'])
            if not 0 <= hour <= 23:
                raise ValueError('hour must be between 0 and 23')
            out['value'] = amount
            out['flag'] = int(amount >= 10000 or hour < 5)
        elif DOMAIN == 'energy':
            consumption = float(row['consumption_kwh'])
            if consumption < 0: raise ValueError('negative consumption')
            out['value'] = consumption
            out['flag'] = int(consumption > 30)
        elif DOMAIN == 'healthcare':
            tests = int(row['lab_tests'])
            if tests < 0 or row['status'] not in ('completed', 'cancelled', 'scheduled'): raise ValueError('invalid appointment')
            out['value'] = 1.0
            out['flag'] = int(row['status'] == 'cancelled')
        else: raise ValueError('unknown domain')
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f'Invalid row: {exc}') from exc
    return out

def run():
    input_path = BASE / 'data' / 'input.csv'
    output = BASE / 'output'
    output.mkdir(exist_ok=True)
    valid, rejected = [], []
    with input_path.open(newline='', encoding='utf-8') as handle:
        for row in csv.DictReader(handle):
            try: valid.append(transform(row))
            except ValueError as exc: rejected.append({'row': row, 'reason': str(exc)})
    with closing(sqlite3.connect(output / 'warehouse.db')) as con, con:
        con.execute('BEGIN')
        con.execute('DROP TABLE IF EXISTS events')
        con.execute('CREATE TABLE events (group_key TEXT NOT NULL, value REAL NOT NULL, flag INTEGER NOT NULL)')
        con.executemany('INSERT INTO events VALUES (?, ?, ?)', [(r['account_id'], r['value'], r['flag']) for r in valid])
        result = con.execute('SELECT group_key, COUNT(*), ROUND(SUM(value),2), SUM(flag) FROM events GROUP BY group_key ORDER BY group_key').fetchall()
        summary = [
            {'group': group, 'records': count, 'total_value': total, 'flags': flags}
            for group, count, total, flags in result
        ]
        try:
            summary_json = json.dumps(summary, indent=2, allow_nan=False)
        except ValueError as exc:
            raise RuntimeError('Transaction totals exceed the supported numeric range') from exc
    with (output / 'clean.jsonl').open('w', encoding='utf-8') as handle:
        for row in valid:
            handle.write(json.dumps(row, allow_nan=False) + '\n')
    (output / 'rejected.json').write_text(json.dumps(rejected, indent=2), encoding='utf-8')
    (output / 'summary.json').write_text(summary_json, encoding='utf-8')
    if not valid:
        raise RuntimeError('No valid records. See output/rejected.json for rejected rows.')
    print(json.dumps({'accepted': len(valid), 'rejected': len(rejected), 'summary': summary}, indent=2))
    return summary

if __name__ == '__main__': run()
