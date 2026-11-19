import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.pipeline import transform, run

class PipelineTests(unittest.TestCase):
    def test_valid_row(self):
        import csv
        path = Path(__file__).resolve().parents[1] / 'data' / 'input.csv'
        with path.open(newline='') as f: row = next(csv.DictReader(f))
        self.assertIn('value', transform(row))
        self.assertIn('flag', transform(row))

    def test_pipeline(self):
        self.assertTrue(run())

    def test_invalid_transactions_are_rejected(self):
        valid = {'account_id': 'A1', 'amount': '100', 'hour': '12'}
        invalid_rows = [
            ({'amount': '100', 'hour': '12'}, 'account_id'),
            ({**valid, 'account_id': None}, 'account_id'),
            ({**valid, 'account_id': ''}, 'account_id'),
            ({**valid, 'account_id': '  '}, 'account_id'),
            ({**valid, 'amount': 'NaN'}, 'amount'),
            ({**valid, 'amount': 'inf'}, 'amount'),
            ({**valid, 'amount': '-1'}, 'amount'),
            ({**valid, 'hour': '24'}, 'hour'),
        ]
        for row, reason in invalid_rows:
            with self.subTest(row=row), self.assertRaisesRegex(ValueError, reason):
                transform(row)

    def test_all_rejected_refreshes_outputs(self):
        with TemporaryDirectory() as directory, patch('src.pipeline.BASE', Path(directory)):
            base = Path(directory)
            source = base / 'data' / 'input.csv'
            source.parent.mkdir()
            source.write_text('account_id,amount,hour\nA1,100,12\n', encoding='utf-8')
            run()

            source.write_text('account_id,amount,hour\n,100,12\nA1,NaN,12\n', encoding='utf-8')
            with self.assertRaisesRegex(RuntimeError, 'No valid records'):
                run()

            output = base / 'output'
            rejected = json.loads((output / 'rejected.json').read_text(encoding='utf-8'))
            self.assertEqual(len(rejected), 2)
            self.assertIn('account_id', rejected[0]['reason'])
            self.assertIn('amount', rejected[1]['reason'])
            self.assertEqual((output / 'clean.jsonl').read_text(encoding='utf-8'), '')
            self.assertEqual(json.loads((output / 'summary.json').read_text(encoding='utf-8')), [])
            with closing(sqlite3.connect(output / 'warehouse.db')) as con:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM events').fetchone(), (0,))

if __name__ == '__main__': unittest.main()
