#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unittest for src/qa_staging module."""

from src.settings import db_staging_file, schema_staging_file
import unittest
from src import qa_staging
import sqlite3
import os

class QaStagingTestCase(unittest.TestCase):
    """Run test for the module."""

    def setUp(self):
        """Initialize test."""
        with sqlite3.connect(db_staging_file) as conn:
            with open(schema_staging_file, 'r') as schema:
                conn.executescript(schema.read())
        self.login_content = [('2018-05-04 16:51:37.3758518', 'websiteF', '565560'),
                              ('2020-01-29 09:17:53.3237453', 'websiteZ', '566115'),
                              ('2019-02-16 02:20:07.5570161', 'websiteX', '564512'),
                              ('2019-04-11 18:36:59.4178461', 'websiteZ', '565588'),
                              ('2019-04-11 18:36:59.4178461', 'websiteZ', '5655AA'),
                              ('2019-04-11 18:36:59.4178461', 'website3', '565588'),
                              ('2019-04-11 18:36:59.4178461', '', ''),
                              ('2018-06-07 13:47:01.9888069', 'websiteE', '565823')]
        self.registration_content = [('2018-01-01 00:03:00.3136468','websiteF','riio@uqkry-.org','1988-12-13 16:57:40.7219400','Justin Dorsey','Rickey Villa','960 Nobel Boulevard','40 Fabien Boulevard','Michigan', '', 'Arkansas','63 Second St.','2018-01-01 00:03:00.3136468','477952'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net', '', 'Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','','', '','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','@nzcly.lfebpe.net', '1998-12-13 16:57:40.7219400','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','sd.kyjs@nzcly.lfebpe.net', '','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net', '1985-12-13 16:57:40.7219400','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net', '1998-12-23 16:57:40.7219400','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135'),
                                    ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net', '','Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135')]
        self.games_content = [('2018-11-13 06:30:52.1998217','websiteH','667124','eur','0716','gslsngoldenesieben','', 2.0, 0.4,'GOV7', 0.0),	
                            ('2019-01-17 00:55:44.1996456','websiteZ','666294','eur','0148','Nikolos','',3.0, 0.8,'HUC49302', 0.0),	
                            ('2019-01-17 00:55:44.1996456','websiteZ','','eur','','Nikolos','','', 0.8 ,'HUC49302', 0.0),	
                            ('2019-01-17 00:55:44.1996456','websiteZ','666294','eur','0148','Nikolos','',-3.0, -0.8,'HUC49302', 0.0)]
        self.lottery_content = [('1558931809','websiteA','132197','eur', 600.0, 60.0,'LOTTO','ZAH1258', 0.0, '34771','12','{"discountInMinor":120,"type":"marketing"}'),
                                ('1544542681','websiteE','131323','eur', 750.0, -75.0,'CASH_4_LIFE','HAC4670', 0.0, '93454','3','{"discountInMinor":120,"type":"marketing"}'),
                                ('1544542681','websiteE','131323','eur', -750.0, 75.0,'CASH_4_LIFE','HAC4670', 0.0, '93454','3','{"discountInMinor":120,"type":"marketing"}'),
                                ('1544542681','websiteE','131323','eur','', 75.0,'CASH_4_LIFE','HAC4670', 0.0, '93454','3','{"discountInMinor":120,"type":"marketing"}')]

    def tearDown(self):
        """Clean up after test."""
        if os.path.exists(db_staging_file):
            os.remove(db_staging_file)
    
    def test_update_audit_table(self):
        """Test update_audit_table method."""
        qa_staging.update_audit_table()
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM audit_events;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 1)

    def test_run_qa_logins(self):
        """Test run_qa_logins method."""
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO customer_logins_stg VALUES (?, ?, ?);", self.login_content)
            conn.commit()
        n_rows_qrt, n_rows_clean = qa_staging.run_qa_logins(debug=1)
        self.assertEqual(n_rows_qrt, 3)
        self.assertEqual(n_rows_clean, 5)

    def test_run_qa_registration(self):
        """Test run_qa_registration method."""
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO customer_registration_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", self.registration_content)
            conn.commit()
        n_rows_qrt, n_rows_clean = qa_staging.run_qa_registration(debug=1)
        self.assertEqual(n_rows_qrt, 8)
        self.assertEqual(n_rows_clean, 0)

    def test_run_qa_games(self):
        """Testrun_qa_games method."""
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO games_purchase_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", self.games_content)
            conn.commit()
        n_rows_qrt, n_rows_clean = qa_staging.run_qa_games(debug=1)
        self.assertEqual(n_rows_qrt, 2)
        self.assertEqual(n_rows_clean, 2)
    
    def test_run_qa_lottery(self):
        """Test run_qa_lottery method."""
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO lottery_purchase_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", self.lottery_content)
            conn.commit()
        n_rows_qrt, n_rows_clean = qa_staging.run_qa_lottery(debug=1)
        self.assertEqual(n_rows_qrt, 3)
        self.assertEqual(n_rows_clean, 1)

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)