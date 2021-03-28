#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unittest for src/generate_reports module."""

from src.settings import db_production_file, schema_production_file, report_folder
import unittest
from src import generate_reports
import sqlite3
import shutil
import os

class GenerateReportsTestCase(unittest.TestCase):
    """Run test for the module."""
    
    def setUp(self):
        """Initialize test."""
        with sqlite3.connect(db_production_file) as conn:
            with open(schema_production_file, 'r') as schema:
                conn.executescript(schema.read())
    
    def tearDown(self):
        """Clean up after test."""
        shutil.rmtree(db_production_file)
        for file in os.listdir(report_folder):
            if file.endswith('.csv'):
                shutil.rmtree(os.path.join(report_folder, file))

        
    def test_generate_billing(self):
        """Test billing report by a certain parameter."""
        product_content = [('1','cash_4_life','lottery_game'),
                           ('32','lotto','lottery_game')]
        ticket_content = [('34771ZAH1258132197','32','12','eur','6.0','0.6','1'),
                          ('93454HAC4670131323','1','3','eur','7.5','0.75','1')]
        booking_content = [('ZAH1258','132197','34771ZAH1258132197','2019-05-27 06:36:49','websiteA'),
                           ('HAC4670','131323','93454HAC4670131323','2018-12-11 16:38:01','websiteE')]
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO product VALUES (?, ?, ?);",
                            product_content)
            cur.executemany("INSERT INTO ticket VALUES (?, ?, ?, ?, ?, ?, ?);",
                            ticket_content)
            cur.executemany("INSERT INTO booking VALUES (?, ?, ?, ?, ?);",
                            booking_content)
            conn.commit()
        file_name = generate_reports.generate_billing(by='type', debug=1)
        self.assertEqual(file_name, 'tests/reports/billing_by_type.csv')
        self.assertTrue(os.stat(file_name).st_size > 0)

    def test_generate_active_customers(self):
        """Test active customers report by a certain parameter."""
        customer_content = [('565588','ivht871@yenqaf.com','1992-07-20 17:30:51.8694590','Darius Chase','Ruby Downs','24 Nobel Blvd.','','Iowa','2886','Vermont','193 Second Parkway'),
                            ('566115','tizs57@sljwnl.org','964-09-15 07:12:16.8060203','Andres Avila','Naomi Mann','647 Rocky Oak Freeway','341 Milton Blvd.','Pennsylvania','32659','Iowa','160 Rocky Hague Street')]
        login_content = [('66115','websiteZ','2020-01-29 09:17:53.3237453'),
                        ('65588','websiteZ','2019-04-11 18:36:59.4178461'),
                        ('65588','websiteZ','2019-04-27 05:01:48.0632458'),
                        ('66115','websiteH','2018-11-09 18:16:21.4938017'),
                        ('65588','websiteC','2018-02-04 20:16:08.7212061'),
                        ('65588','websiteC','2018-10-19 05:38:38.1437959'),
                        ('66115','websiteD','2019-10-21 08:12:24.8212136'),
                        ('66115','websiteD','2018-12-13 07:28:43.9230967'),
                        ('65588','websiteE','2018-10-20 10:52:03.4971646'),
                        ('65588','websiteZ','2019-06-21 02:05:36.8349230'),
                        ('66115','websiteF','2018-01-31 13:06:49.1726433'),
                        ('66115','websiteA','2018-10-07 18:44:17.3515804'),
                        ('65588','websiteF','2019-05-04 03:56:19.7317452'),
                        ('66115','websiteC','2018-04-19 12:40:37.2774873'),
                        ('66115','websiteA','2019-05-06 13:49:14.7931158'),
                        ('65588','websiteC','2018-05-25 13:18:42.0169767'),
                        ('66115','websiteH','2018-12-17 13:06:21.3828122')]
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO customer_registration_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                            customer_content)
            cur.executemany("INSERT INTO customer_logins_stg VALUES (?, ?, ?);",
                            login_content)
            conn.commit()
        file_name = generate_reports.generate_billing(by='month', debug=1)
        self.assertEqual(file_name, 'tests/reports/active_customers_by_month.csv')
        self.assertTrue(os.stat(file_name).st_size > 0)
        
    def test_generate_avg_checkout(self):
        """Test average checkout report by yearly quarter."""
        customer_content = [('132175','obkpa02@nkytxk.org','1954-11-22 04:27:44.0627444','Gena Hunter','Maribel Garza','79 Old Street','949 Rocky Clarendon Avenue','Ohio','8237','Kansasv235','Green Milton Boulevard'),
                            ('132197','ndkhad.njftcwiyf@ywkbbx.com','1979-03-25 13:35:34.8481361','Brenda Russo','Ian Riggs','69 Green Nobel St.','58 Rocky First Avenue','Indiana','47089','Kansas','996 East White Milton St.')]
        booking_content = [('ZAH1258','132197','34771ZAH1258132197','2019-05-27 06:36:49','websiteA'),
                           ('ZIN9206','132175','83972ZIN9206132175','2018-06-21 18:40:56','websiteA')]
        ticket_content = [('34771ZAH1258132197','32','12','eur','6.0','0.6','1'),
                          ('83972ZIN9206132175','32','2','eur','1.0','0.1','1')]
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO customer_registration_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                            customer_content)
            cur.executemany("INSERT INTO ticket VALUES (?, ?, ?, ?, ?, ?, ?);",
                            ticket_content)
            cur.executemany("INSERT INTO booking VALUES (?, ?, ?, ?, ?);",
                            booking_content)
            conn.commit()
        file_name = generate_reports.generate_avg_checkout(debug=1)
        self.assertEqual(file_name, 'tests/reports/avg_customer_basket.csv')
        self.assertTrue(os.stat(file_name).st_size > 0)
    
    def test_generate_monthly_diff(self):
        """Test monthly difference for each customer."""
        customer_content = [('132175','obkpa02@nkytxk.org','1954-11-22 04:27:44.0627444','Gena Hunter','Maribel Garza','79 Old Street','949 Rocky Clarendon Avenue','Ohio','8237','Kansasv235','Green Milton Boulevard'),
                            ('132197','ndkhad.njftcwiyf@ywkbbx.com','1979-03-25 13:35:34.8481361','Brenda Russo','Ian Riggs','69 Green Nobel St.','58 Rocky First Avenue','Indiana','47089','Kansas','996 East White Milton St.')]
        booking_content = [('ZAH1258','132197','34771ZAH1258132197','2019-05-27 06:36:49','websiteA'),
                           ('ZIN9206','132175','83972ZIN9206132175','2018-06-21 18:40:56','websiteA')]
        ticket_content = [('34771ZAH1258132197','32','12','eur','6.0','0.6','1'),
                          ('83972ZIN9206132175','32','2','eur','1.0','0.1','1')]
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO customer_registration_stg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                            customer_content)
            cur.executemany("INSERT INTO ticket VALUES (?, ?, ?, ?, ?, ?, ?);",
                            ticket_content)
            cur.executemany("INSERT INTO booking VALUES (?, ?, ?, ?, ?);",
                            booking_content)
            conn.commit()
        file_name = generate_reports.generate_monthly_diff(debug=1)
        self.assertEqual(file_name, 'tests/reports/monthly_diff_customer.csv')
        self.assertTrue(os.stat(file_name).st_size > 0)
    
    
if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)