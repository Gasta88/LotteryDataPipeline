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
        generate_reports.generate_billing(by='type')
        # how to verify that the file is created and populated....?
    
    def test_generate_active_customers(self):
        """Test active customers report by a certain parameter."""
        pass
    
    def test_generate_avg_checkout(self):
        """Test average checkout report by yearly quarter."""
        pass
    
    def test_generate_monthly_diff(self):
        """Test monthly difference for each customer."""
        pass
    
    
if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)