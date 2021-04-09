#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unittest for src/init_dbs module."""

import unittest
from src import init_dbs
from src.settings import db_staging_file, db_production_file, schema_staging_file
import sqlite3
import os
import tempfile
import shutil


class InitDbsTestCase(unittest.TestCase):
    """Run test for the module."""

    # def setUp(self):
    #     """Initialize test."""
    #     print(db_staging_file)
    #     print(db_production_file)
    #     print(schema_staging_file)
        
    def tearDown(self):
        """Clean up after test."""
        if os.path.exists(db_staging_file):
            os.remove(db_staging_file)
        if os.path.exists(db_production_file):
            os.remove(db_production_file)

    def test_init_staging_database(self):
        """Test staging db initialization."""
        init_dbs.init_staging_database()
        self.assertTrue(os.path.isfile(db_staging_file))
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customer_logins_stg;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 0)
            

    def test_init_prod_database(self):
        """Test production db initialization."""
        init_dbs.init_prod_database()
        self.assertTrue(os.path.isfile(db_production_file))
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customer;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 0)

    def test_validate_folders_and_files(self):
        """Test validation of folders and files."""
        with tempfile.TemporaryDirectory() as tempdir:
            good_tmpfilepath = os.path.join(tempdir, 'good_test_file.csv')
            bad_tmpfilepath = os.path.join(tempdir, 'bad_test_file.tab')
            with open(good_tmpfilepath, 'w') as good_tmpfile, open(bad_tmpfilepath, 'w') as bad_tmpfile:
                good_tmpfile.write("good test file")
                bad_tmpfile.write("bad test file")
                file_paths = init_dbs.validate_folder_and_files(tempdir)
                self.assertCountEqual(file_paths,
                                      [good_tmpfilepath])

    def test_validate_file_header(self):
        """Test validation of file header."""
        tempdir = 'data/test'
        os.mkdir(tempdir)
        good_tmpfilepath = os.path.join(tempdir, 'good_customerlogins.csv')
        bad_tmpfilepath = os.path.join(tempdir, 'bad_customerlogins.csv')
        with open(good_tmpfilepath, 'w') as good_tmpfile, open(bad_tmpfilepath, 'w') as bad_tmpfile:
            good_tmpfile.write("timestamp;site;customernumber")
            bad_tmpfile.write("wrong/wrongAgain/stillWrong")
        testing_files = [good_tmpfilepath, bad_tmpfilepath]
        file_to_tables = init_dbs.validate_file_header(testing_files)
        self.assertCountEqual(list(file_to_tables.keys()),
                              ['customer_logins_stg', 
                               'customer_registration_stg',
                               'games_purchase_stg',
                               'lottery_purchase_stg'])
        self.assertEqual(len(file_to_tables['customer_logins_stg']),
                         1)
        shutil.rmtree(tempdir)

    def test_load_file_in_staging(self):
        """Test loading files in staging db."""
        with sqlite3.connect(db_staging_file) as conn:
            with open(schema_staging_file, 'r') as schema:
                conn.executescript(schema.read())
        with tempfile.TemporaryDirectory() as tempdir:
            tmpfilepath = os.path.join(tempdir, 'customerlogins.csv')
            with open(tmpfilepath, 'w') as tmpfile:
                content = """timestamp;site;customernumber\n
                             2020-10-25;websiteA;1458756\n
                             2020-10-28;websiteB;1458786\n
                             2020-12-25;websiteC;1428756\n
                          """
                tmpfile.write(content)
            init_dbs.load_file_in_staging(tmpfilepath,
                                          'customer_logins_stg')
            with sqlite3.connect(db_staging_file) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM customer_logins_stg;")
                rows = cur.fetchall()
                self.assertEqual(rows[0][0], 3)
        

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)
