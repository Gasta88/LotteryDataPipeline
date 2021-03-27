#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unittest for src/init_dbs module."""

import unittest
from src import init_dbs
from src.settings import db_staging_file, db_production_file, schema_staging_file
import sqlite3
import os
import shutil
import tempfile


class InitDbsTestCase(unittest.TestCase):
    """Run test for the module."""

    # def setUp(self):
    #     """Initialize test."""
    #     pass
        
    def tearDown(self):
        """Clean up after test."""
        shutil.rmtree(db_staging_file)
        shutil.rmtree(db_production_file)

    def test_init_staging_database(self):
        """Test staging db initialization."""
        init_dbs.init_staging_database()
        self.assertTrue(os.path.isfile(db_staging_file))
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customer_logins_stg;")
            rows = cur.fetchall()
            self.assertEqual(rows[0], 0)
            

    def test_init_production_database(self):
        """Test production db initialization."""
        init_dbs.init_production_database()
        self.assertTrue(os.path.isfile(db_production_file))
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customer;")
            rows = cur.fetchall()
            self.assertEqual(rows[0], 0)

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
        with tempfile.TemporaryDirectory() as tempdir:
            good_tmpfilepath = os.path.join(tempdir, 'good_customerlogin.csv')
            bad_tmpfilepath = os.path.join(tempdir, 'bad_customerlogin.csv')
            testing_files = [good_tmpfilepath, bad_tmpfilepath]
            with open(good_tmpfilepath, 'w') as good_tmpfile, open(bad_tmpfilepath, 'w') as bad_tmpfile:
                good_tmpfile.write("timestamp;site;customernumber".encode('latin-1'))
                bad_tmpfile.write("timestamp/site/customernumber".encode('latin-1'))
                file_to_tables = init_dbs.validate_file_header(testing_files)
                self.assertCountEqual(list(file_to_tables.keys()),
                                      ['customer_logins_stg'])

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
                tmpfile.write(content.encode('latin-1'))
            init_dbs.load_file_in_staging(tmpfilepath,
                                          'customer_logins_stg')
            with sqlite3.connect(db_staging_file) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM customer_logins_stg;")
                rows = cur.fetchall()
                self.assertEqual(rows[0], 3)
        

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)
