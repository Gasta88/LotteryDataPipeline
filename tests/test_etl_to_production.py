#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unittest for src/etl_to_production module."""

from src.settings import db_staging_file, db_production_file, schema_staging_file, \
    schema_production_file
import unittest
from src import etl_to_production
import sqlite3
import os
import pandas as pd

class ETLtoProdTestCase(unittest.TestCase):
    """Run test for the module."""
    
    def setUp(self):
        """Initialize test."""
        with sqlite3.connect(db_staging_file) as conn:
            with open(schema_staging_file, 'r') as schema:
                conn.executescript(schema.read())
            sql = "INSERT INTO audit_events (last_event) VALUES (current_timestamp);"
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        with sqlite3.connect(db_production_file) as conn:
            with open(schema_production_file, 'r') as schema:
                conn.executescript(schema.read())
    
    def tearDown(self):
        """Clean up after test."""
        os.remove(db_staging_file)
        os.remove(db_production_file)

    def test_prepare_discount_table(self):
        """Extact information from discount_vw and prepare production table."""
        lottery_content = [('1558931809','websiteA','132197','eur',600.0,60.0,'LOTTO','ZAH1258','','34771','12','{"discountInMinor":10,"type":"marketing"}'),
                           ('1558931809','websiteA','132197','eur',700.0,70.0,'LOTTO','ZBH1258','','33771','12','{"discountInMinor":80,"type":"local"}'),
                           ('1558931809','websiteA','132197','eur',800.0,80.0,'LOTTO','ZCH1258','','32771','12','{"discountInMinor":120,"type":"sale"}')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO lottery_purchase_clean (timestampunix, site, customernumber, currency, amountincents, feeamountincents, game, orderidentifier, paymentamountincents, ticketid, betindex, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            lottery_content)
            conn.commit()
        etl_to_production.prepare_discount_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM discount;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 3)
        
    
    def test_prepare_login_table(self):
        """Extact information from logins_vw and prepare production table."""
        login_content = [('2018-05-04 16:51:37.3758518', 'websiteF', '565560'),
                        ('2020-01-29 09:17:53.3237453', 'websiteZ', '566115'),
                        ('2019-02-16 02:20:07.5570161', 'websiteX', '564512'),
                        ('2019-04-11 18:36:59.4178461', 'websiteZ', '565588')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO customer_logins_clean ("timestamp", site, customernumber) VALUES (?, ?, ?);',
                            login_content)
            conn.commit()
        etl_to_production.prepare_login_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM login;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 4)
        
    
    def test_prepare_registration_table(self):
        """Extact information from registration_vw and prepare production table."""
        registration_content = [('2018-01-01 00:03:00.3136468','websiteF','riio@uqkry-.org','1988-12-13 16:57:40.7219400','Justin Dorsey','Rickey Villa','960 Nobel Boulevard','40 Fabien Boulevard','Michigan','76727','Arkansas','63 Second St.','2018-01-01 00:03:00.3136468','477952'),
                                ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net','1989-12-23 16:57:40.7219400', 'Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO customer_registration_clean ("timestamp", site, customeremail, dateofbirth, familyname, givennames, primaryaddress_addressline, primaryaddress_city, primaryaddress_federalstate, primaryaddress_postalcode, primaryaddress_sovereignstate, primaryaddress_street, registrationdate, customernumber) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            registration_content)
            conn.commit()
        etl_to_production.prepare_registration_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM registration;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 2)
            
    def test_prepare_customer_table(self):
        """Extact information from customer_vw and prepare production table."""
        registration_content = [('2018-01-01 00:03:00.3136468','websiteF','riio@uqkry-.org','1988-12-13 16:57:40.7219400','Justin Dorsey','Rickey Villa','960 Nobel Boulevard','40 Fabien Boulevard','Michigan','76727','Arkansas','63 Second St.','2018-01-01 00:03:00.3136468','477952'),
                                ('2018-01-01 00:05:58.4257365','websiteE','snnd.kyjs@nzcly.lfebpe.net','1989-12-23 16:57:40.7219400', 'Lesley Carroll','Heidi Bray','353 White Clarendon Road','323 North First Way','Missouri','76737','Tennessee','46 Old Blvd.','2018-01-01 00:05:58.4257365','97135')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO customer_registration_clean ("timestamp", site, customeremail, dateofbirth, familyname, givennames, primaryaddress_addressline, primaryaddress_city, primaryaddress_federalstate, primaryaddress_postalcode, primaryaddress_sovereignstate, primaryaddress_street, registrationdate, customernumber) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            registration_content)
            conn.commit()
        etl_to_production.prepare_customer_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM customer;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 2)
    
    def test_prepare_ticket_table(self):
        """Extact information from ticket_vw and prepare production table."""
        games_content = [('2018-11-13 06:30:52.1998217','websiteH','667124','eur','0716','gslsngoldenesieben','',2.0,0.4,'GOV7',''),	
                         ('2019-01-17 00:55:44.1996456','websiteZ','666294','eur','0148','Nikolos','',3.0,0.8,'HUC49302','')]
        lottery_content = [('1558931809','websiteA','132197','eur',600.0,60.0,'LOTTO','ZAH1258','','34771','12','{"discountInMinor":120,"type":"marketing"}'),
                           ('1544542681','websiteE','131323','eur',750.0,-75.0,'CASH_4_LIFE','HAC4670','','93454','3','{"discountInMinor":120,"type":"marketing"}')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO games_purchase_clean ("timestamp", sitetid, customernumber, currency, aggregationkey, gamename, highfrequencygame, priceineur, feeineur, ticketexternalid, winningsineur) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            games_content)
            # conn.commit()
            cur.executemany('INSERT INTO lottery_purchase_clean (timestampunix, site, customernumber, currency, amountincents, feeamountincents, game, orderidentifier, paymentamountincents, ticketid, betindex, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            lottery_content)
            conn.commit()
        etl_to_production.prepare_ticket_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM ticket;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 4)
            
    def test_prepare_booking_table(self):
        """Extact information from booking_vw and prepare production table."""
        games_content = [('2018-11-13 06:30:52.1998217','websiteH','667124','eur','0716','gslsngoldenesieben','',2.0,0.4,'GOV7',''),	
                         ('2019-01-17 00:55:44.1996456','websiteZ','666294','eur','0148','Nikolos','',3.0,0.8,'HUC49302','')]
        lottery_content = [('1558931809','websiteA','132197','eur',600.0,60.0,'LOTTO','ZAH1258','','34771','12','{"discountInMinor":120,"type":"marketing"}'),
                           ('1544542681','websiteE','131323','eur',750.0,-75.0,'CASH_4_LIFE','HAC4670','','93454','3','{"discountInMinor":120,"type":"marketing"}')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO games_purchase_clean ("timestamp", sitetid, customernumber, currency, aggregationkey, gamename, highfrequencygame, priceineur, feeineur, ticketexternalid, winningsineur) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            games_content)
            # conn.commit()
            cur.executemany('INSERT INTO lottery_purchase_clean (timestampunix, site, customernumber, currency, amountincents, feeamountincents, game, orderidentifier, paymentamountincents, ticketid, betindex, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            lottery_content)
            conn.commit()
        etl_to_production.prepare_booking_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM booking;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 4)
    
    def test_prepare_product_table(self):
        """Extact information from product_vw and prepare production table."""
        games_content = [('2018-11-13 06:30:52.1998217','websiteH','667124','eur','0716','gslsngoldenesieben','',2.0,0.4,'GOV7',''),	
                         ('2019-01-17 00:55:44.1996456','websiteZ','666294','eur','0148','Nikolos','',3.0,0.8,'HUC49302','')]
        lottery_content = [('1558931809','websiteA','132197','eur',600.0,60.0,'LOTTO','ZAH1258','','34771','12','{"discountInMinor":120,"type":"marketing"}'),
                           ('1544542681','websiteE','131323','eur',750.0,-75.0,'CASH_4_LIFE','HAC4670','','93454','3','{"discountInMinor":120,"type":"marketing"}')]
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.executemany('INSERT INTO games_purchase_clean ("timestamp", sitetid, customernumber, currency, aggregationkey, gamename, highfrequencygame, priceineur, feeineur, ticketexternalid, winningsineur) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            games_content)
            # conn.commit()
            cur.executemany('INSERT INTO lottery_purchase_clean (timestampunix, site, customernumber, currency, amountincents, feeamountincents, game, orderidentifier, paymentamountincents, ticketid, betindex, discount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                            lottery_content)
            conn.commit()
        etl_to_production.prepare_product_table()
        with sqlite3.connect(db_production_file) as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM product;")
            rows = cur.fetchall()
            self.assertEqual(rows[0][0], 4)
   
    
if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbose=3)
    unittest.main(testRunner=runner)