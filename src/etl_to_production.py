#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to move records from staging to production tables."""

import logging
import sqlite3
import pandas as pd

logger = logging.getLogger('file_logger')

def prepare_discount_table(db_staging, db_prod):
    """Extact information from discount_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM discount;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM discount_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        category_series = pd.Series([])
        value_series = pd.Series([])
        type_series = pd.Series([])
        for i, row in staging_df.iterrows():
            category_series[i] = row[1].replace('{','').replace('}','').replace('"','').replace(',type','').split(':')[0]
            value_series[i] = float(row[1].replace('{','').replace('}','').replace('"','').replace(',type','').split(':')[1])
            type_series[i] = row[1].replace('{','').replace('}','').replace('"','').replace(',type','').split(':')[2]
        staging_df.drop(['discount'], axis=1, inplace=True)
        staging_df.insert(1, 'category', category_series)
        staging_df.insert(2, 'value', value_series)
        staging_df.insert(3, 'type', type_series)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('discount', conn_prod, if_exists='append',
                              index=False)
        logger.info('Discount table updated.')
        return
    
    

def prepare_login_table(db_staging, db_prod):
    """Extact information from logins_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM login;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM login_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('login', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Login table updated.')
        return

def prepare_registration_table(db_staging, db_prod):
    """Extact information from registration_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM registration;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM registration_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('registration', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Registration table updated.')
        return

def prepare_customer_table(db_staging, db_prod):
    """Extact information from customer_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM customer;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM customer_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('customer', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Customer table updated.')
        return

def prepare_ticket_table(db_staging, db_prod):
    """Extact information from ticket_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM ticket;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM ticket_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('ticket', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Ticket table updated.')
        return

def prepare_booking_table(db_staging, db_prod):
    """Extact information from booking_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM booking;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM booking_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('booking', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Booking table updated.')
        return

def prepare_product_table(db_staging, db_prod):
    """Extact information from product_vw and prepare production table."""
    prod_df = pd.DataFrame([])
    staging_df = pd.DataFrame([])
    with sqlite3.connect(db_prod) as conn_prod, sqlite3.connect(db_staging) as conn_stg:
        query = "SELECT * FROM product;"
        prod_df = pd.read_sql_query(query, con=conn_prod)
        query = "SELECT * FROM product_vw;"
        staging_df = pd.read_sql_query(query, con=conn_stg)
        prod_df = pd.concat([prod_df, staging_df]).drop_duplicates(keep=False)
        if prod_df.shape[0] > 0:
            prod_df.drop(labels='audittime', axis=1, inplace=True)
            prod_df.to_sql('product', conn_prod, if_exists='append',
                              index=False, chunksize=200000)
        logger.info('Product table updated.')
        return

