#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to move records in quarantine/clean staging tables."""

import logging
import sqlite3
import pandas as pd
import numpy as np
import dask.dataframe as dd
from .settings import db_staging_file

logger = logging.getLogger('file_logger')

def update_audit_table():
    """ Update audit_events table."""
    with sqlite3.connect(db_staging_file) as conn:
        sql = "INSERT INTO audit_events (last_event) VALUES (current_timestamp);"
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    return

def run_qa_logins(debug=0):
    """Run QA checks on logins records.

    Bad records are sent to customer_logins_qrtn for manual inspection.
    Good records are transformed and sent to customer_logins_clean.
    """
    qrt_df = dd.DataFrame([])
    clean_df = dd.DataFrame([])
    tmp_row_idx = pd.Series([])
    n_rows_qrt = 0
    n_rows_clean = 0
    n_rows_missing = 0
    n_rows_digits = 0
    n_rows_literals = 0
    logger.info('Running QA checks on customer_logins begin.')
    conn = f'sqlite:///{db_staging_file}'
    query = "SELECT * FROM customer_logins_stg;"
    chuncks = dd.read_sql_query(query, con=conn, chunksize=20000)
    for clean_df in chuncks:
        clean_df.fillna(value=np.nan, inplace=True)
        
        # no null values allowed
        tmp_row_idx = clean_df.isna().any(axis=1)
        if tmp_row_idx.sum() > 0:
            n_rows_missing += tmp_row_idx.sum()
            qrt_df = clean_df[tmp_row_idx ]
            clean_df = clean_df[~tmp_row_idx]
            
        # no digits on website   
        tmp_row_idx = clean_df['site'].str.isnumeric()
        if tmp_row_idx.sum() > 0:
            n_rows_digits += tmp_row_idx.sum()
            qrt_df.append(clean_df[tmp_row_idx ])
            clean_df = clean_df[~tmp_row_idx]
            
        # no literal on customernumber    
        tmp_row_idx = clean_df['customernumber'].str.isalpha()
        if tmp_row_idx.sum() > 0:
            n_rows_literals += tmp_row_idx.shape[0]
            qrt_df.append(clean_df[tmp_row_idx ])
            clean_df = clean_df[~tmp_row_idx]

        qrt_df.to_sql('customer_logins_qrtn', conn, index=False, if_exists='append')
        
        query = "SELECT * FROM customer_logins_clean;"
        old_clean_df = pd.read_sql_query(query, con=conn)
        clean_df = pd.concat([clean_df, old_clean_df]).drop_duplicates(
            keep=False)
        if clean_df.shape[0] > 0:
            clean_df['audittime'] = pd.to_datetime('now')
            clean_df.to_sql('customer_logins_clean', conn, index=False, if_exists='append')
            n_rows_qrt += qrt_df.shape[0]
            n_rows_clean += clean_df.shape[0]
            del qrt_df
            del clean_df
            del tmp_row_idx
    logger.info(f'{n_rows_missing} rows with missing values.')
    logger.info(f'{n_rows_digits} rows with digits in website.')
    logger.info(f'{n_rows_literals} rows with literals in customer id.')
    logger.info('Running QA checks on customer_logins complete.')
    if debug == 1:
        return(n_rows_qrt, n_rows_clean)
    return

# def run_qa_logins(debug=0):
#     """Run QA checks on logins records.

#     Bad records are sent to customer_logins_qrtn for manual inspection.
#     Good records are transformed and sent to customer_logins_clean.
#     """
#     qrt_df = pd.DataFrame([])
#     clean_df = pd.DataFrame([])
#     tmp_row_idx = pd.Series([])
#     n_rows_qrt = 0
#     n_rows_clean = 0
#     n_rows_missing = 0
#     n_rows_digits = 0
#     n_rows_literals = 0
#     logger.info('Running QA checks on customer_logins begin.')
#     with sqlite3.connect(db_staging_file) as conn:
#         query = "SELECT * FROM customer_logins_stg;"
#         chuncks = pd.read_sql_query(query, con=conn, chunksize=20000)
#         for clean_df in chuncks:
#             clean_df.fillna(value=np.nan, inplace=True)
            
#             # no null values allowed
#             tmp_row_idx = clean_df.isna().any(axis=1)
#             if tmp_row_idx.sum() > 0:
#                 n_rows_missing += tmp_row_idx.sum()
#                 qrt_df = clean_df[tmp_row_idx ]
#                 clean_df = clean_df[~tmp_row_idx]
                
#             # no digits on website   
#             tmp_row_idx = clean_df['site'].str.isnumeric()
#             if tmp_row_idx.sum() > 0:
#                 n_rows_digits += tmp_row_idx.sum()
#                 qrt_df.append(clean_df[tmp_row_idx ])
#                 clean_df = clean_df[~tmp_row_idx]
                
#             # no literal on customernumber    
#             tmp_row_idx = clean_df['customernumber'].str.isalpha()
#             if tmp_row_idx.sum() > 0:
#                 n_rows_literals += tmp_row_idx.shape[0]
#                 qrt_df.append(clean_df[tmp_row_idx ])
#                 clean_df = clean_df[~tmp_row_idx]
    
#             qrt_df.to_sql('customer_logins_qrtn', conn, index=False, if_exists='append')
            
#             query = "SELECT * FROM customer_logins_clean;"
#             old_clean_df = pd.read_sql_query(query, con=conn)
#             clean_df = pd.concat([clean_df, old_clean_df]).drop_duplicates(
#                 keep=False)
#             if clean_df.shape[0] > 0:
#                 clean_df['audittime'] = pd.to_datetime('now')
#                 clean_df.to_sql('customer_logins_clean', conn, index=False, if_exists='append')
#                 n_rows_qrt += qrt_df.shape[0]
#                 n_rows_clean += clean_df.shape[0]
#                 del qrt_df
#                 del clean_df
#                 del tmp_row_idx
#     logger.info(f'{n_rows_missing} rows with missing values.')
#     logger.info(f'{n_rows_digits} rows with digits in website.')
#     logger.info(f'{n_rows_literals} rows with literals in customer id.')
#     logger.info('Running QA checks on customer_logins complete.')
#     if debug == 1:
#         return(n_rows_qrt, n_rows_clean)
#     return
    

def run_qa_registration(debug=0):
    """Run QA checks on registrations records.

    Bad records are sent to customer_registrations_qrtn for manual inspection.
    Good records are transformed and sent to customer_registrations_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on customer_registrations begin.')
    with sqlite3.connect(db_staging_file) as conn:
        query = "SELECT * FROM customer_registration_stg;"
        clean_df = pd.read_sql_query(query, con=conn)
        clean_df.fillna(value=np.nan, inplace=True)
        
        # no null values in selected columns
        not_null_cols = ['timestamp', 'site', 'customeremail', 'familyname',
                         'givennames', 'customernumber', 'dateofbirth']
        for c in not_null_cols:
            tmp_row_idx = clean_df[c].isna()
            if tmp_row_idx.sum() > 0:
                logger.info(
                    '{} rows with {} missing values.'.format(tmp_row_idx.sum(),
                                                             c))
                qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
                clean_df = clean_df[~tmp_row_idx]
            
        # email must be valid
        tmp_row_idx = ~clean_df['customeremail'].str.contains('^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$',
                                                              regex=True)
        if tmp_row_idx.sum() > 0:
            logger.info(
                '{} rows with invalid emails.'.format(
                    tmp_row_idx.shape[0]))
            qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
            clean_df = clean_df[~tmp_row_idx]
            
        # no duplicate customernumber records
        tmp_row_idx = clean_df.duplicated(subset=['customernumber'],keep=False)
        if tmp_row_idx.sum() > 0:
            logger.info(
                '{} rows with duplicate customer id.'.format(
                    tmp_row_idx.shape[0]))
            qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
            clean_df = clean_df[~tmp_row_idx]
        
        # no duplicate customernumber + customeremail records
        tmp_row_idx = clean_df.duplicated(subset=['customeremail',
                                                'customernumber'],keep=False)
        if tmp_row_idx.sum() > 0:
            logger.info(
                '{} rows with duplicate email and customer id.'.format(
                    tmp_row_idx.shape[0]))
            qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
            clean_df = clean_df[~tmp_row_idx]
        
        qrt_df.to_sql('customer_registration_qrtn', conn, index=False, if_exists='append',
                       chunksize=200000)
        query = "SELECT * FROM customer_registration_clean;"
        old_clean_df = pd.read_sql_query(query, con=conn)
        clean_df = pd.concat([clean_df, old_clean_df]).drop_duplicates(
            keep=False)
        if clean_df.shape[0] > 0:
            clean_df['audittime'] = pd.to_datetime('now')
            clean_df.to_sql('customer_registration_clean', conn, index=False, if_exists='append',
                       chunksize=200000)
    logger.info('Running QA checks on customer_registrations complete.')
    n_rows_qrt = qrt_df.shape[0]
    n_rows_clean = clean_df.shape[0]
    del qrt_df
    del clean_df
    del tmp_row_idx
    if debug == 1:
        return(n_rows_qrt, n_rows_clean)
    return

def run_qa_games(debug=0):
    """Run QA checks on instant game purchase records.

    Bad records are sent to games_purchase_qrtn for manual inspection.
    Good records are transformed and sent to games_purchase_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on games_purchase begin.')
    with sqlite3.connect(db_staging_file) as conn:
        query = "SELECT * FROM games_purchase_stg;"
        clean_df = pd.read_sql_query(query, con=conn)
        clean_df.fillna(value=np.nan, inplace=True)
        
        # no null values in selected columns
        not_null_cols = ['timestamp', 'sitetid', 'customernumber', 'gamename',
                         'priceineur', 'feeineur', 'ticketexternalid']
        for c in not_null_cols:
            tmp_row_idx = clean_df[c].isna()
            if tmp_row_idx.sum() > 0:
                logger.info(
                    '{} rows with {} missing values.'.format(tmp_row_idx.sum(),
                                                             c))
                qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
                clean_df = clean_df[~tmp_row_idx]
        # no negative records allowed
        not_neg_cols = ['priceineur', 'feeineur', 'winningsineur']
        for c in not_neg_cols:
            tmp_row_idx = clean_df[c] < 0
            if tmp_row_idx.sum() > 0:
                logger.info(
                    '{} rows with negative values in {}.'.format(tmp_row_idx.sum(),
                                                                 c))
                qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
                clean_df = clean_df[~tmp_row_idx]
        
        qrt_df.to_sql('games_purchase_qrtn', conn, index=False, if_exists='append',
                       chunksize=200000)
        query = "SELECT * FROM games_purchase_clean;"
        old_clean_df = pd.read_sql_query(query, con=conn)
        clean_df = pd.concat([clean_df, old_clean_df]).drop_duplicates(
            keep=False)
        if clean_df.shape[0] > 0:
            clean_df['audittime'] = pd.to_datetime('now')
            clean_df.to_sql('games_purchase_clean', conn, index=False, if_exists='append',
                       chunksize=200000)
    logger.info('Running QA checks on games_purchase complete.')
    n_rows_qrt = qrt_df.shape[0]
    n_rows_clean = clean_df.shape[0]
    del qrt_df
    del clean_df
    del tmp_row_idx
    if debug == 1:
        return(n_rows_qrt, n_rows_clean)
    return

def run_qa_lottery(debug=0):
    """Run QA checks on instant lottery purchase records.

    Bad records are sent to lottery_purchase_qrtn for manual inspection.
    Good records are transformed and sent to lottery_purchase_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on lottery_purchase begin.')
    with sqlite3.connect(db_staging_file) as conn:
        query = "SELECT * FROM lottery_purchase_stg;"
        clean_df = pd.read_sql_query(query, con=conn)
        clean_df.fillna(value=np.nan, inplace=True)
        
        # no null values in selected columns
        not_null_cols = ['timestampunix', 'site', 'customernumber', 'amountincents',
                         'feeamountincents', 'game', 'orderidentifier', 'ticketid']
        for c in not_null_cols:
            tmp_row_idx = clean_df[c].isna()
            if tmp_row_idx.sum() > 0:
                logger.info(
                    '{} rows with {} missing values.'.format(tmp_row_idx.sum(),
                                                             c))
                qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
                clean_df = clean_df[~tmp_row_idx]
        
        # no negative records allowed
        not_neg_cols = ['amountincents', 'feeamountincents', 'paymentamountincents']
        for c in not_neg_cols:
            tmp_row_idx = clean_df[c] < 0
            if tmp_row_idx.sum() > 0:
                logger.info(
                    '{} rows with negative values in {}.'.format(tmp_row_idx.sum(),
                                                                 c))
                qrt_df = pd.concat([qrt_df, clean_df[tmp_row_idx]],
                                   ignore_index=True)
                clean_df = clean_df[~tmp_row_idx]
        
        qrt_df.to_sql('lottery_purchase_qrtn', conn, index=False, if_exists='append',
                       chunksize=200000)
        query = "SELECT * FROM lottery_purchase_clean;"
        old_clean_df = pd.read_sql_query(query, con=conn)
        clean_df = pd.concat([clean_df, old_clean_df]).drop_duplicates(
            keep=False)
        if clean_df.shape[0] > 0:
            clean_df['audittime'] = pd.to_datetime('now')
            clean_df.to_sql('lottery_purchase_clean', conn, index=False, if_exists='append',
                       chunksize=200000)
    logger.info('Running QA checks on lottery_purchase complete.')
    n_rows_qrt = qrt_df.shape[0]
    n_rows_clean = clean_df.shape[0]
    del qrt_df
    del clean_df
    del tmp_row_idx
    if debug == 1:
        return(n_rows_qrt, n_rows_clean)
    return