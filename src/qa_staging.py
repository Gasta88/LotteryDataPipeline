#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to move records in quarantine/clean staging tables."""

import logging
import sqlite3
import pandas as pd
import numpy as np

logger = logging.getLogger('file_logger')

def update_audit_table(db_file):
    """ Update audit_events table."""
    with sqlite3.connect(db_file) as conn:
        sql = "UPDATE audit_events SET last_event = DATETIME('now')"
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

def run_qa_logins(db_file):
    """Run QA checks on logins records.

    Bad records are sent to customer_logins_qrtn for manual inspection.
    Good records are transformed and sent to customer_logins_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on customer_logins begin.')
    with sqlite3.connet(db_file) as conn:
        query = "SELECT * FROM customer_logins_stg;"
        qrt_df = pd.read_sql_query(query, con=conn)
        
        tmp_row_idx = qrt_df.isna().any(axis=1)
        logger.debug(
            '{} rows with missing values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        tmp_row_idx = qrt_df['site'].str.contains('\\d', regex=True)
        logger.debug(
            '{} rows with digits in website.'.format(tmp_row_idx.shape[0]))
        clean_df.append(qrt_df[~tmp_row_idx ])
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        tmp_row_idx = qrt_df['site'].str.contains('\\w', regex=True)
        logger.debug(
            '{} rows with literals in customer id.'.format(
                tmp_row_idx.shape[0]))
        clean_df.append(qrt_df[~tmp_row_idx ])
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        qrt_df.to_sql('customer_logins_qrtn', conn, if_exists='replace',
                      index=False)
        del qrt_df
        cur = conn.cursor()
        for i, row in clean_df.itterows():
            sql = """INSERT OR IGNORE INTO customer_logins_clean
                  (timestamp, site, customer_id ) VALUES (?, ?, ?)"""
            cur.execute(sql, tuple(row))
        del clean_df
        del tmp_row_idx
        conn.commit()
    logger.info('Running QA checks on customer_logins complete.')

def run_qa_registration(db_file):
    """Run QA checks on registrations records.

    Bad records are sent to customer_registrations_qrtn for manual inspection.
    Good records are transformed and sent to customer_registrations_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on customer_registrations begin.')
    with sqlite3.connet(db_file) as conn:
        query = "SELECT * FROM customer_registration_stg;"
        qrt_df = pd.read_sql_query(query, con=conn)
        
        not_null_cols = ['timestamp', 'site', 'customeremail', 'familyname',
                         'givennames', 'customernumber']
        for i, row in qrt_df.itterows():
            missing_values = [np.isnan(row[c]) for c in not_null_cols]
            if sum(missing_values) > 0:
                tmp_row_idx.append(False)    
        logger.debug(
            '{} rows with missing values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        tmp_row_idx = qrt_df.duplicated(subset=['customernumber'],keep=False)
        logger.debug(
            '{} rows with duplicate customer id.'.format(
                tmp_row_idx.shape[0]))
        clean_df.append(qrt_df[~tmp_row_idx ])
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        tmp_row_idx = qrt_df.duplicated(subset=['customeremail',
                                                'customernumber'],keep=False)
        logger.debug(
            '{} rows with duplicate email and customer id.'.format(
                tmp_row_idx.shape[0]))
        clean_df.append(qrt_df[~tmp_row_idx ])
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
       # perform dump into customer_registration_clean
        
        qrt_df.to_sql('customer_registration_qrtn', conn, if_exists='replace',
                      index=False)
        del qrt_df
        cur = conn.cursor()
        for i, row in clean_df.itterows():
            sql = """INSERT OR IGNORE INTO customer_registration_clean
                  VALUES (?, ?, ?)"""
            cur.execute(sql, tuple(row))
        del clean_df
        del tmp_row_idx
        conn.commit()
    logger.info('Running QA checks on customer_registrations complete.')

def run_qa_games(db_file):
    """Run QA checks on instant game purchase records.

    Bad records are sent to games_purchase_qrtn for manual inspection.
    Good records are transformed and sent to games_purchase_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on games_purchase begin.')
    with sqlite3.connet(db_file) as conn:
        query = "SELECT * FROM games_purchase_stg;"
        qrt_df = pd.read_sql_query(query, con=conn)
        
        not_null_cols = ['timestamp', 'sitetid', 'customernumber', 'gamename',
                         'priceineur', 'feeineur', 'ticketexternalid']
        for i, row in qrt_df.itterows():
            missing_values = [np.isnan(row[c]) for c in not_null_cols]
            if sum(missing_values) > 0:
                tmp_row_idx.append(False)    
        logger.debug(
            '{} rows with missing values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        not_neg_cols = ['priceineur', 'feeineur', 'winningsineur']
        for i, row in qrt_df.itterows():
            neg_values = [row[c] < 0 for c in not_neg_cols]
            if sum(neg_values) > 0:
                tmp_row_idx.append(False)    
        logger.debug(
            '{} rows with negative values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        # combo customer_id + ticketexternalid + aggregationkey must be unique. Discard otherwise
        # perform dump into games_purchase_clean
        
        qrt_df.to_sql('games_purchase_qrtn', conn, if_exists='replace',
                      index=False)
        del qrt_df
        cur = conn.cursor()
        for i, row in clean_df.itterows():
            sql = """INSERT OR IGNORE INTO games_purchase_clean
                  VALUES (?, ?, ?)"""
            cur.execute(sql, tuple(row))
        del clean_df
        del tmp_row_idx
        conn.commit()
    logger.info('Running QA checks on games_purchase complete.')
    
def run_qa_lottery(db_file):
    """Run QA checks on instant lottery purchase records.

    Bad records are sent to lottery_purchase_qrtn for manual inspection.
    Good records are transformed and sent to lottery_purchase_clean.
    """
    qrt_df = pd.DataFrame([])
    clean_df = pd.DataFrame([])
    tmp_row_idx = pd.Series([])
    logger.info('Running QA checks on lottery_purchase begin.')
    with sqlite3.connet(db_file) as conn:
        query = "SELECT * FROM lottery_purchase_stg;"
        qrt_df = pd.read_sql_query(query, con=conn)
        
        not_null_cols = ['timestamp', 'site', 'customernumber', 'amountineur',
                         'feeamountineur', 'game', 'orderidentifier',
                         'paymentamountineur', 'ticketid']
        for i, row in qrt_df.itterows():
            missing_values = [np.isnan(row[c]) for c in not_null_cols]
            if sum(missing_values) > 0:
                tmp_row_idx.append(False)    
        logger.debug(
            '{} rows with missing values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
        not_neg_cols = ['amountineur', 'feeamountineur', 'paymentamountineur']
        for i, row in qrt_df.itterows():
            neg_values = [row[c] < 0 for c in not_neg_cols]
            if sum(neg_values) > 0:
                tmp_row_idx.append(False)    
        logger.debug(
            '{} rows with negative values.'.format(tmp_row_idx.shape[0]))
        clean_df = qrt_df[~tmp_row_idx ]
        qrt_df.drop(~tmp_row_idx, inplace=True)
        
       # perform dump into lottery_purchase_clean
        
        qrt_df.to_sql('lottery_purchase_qrtn', conn, if_exists='replace',
                      index=False)
        del qrt_df
        cur = conn.cursor()
        for i, row in clean_df.itterows():
            sql = """INSERT OR IGNORE INTO lottery_purchase_clean
                  VALUES (?, ?, ?)"""
            cur.execute(sql, tuple(row))
        del clean_df
        del tmp_row_idx
        conn.commit()
    logger.info('Running QA checks on lottery_purchase complete.')
