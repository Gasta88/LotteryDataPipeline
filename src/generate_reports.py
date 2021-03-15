#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to generate quaterly reports in CSV format."""

import logging
import sqlite3
import pandas as pd
import os

logger = logging.getLogger('file_logger')
db_file = os.path.join('..', 'db', 'production.db')

def generate_billing(by=None):
    """Write billing report by a certain parameter."""
    if by is not None:
        df = pd.DataFrame([])
        with sqlite3.connect(db_file) as conn:
            query = """SELECT \"{}\",
                              booking_year ||\'-Q\'||booking_quarter AS quarter,
                              SUM(total_price) AS total_price
                    FROM billing_vw 
                    GROUP BY \"{}\", booking_year ||\'-Q\'||booking_quarter;""".format(by,by)
                    
            dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
        for chunck_df in dfs:
            df = pd.concat([df, chunck_df], ignore_index=True)
        file_name = '../reports/billing_by_{}.csv'.format(by)
        df_pivot = df.pivot(index=by, columns='quarter', values='total_price')
        df_pivot.fillna(0).to_csv(file_name)
        del df
        logger.debug('Billing by {} complete.'.format(by))
    else:
        logger.debug('Missing parameter. No report.')
    return

def generate_active_customers(by=None):
    """Write active customers report by a certain parameter."""
    if by is not None:
        df = pd.DataFrame([])
        with sqlite3.connect(db_file) as conn:
            query = """SELECT \"{}\",
                              login_year ||\'-Q\'||login_quarter AS quarter,
                              SUM(customer_id) AS total_customers
                    FROM active_customers_vw 
                    GROUP BY \"{}\", login_year ||\'-Q\'||login_quarter;""".format(by,by)
                    
            dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
        for chunck_df in dfs:
            df = pd.concat([df, chunck_df], ignore_index=True)
        file_name = '../reports/active_customers_by_{}.csv'.format(by)
        df_pivot = df.pivot(index=by, columns='quarter', values='total_customers')
        df_pivot.fillna(0).to_csv(file_name)
        del df
        logger.debug('Active customers by {} complete.'.format(by))
    else:
        logger.debug('Missing parameter. No report.')
    return

def generate_avg_checkout():
    """Write average checkout report by yearly quarter."""
    df = pd.DataFrame([])
    with sqlite3.connect(db_file) as conn:
        query = """SELECT customer_id,
                          booking_year ||\'-Q\'||booking_quarter AS quarter,
                          AVG(total_price) AS total_price
                FROM checkout_vw 
                GROUP BY customer_id, booking_year ||\'-Q\'||booking_quarter;"""
                
        dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
    for chunck_df in dfs:
        df = pd.concat([df, chunck_df], ignore_index=True)
    file_name = '../reports/avg_customer_basket.csv'
    df_pivot = df.pivot(index='customer_id', columns='quarter', values='total_price')
    df_pivot.fillna(0).to_csv(file_name)
    del df
    logger.info('Average customer basket complete.')
    return

# def generate_diff_checkout(db_file):
#     """Write previous month difference checkout report by yearly quarter."""
#     df = pd.DataFrame([])
#     with sqlite3.connect(db_file) as conn:
#         query = """SELECT customer_id,
#                           booking_year ||\'-Q\'||booking_quarter AS quarter,
#                           AVG(total_price) AS total_price
#                 FROM checkout_vw 
#                 GROUP BY customer_id, booking_year ||\'-Q\'||booking_quarter;"""
                
#         dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
#     for chunck_df in dfs:
#         df = pd.concat([df, chunck_df], ignore_index=True)
#     df = df.fillna(0)
#     file_name = '../reports/avg_customer_basket.csv'
#     df.pivot(index='customer_id', columns='quarter', values='total_price').to_csv(file_name)
#     del df
#     logger.debug('Average customer basket complete.')
#     return
