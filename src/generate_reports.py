#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to generate quaterly reports in CSV format."""

import logging
import sqlite3
import pandas as pd
from .settings import db_production_file, report_folder
import os

logger = logging.getLogger('file_logger')

if not os.path.exists(report_folder):
    os.mkdir(report_folder)

def generate_billing(by=None):
    """Write billing report by a certain parameter."""
    if by is not None:
        df = pd.DataFrame([])
        with sqlite3.connect(db_production_file) as conn:
            query = """SELECT \"{}\",
                              booking_year ||\'-Q\'||booking_quarter AS quarter,
                              SUM(total_price) AS total_price
                    FROM billing_vw 
                    GROUP BY \"{}\", booking_year ||\'-Q\'||booking_quarter;""".format(by,by)
                    
            dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
        for chunck_df in dfs:
            df = pd.concat([df, chunck_df], ignore_index=True)
        file_name = os.path.join(report_folder, 'billing_by_{}.csv'.format(by))
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
        with sqlite3.connect(db_production_file) as conn:
            query = """SELECT \"{}\",
                              login_year ||\'-Q\'||login_quarter AS quarter,
                              SUM(customer_id) AS total_customers
                    FROM active_customers_vw 
                    GROUP BY \"{}\", login_year ||\'-Q\'||login_quarter;""".format(by,by)
                    
            dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
        for chunck_df in dfs:
            df = pd.concat([df, chunck_df], ignore_index=True)
        file_name = os.path.join(report_folder,
                                 'active_customers_by_{}.csv'.format(by))
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
    with sqlite3.connect(db_production_file) as conn:
        query = """SELECT customer_id,
                          booking_year ||\'-Q\'||booking_quarter AS quarter,
                          AVG(total_price) AS total_price
                FROM checkout_vw 
                GROUP BY customer_id, booking_year ||\'-Q\'||booking_quarter;"""
                
        dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
    for chunck_df in dfs:
        df = pd.concat([df, chunck_df], ignore_index=True)
    file_name = os.path.join(report_folder, 'avg_customer_basket.csv')
    df_pivot = df.pivot(index='customer_id', columns='quarter', values='total_price')
    df_pivot.fillna(0).to_csv(file_name)
    del df
    logger.info('Average customer basket complete.')
    return

def generate_monthly_diff():
    """Write monthly difference for each customer."""
    df = pd.DataFrame([])
    with sqlite3.connect(db_production_file) as conn:
        query = """SELECT t.customer_id, t.booking_year ||\'-\'||t.booking_month as year_month, t.monthly_price - LAG(t.monthly_price) 
                    OVER(ORDER BY t.booking_year ,t.booking_month) AS monthly_diff
                    FROM (SELECT customer_id , booking_year ,booking_month, sum(total_price) AS monthly_price 
                      FROM checkout_vw cv GROUP BY customer_id , booking_year ,booking_month) t ORDER BY t.booking_year ,t.booking_month ;"""
                
        dfs = pd.read_sql_query(query, con=conn, chunksize= 200000)
    for chunck_df in dfs:
        df = pd.concat([df, chunck_df], ignore_index=True)
    file_name = os.path.join(report_folder, 'monthly_diff_customer.csv')
    df_pivot = df.pivot(index='customer_id', columns='year_month', values='monthly_diff')
    df_pivot.fillna(0).to_csv(file_name)
    del df
    logger.info('Monthly customer report complete.')
    return

