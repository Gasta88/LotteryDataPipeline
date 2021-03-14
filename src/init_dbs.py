#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to initialize the databases."""

import logging
import sqlite3
import os
import sys
import pandas as pd

logger = logging.getLogger('file_logger')


def init_database(db_file, schema_file):
    """Initialize database from file."""
    if not os.path.exists(db_file):
        logger.debug('Initialize database with {}'.format(db_file))
        with sqlite3.connect(db_file) as conn:
            with open(schema_file, 'r') as schema:
                conn.executescript(schema.read())
    else:
        logger.debug('Cleaning *_stg and *_qrtn tables')
        with sqlite3.connect(db_file) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM customer_logins_stg;")
            cur.execute("DELETE FROM customer_registration_stg;")
            cur.execute("DELETE FROM games_purchase_stg;")
            cur.execute("DELETE FROM lottery_purchase_stg;")
            cur.execute("DELETE FROM customer_logins_qrtn;")
            cur.execute("DELETE FROM customer_registration_qrtn;")
            cur.execute("DELETE FROM games_purchase_qrtn;")
            cur.execute("DELETE FROM lottery_purchase_qrtn;")
            cur.execute("DELETE FROM audit_events;")
            conn.commit()
    return
                

def validate_folder_and_files(folder):
    """Return valid files from import folder."""
    if os.path.exists(folder):
        if [f for f in os.listdir(folder) if not f.startswith('.')] != []:
            files = [os.path.join(folder, f) for f in os.listdir(folder)]
            logger.info('Found {} files to import.'.format(len(files)))
            excluded_files = [f for f in files if not f.endswith('csv')]
            if len(excluded_files) > 0:
                logger.debug(
                    '{} files not suitable for import: {}'.format(
                        len(excluded_files), "\n".join(excluded_files)))
            return files
        else:
            logger.debug('No files to import. ETL stopped.')
            sys.exit('No files to import.')
    else:
        logger.debug('{} does not exist. ETL stopped.'.format(folder))
        sys.exit('Import folder does not exist.')

def validate_file_header(files, headers):
    """Validate first line of the file."""
    for file_path in files:
        for k, v in headers.items():
            if k in file_path:
                with open(file_path, 'r', encoding='latin-1') as f:
                    first_line = f.readline()
                    if not (first_line.rstrip() == v):
                        file_name = file_path.split(os.sep)[-1]
                        logger.debug(
                            '{} does not have correct header.'.format(
                                file_name))
                        files.remove(file_path)
    logger.info('{} files with valid headers.'.format(len(files)))
    files_to_tables = {
        'customer_logins_stg': [],
        'customer_registration_stg': [],
        'games_purchase_stg': [],
        'lottery_purchase_stg': []
            }
    for file_path in files:
        if 'customerlogin' in file_path:
            files_to_tables['customer_logins_stg'].append(file_path)
        if 'customerregistration' in file_path:
            files_to_tables['customer_registration_stg'].append(file_path)
        if 'instantgame' in file_path:
            files_to_tables['games_purchase_stg'].append(file_path)
        if 'lotterygame' in file_path:
            files_to_tables['lottery_purchase_stg'].append(file_path)
    return files_to_tables


def load_file_in_staging(file_path, table_name, db_file):
    """Load content of file inside *_stg tables."""
    tmp_df = pd.read_table(file_path, header=0, sep=';', dtype='str',
                            encoding='latin-1')
    logger.info('Uploading {} into {}.'.format(file_path, table_name))
    with sqlite3.connect(db_file) as conn:
         tmp_df.to_sql(table_name, conn, index=False, if_exists='append',
                       chunksize=200000)
         logger.info('Imported {} rows.'.format(tmp_df.shape[0]))
         del tmp_df
    return

