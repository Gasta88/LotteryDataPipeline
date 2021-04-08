#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to initialize the databases."""

import logging
import sqlite3
import os
import sys
import dask.dataframe as dd
from .settings import db_staging_file, db_production_file, schema_staging_file,\
    schema_production_file, inputfiles_headers

logger = logging.getLogger('file_logger')




def init_staging_database():
    """Initialize database from file."""
    if not os.path.exists(db_staging_file):
        logger.debug('Initialize database with {}'.format(db_staging_file))
        with sqlite3.connect(db_staging_file) as conn:
            with open(schema_staging_file, 'r') as schema:
                conn.executescript(schema.read())
    else:
        logger.debug('Cleaning *_qrtn tables')
        with sqlite3.connect(db_staging_file) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM customer_logins_qrtn;")
            cur.execute("DELETE FROM customer_registration_qrtn;")
            cur.execute("DELETE FROM games_purchase_qrtn;")
            cur.execute("DELETE FROM lottery_purchase_qrtn;")
            cur.execute("DELETE FROM audit_events;")
            conn.commit()
    return

def init_prod_database():
    """Initialize database from file."""
    if not os.path.exists(db_production_file):
        logger.debug('Initialize database with {}'.format(db_production_file))
        with sqlite3.connect(db_production_file) as conn:
            with open(schema_production_file, 'r') as schema:
                conn.executescript(schema.read())
    else:
        return
                

def validate_folder_and_files(folder):
    """Return valid files from import folder."""
    if os.path.exists(folder):
        if [f for f in os.listdir(folder) if not f.startswith('.')] != []:
            files = [os.path.join(folder, f) for f in os.listdir(folder)]
            logger.info(f'Found {len(files)} files to import.')
            excluded_files = [f for f in files if not f.endswith('csv')]
            included_files = [f for f in files if f.endswith('csv')]
            if len(excluded_files) > 0:
                logger.debug(
                    '{} files not suitable for import: {}'.format(
                        len(excluded_files), "\n".join(excluded_files)))
            return included_files
        else:
            logger.debug('No files to import. ETL stopped.')
            sys.exit('No files to import.')
    else:
        logger.debug('{} does not exist. ETL stopped.'.format(folder))
        sys.exit('Import folder does not exist.')

def validate_file_header(files):
    """Validate first line of the file."""
    good_files = []
    for file_path in files:
        for k, v in inputfiles_headers.items():
            if k in file_path:
                with open(file_path, 'r', encoding='latin-1') as f:
                    first_line = f.readline()
                    print(f'First line:{first_line}')
                    if first_line.rstrip() == v:
                        good_files.append(file_path)
                    else:
                        file_name = file_path.split(os.sep)[-1]
                        logger.debug(
                            '{} does not have correct header.'.format(
                                file_name))
                    break
    logger.info('{} files with valid headers.'.format(len(files)))
    files_to_tables = {
        'customer_logins_stg': [],
        'customer_registration_stg': [],
        'games_purchase_stg': [],
        'lottery_purchase_stg': []
            }
    for file_path in good_files:
        if 'customerlogin' in file_path:
            files_to_tables['customer_logins_stg'].append(file_path)
        if 'customerregistration' in file_path:
            files_to_tables['customer_registration_stg'].append(file_path)
        if 'instantgame' in file_path:
            files_to_tables['games_purchase_stg'].append(file_path)
        if 'lotterygame' in file_path:
            files_to_tables['lottery_purchase_stg'].append(file_path)
    return files_to_tables


def load_file_in_staging(file_path, table_name):
    """Load content of file inside *_stg tables."""
    tmp_df = dd.read_table(file_path, header=0, sep=';', dtype='str',
                            encoding='latin-1')
    logger.info('Uploading {} into {}.'.format(file_path, table_name))
    conn = f'sqlite:///{db_staging_file}'
    tmp_df.to_sql(table_name, conn, index=False, if_exists='replace',
                           chunksize=200000)
    logger.info('Imported {} rows.'.format(tmp_df.shape[0]))
    del tmp_df
    return

