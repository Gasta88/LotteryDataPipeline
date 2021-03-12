#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Used to initialize the databases."""

import logging
import sqlite3
import os
import sys

logger = logging.getLogger('file_logger')


def init_database(db_file, schema_file):
    """Initialize database from file."""
    if not os.path.exists(db_file):
        logger.debug('Initialize database with {}'.format(db_file))
        with sqlite3.connet(db_file) as conn:
            with open(schema_file, 'r') as schema:
                conn.executescript(schema.read())

def validate_folder_and_files(folder):
    """Return valid files from import folder."""
    if not os.path.exist(folder):
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

def validate_file_header(file_path, file_header):
    """Validate first line of the file."""
    with open(file_path, 'r') as f:
        check= f.readline() == file_header
        if not check:
            file_name = file_path.split(os.sep)[-1]
            logger.debug('{} does not have correct header.'.format(file_name))
        return check
            
