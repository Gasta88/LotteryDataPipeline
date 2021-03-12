#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main scriptfor the case study."""

from src import setup_logging
from src.init_dbs import init_database, validate_folder_and_files, 
from settings import db_staging_file, db_production_file, schema_staging_file,\
    schema_production_file, headers_dict
import os


def main():
    logger.info('Begin ETL process.')
    init_database(db_staging_file, schema_staging_file)
    init_database(db_production_file, schema_production_file)
    file_paths = validate_folder_and_files('data/input')
    for file_path in file_paths:
        for k, v in headers_dict:
            if k in file_path:
                if 


if __name__ == "__main__":
    logger = setup_logging(logFile='etl-info.log', level='info')
    main()