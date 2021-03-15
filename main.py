#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main script for the case study."""

from src import setup_logging
from src import init_dbs
from src import qa_staging 
from src import etl_to_production
from settings import db_staging_file, db_production_file, schema_staging_file,\
    schema_production_file, headers_dict


def main():
    logger.info('Begin ETL process.')
    init_dbs.init_database(db_staging_file, schema_staging_file)
    init_dbs.init_database(db_production_file, schema_production_file)
    file_paths = init_dbs.validate_folder_and_files('data/input')
    file_path_dict = init_dbs.validate_file_header(file_paths, headers_dict)
    
    logger.info('Load content into *_stg tables begin.')
    for table_name, files in file_path_dict.items():
        for file_path in files:
            init_dbs.load_file_in_staging(file_path,
                                          table_name,
                                          db_staging_file)   
    
    logger.info('Initialize data QA and transfer to *_clean tables.')
    qa_staging.update_audit_table(db_staging_file)
    
    qa_staging.run_qa_logins(db_staging_file) 
    qa_staging.run_qa_registration(db_staging_file)
    qa_staging.run_qa_games(db_staging_file)
    qa_staging.run_qa_lottery(db_staging_file) 
      
    logger.info('Initialize data transfer to production.')
    etl_to_production.prepare_customer_table(db_staging_file, db_production_file)
    etl_to_production.prepare_discount_table(db_staging_file, db_production_file)
    etl_to_production.prepare_login_table(db_staging_file, db_production_file)
    etl_to_production.prepare_registration_table(db_staging_file, db_production_file)
    etl_to_production.prepare_booking_table(db_staging_file, db_production_file)
    etl_to_production.prepare_ticket_table(db_staging_file, db_production_file)
    etl_to_production.prepare_product_table(db_staging_file, db_production_file)
    
    logger.info('Concluded ETL process.')
            
    
if __name__ == "__main__":
    logger = setup_logging(logFile='etl-info.log', level='info')
    main()