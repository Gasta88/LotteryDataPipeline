#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Main script for the case study."""

from src import setup_logging
from src import init_dbs
from src import qa_staging 
from src import etl_to_production
from src import generate_reports

def main():
    logger.info('Begin ETL process.')
    init_dbs.init_staging_database()
    init_dbs.init_prod_database()
    file_paths = init_dbs.validate_folder_and_files('data/input')
    file_path_dict = init_dbs.validate_file_header(file_paths)
    
    logger.info('Load content into *_stg tables begin.')
    for table_name, files in file_path_dict.items():
        for file_path in files:
            init_dbs.load_file_in_staging(file_path,
                                          table_name)   
    
    logger.info('Initialize data QA and transfer to *_clean tables.')
    qa_staging.update_audit_table()
    
    qa_staging.run_qa_logins() 
    qa_staging.run_qa_registration()
    qa_staging.run_qa_games()
    qa_staging.run_qa_lottery() 
      
    logger.info('Initialize data transfer to production.')
    etl_to_production.prepare_customer_table()
    etl_to_production.prepare_discount_table()
    etl_to_production.prepare_login_table()
    etl_to_production.prepare_registration_table()
    etl_to_production.prepare_booking_table()
    etl_to_production.prepare_ticket_table()
    etl_to_production.prepare_product_table()
    
    logger.info('Concluded ETL process. Generating quaterly reports.')
    
    generate_reports.generate_billing(by='type')
    generate_reports.generate_billing(by='name')
    generate_reports.generate_billing(by='website')
    generate_reports.generate_billing(by='booking_month')
    
    generate_reports.generate_active_customers(by='login_month')
    generate_reports.generate_active_customers(by='login_day')
    generate_reports.generate_active_customers(by='login_week')
    generate_reports.generate_active_customers(by='website')
    
    generate_reports.generate_avg_checkout()
   
    logger.info('Concluded generation of reports.')         
    
if __name__ == "__main__":
    logger = setup_logging(logFile='etl-info.log', level='info')
    main()