"""General settings fo the case study"""

import os

TEST = os.environ.get('TEST', False)

if TEST:
    print("Using TEST settings.")
    db_staging_file = os.path.join('db', 'test_staging.db')
    db_production_file = os.path.join('db', 'test_production.db')
    report_folder = 'tests/reports'
else:
    db_staging_file = os.path.join('db', 'staging.db')
    db_production_file = os.path.join('db', 'production.db')
    report_folder = 'reports'

schema_staging_file = os.path.join('data', 'db', 'staging_area.sql')
schema_production_file = os.path.join('data', 'db', 'production_area.sql')

inputfiles_headers = {
    'customerlogins': 'timestamp;site;customernumber',
    'customerregistration': 'timestamp;site;customeremail;dateofbirth;familyname;givennames;primaryaddress_addressline;primaryaddress_city;primaryaddress_federalstate;primaryaddress_postalcode;primaryaddress_sovereignstate;primaryaddress_street;registrationdate;customernumber',
    'instantgamespurchase': 'timestamp;sitetid;customernumber;currency;aggregationkey;gamename;highfrequencygame;priceineur;feeineur;ticketexternalid;winningsineur',
    'lotterygamespurchase': 'timestampunix;site;customernumber;currency;amountincents;feeamountincents;game;orderidentifier;paymentamountincents;ticketid;betindex;discount'
    }

