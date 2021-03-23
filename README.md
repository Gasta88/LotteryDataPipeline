# Lotto24 Study Case - Francesco Gastaldello


## Design

The project is developed using SQLite database and python scripts to perform the different stages of the ETL process.

The choice is based on personal knowledge and time constrains. Alternatives can be unlimited and open for discussion during the following meeting.

### Project layout

Two SQLite databases to store inside *staging* and *production* part. ETL performed via Python scripts supported with Pandas, along with views generated on the databases.
Data visualization via Jupyter inside a Docker container.


### Staging area

The *staging area* in divided in sub-parts:

- tables with "stg" appendix: direct upload from source material. Volatile tables without QA checks performed.
    
- tables with "qrtn" appendix: quarantine tables to hold those records that fail QA checks. Require manual inspections to be moved forward or removed completely.

- tables with "clean" appendix: tables with records that pass the QA checks with some light data convertions (ex: currency, timestamps normalization,..)

Presence of triggers only on the clean tables to guarantee the update of the *audittime* column whenever an UPDATE is performed as a row-event.

Indexes implemented at the *audittime* columns in the clean tables to improve performance of the balanced load from *staging* to *production*.

This result in a drop in performance during INSERT inside the before mentioned tables.

A picture of the schema EDR is in `imgs` folder.

### Production area

A picture of the schema EDR is in `imgs` folder.

Views are used to organize the data quickly inside the database, to be later visualized inside the Jupyter notebook.

### Data workflow

A summary of the pipeline is in `imgs` folder.

The main points are the following:

- **Collection:** Data is extracted with Pandas and allocated inside the respctive tables in the *staging* database.

- **Store:**  SQLite *staging* database is modeled to guarantee QA checks and a incremental upload into the *production* database.

- **Analyze:** SQLite is been leveraged with views to present the data for analysis and presentation.

- **Consume:** Jupyter is used to visualize the data in a tabulated format. Data can be plotted as well for a graphical overview.


--------------------
## Assumptions

### Staging area:

- For the following tables, these columns have been considered important (constrain NOT NULL) when cleaned up:
  - customer_logins: entire column set
  - customer_registration: timestap, site, customeremail, dateofbirth, familyname, givennames, customernumber
  - games_purchase: timestap, siteid, customernumber, gamename, priceineur, feeineur, ticketexternalid
  - lottery_purchase: timestamp, site, customernumber, amountineur,feeamountineur, game, orderidentifier, tickertid

- Inside customer_registration table, columns timestamp and registrationdate might be rendundant.
- Inside lottery_purchase table, columns amountincents and paymentamountincents might be rendundant.
- Cleaned pricing data in the *staging* are is normalized to EUR across the tables

The QA checks are based on the following assumptions:

- customer_logins:
    - all columns are necessary
    - no numbers on the website name
    - no letters on the customer name
- customer_registrations:
    - necessary columns are: timestamp, site, customeremail, familyname, givennames, customernumber, dateofbirth
    - no duplicates allowed on: customernumber and "customernumber + email"
    - email must be valid 
- instant games puchases:
    - necessary columns are: timestamp, sitetid, customernumber, gamename, ticketexternalid
    - no negative values allowed on: priceineur, feeineur, winningsineur
- lottery ticket purchases:
    - necessary columns are: timestampunix, site, customernumber, game, orderidentifie, ticketid
    - no negative values allowed on: amountincents, feeamountincents, paymentamountincents



### Production area:

- Customer: table for customers and their details.

- Registration: tables for customer registration events.
  - Constrains:
    - if a customer is deleted from the system, the registration event remains without a customer_id associated (inactive history)
    
- Product: table for lottery games and instant games offered by the company.

- Login: table for customer log events.
  - Constrains:
    - if a customer is deleted from the system, the login event remains without a customer_id associated (inactive history)

- Ticket: table for tickets related to either lottery or instant games.
  - Constrains:
    - if a product is deleted, the ticket is invalidated (cascade event).
    - if a discount is not offered any longer, if is set to NULL in the table.
    
- Booking: table for booking events, which are considered as purchases at a certain time, from a certain website, by a certain customer of a certain product. Uniquely identified by a order_id value. 
    - Constrains:
        - if a customer is deleted from the system, the order is deleted (cascade event).
        - if a ticket is deleted from the system,the order is deleted (cascade event).
    
- Discount: table for unique types of discounts



--------------------
## How to run it

### ETL pipeline

Follow these steps:

 1. Activate virtual environment via `source venv/bin/activate`
 2. Allocate input files in `data/input` folder
 3. Run script with one of these options:
   3a. `python main.py`
   3b. `python3 main.py`
   3c. Open main.py with preferred IDE and iterate through the lines of the main().
   
Run time is approximatelly 3 hours and 20 minutes until report generation.
My station specs are:
 
 - CPU Intel i7-7700HQ @ 2.8GHz 2.81GHz
 - 16 GB RAM
 - Windows 10

### Visualize Reports

Please make sure that Docker is installed on the computer.
Extract generated reports from `reports/reports.zip`. Run on terminal :

`
docker run -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/minimal-notebook
`

On console it should appear at the end some instruction on how to start the Jupyter Notebook.
Access the notebook on `work/reports` folder.

Unfortunatelly, I was unable to generate the fourth type of report from the list.

### Software requirements

Check the requirements.txt for any details. a virtual environment is available as well.
Make sure docker is available for the visualization of the reports.


### Possible improvements

During development I came to realize that this project is far from perfect, therefore I have  a couple of points that could be seen as weak points/possible improvements:

- Manual insertion of records from *_qrtn to *_clean needs to be provided with "current_timestamp" to guaranteed the value in the audittime column. 
- Dask over Pandas: faster in reading files, but not much SQL support on the API at the moment (?). Other workflow pakages could be interesting(ex: Airflow, Luigi)
- Alternative to Jupyter for report visualization. Generation of reports is done via Python script because it incours into a limit of IO processing data volume.
- Use materialized views over simple views in *production*. These have to be manually refreshed, but they would be a faster mean to the report generation.
- Different database technology (ex: Oracle, SQL Server or PostgreSQL) which has its own "change content monitor" feature.
