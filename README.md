# Lotto24 Study Case - Francesco Gastaldello


## Design

The project is developed using SQLite database and python scripts to perform the different stages of the ETL process.

The choice is based on personal knowledge and time constrains. Alternatives can be unlimited.

### Project layout

Two SQLite databases to store *staging* and *production*. ETL performed via Python scripts supported with Pandas.
 Data visualization via Jupyter inside a Docker container.


### Staging area

The *staging area* in divided in sub-parts:
- tables with "stg" appendix: direct upload from source material. Volatile tables without QA checks performed.
    
- tables with "qrtn" appendix: quarantine tables to hold those records that fail QA checks. Require manual inspections to be moved forward or removed completely.

- tables with "clean" appendix: tables with records that pass the QA checks with some light data convertions (ex: currency and timestamps normalization)

Presence of triggers only on the clean tables to guarntee the update of the *audittime* column whenever a DELETE or UPDATE is performed as a row-event.

Indexes implemented at the *audittime* columns in the clean tables to improve performance of the balanced load from *staging* to *production*.
This result in a drop in performance during INSERT inside the before mentioned tables.

### Production area

The *production area* has the following EDR: *insert pic here*

Views are used to organize the data quickly inside the database, to be later visualized inside the Jupyter notebook.

### Data workflow

*insert pic here*


--------------------
## Assumptions

### Staging area:

- For the following tables, these columns have been considered important (constrain NOT NULL) when cleaned up:
  - customer_logins: entire column set
  - customer_registration: timestap, site, customeremail, dateofbirth, familyname, givennames, customernumber
  - games_purchase: timestap, siteid, customernumber, gamename, priceineur, feeineur, ticketexternalid
  - lottery_purchase: timestamp, site, customernumber, amountineur,feeamountineur, game, orderidentifier, tickertid

- Inside customer_registration table, column timestamp and registrationdate might be rendundant.
- Inside lottery_purchase table, column amountincents and paymentamountincents might be rendundant.
- Cleaned pricing data in the staging are is normalized to EUR across the tables

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
    
- Discount: table for unqiue tipes of discounts



--------------------
## How to run it

### ETL pipeline

Follow these steps:

 1. Activate virtual environment via `source venv/bin/activate`
 2. Run script with one of these options:
   2a. `python main.py`
   2b. `python3 main.py`
   2c. Open main.py with preferred IDE and iterate through the lines of the main().

### Test cases

Follow these steps:

 1. Activate virtual environment via `source venv/bin/activate`
 2. Open main.py with preferred IDE and change line 19 'data/input' to 'data/test_input'
 3. Run script with one of these options:
   3a. `python main.py`
   3b. `python3 main.py`
   3c. Open main.py with preferred IDE and iterate through the lines of the main().

### Reporting

Please make sure that Docker is installed on the computer. Run:

`
docker run -p 8888:8888 -v $(pwd):/home/jovyan/work jupyter/minimal-notebook
`

On console it should appear at the end some instruction on how to start the Jupyter Notebook.
Access the notebook on work/reports folder.

### Software requirements

Check the requirements.txt for any details. a virtual environment is available as well.
Make sure docker is available for the reporting.

 