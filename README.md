# Lotto24 Study Case - Francesco Gastaldello


## Design

The project is developed using SQLite database and python scripts to perform the different stages of the ETL process.

The choice is based on personal knowledge and time constrains. Alternatives can be unlimited

### Project layout

Two SQLite databases to store *staging* and *production*.

 ETL performed via Python scripts supported with Pandas.
 
 Data visualization via Jupyter inside a Docker container.


### Staging area

The *staging area* in divided in sub-parts:
    - tables with "stg" appendix: direct upload from source material. Volatile tables without QA checks performed.
    - tables with "qrtn" appendix: quarantine tables to hold those records that fail QA checks. Require manual inspections to be moved forward or removed completely.
    - tables with "clean" appendix: tables with records that pass the QA checks with some light data convertions (ex: currency and timestamps normalization)

Presence of triggers only on the clean tables to guarntee the update of the *audittime* column whenever a DELETE or UPDATE is performed as a row-event.

Indexes implemented at the *audittime* columns int he clean tables to improve performance of the balanced load from *staging* to *production*.
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
 - Unsure about creating index on *audittime* columns due to frequent DML statements. On the other hand, it will speed up the look-up of newly created/updated records to move into Production.

The QA checks are based on the following assumptions:
    - customer_logins:
      - all columns are necessary
      - no numbers on the website name
      - no letters on the customer name
    - customer_registrations:
      - necessary columns are: timestamp, site, customeremail, familyname, givennames, customernumber
      - no duplicates allowed on: customernumber and "customernumber + email"
    - instant games puchases:
      - necessary columns are: timestamp, sitetid, customernumber, gamename, ticketexternalid
      - no negative values allowed on: priceineur, feeineur, winningsineur
    - lottery ticket purchases:
      - necessary columns are: timestampunix, site, customernumber, game, orderidentifie, ticketid
      - no negative values allowed on: amountincents, feeamountincents, paymentamountincents





 
### Production area:

- Customer:
- Registration:
- Product: ?
- Ticket:
- Booking:
--------------------
## How to run it

### Software requirements

 