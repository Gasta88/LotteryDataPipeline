# Lotto24 Study Case - Francesco Gastaldello


## Design

The project is developed using SQLite database and python scripts to perform the different stages of the ETL process.

The choice is based on personal knowledge and time constrains.

### Project layout

### Staging area

### Production area

### Data workflow


--------------------
## Assumptions

### Staging area:
 - For the following tables, these columns have been considered important (constrain NOT NULL) when cleaned up:
  - customer_logins: entire column set
  - customer_registration: timestap, site, customeremail, dateofbirth, familyname, givennames, customernumber
  - games_purchase: timestap, siteid, customernumber, gamename, priceineur, feeineur, ticketexternalid
  - lottery_purchase: timestamp, site, customernumber, amountineur,feeamountineur, game, orderidentifier, tickertid

 - Inside customer_registration table, column timestamp and registrationdate might be rendundant.
 - Cleaned pricing data in the staging are is normalized to EUR across the tables
 - Unsure about creating index on *audittime* columns due to frequent DML statements. On the other hand, it will speed up the look-up of newly created/updated records to move into Production.
 
 
 ### Production area:
 
--------------------
## How to run it

### Software requirements

 