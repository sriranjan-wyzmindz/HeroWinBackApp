
# HeroCorp Winback Data Migrator

This project is a .net console application to pull/sync data from HeroWinbackTable Remote MS SQL Server to Mysql server.




## Technical Description

Download the latest code and generate the bin folder and run the exe file under the bin folder

We need to run the console application on the server which host MySql server and to which data from remote MS Sql Server has to be synced.


App.config file contains the following keys which can be set as per need before executing the application

### ConnectionStrings

* mysqlConnection -- my sql server connectionString 

* remoteMSSqlConnection - ms sql server connectionString

### App keys


* BatchLimitPerInsert - Batch limit to insert data .( Huget data will be inserted in chunk)

* MySqlTableName - Table to which data will be inserted

* remoteMSSqlTableName - Table to which data will be pulled from


## Authors

- RaghavendraMB

