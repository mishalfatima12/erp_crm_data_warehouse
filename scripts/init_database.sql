/*
------------------------------ CREATE DATABASE AND SCHEMAS ------------------------------
PURPOSE:
The script creates a new database after checking that it does not exist. If it does exist then the database is foirst dropped and then recreated.
Within the database, three schemas namely: bronze, silver and gold are also created.

WARNING:
PLEASE BE MINDFUL THAT RUNNING THE SCRIPT WILL DELETE ANY EXISTING DATABASE WITH THE NAME 'DataWarehouse'. 

*/


USE master;
GO
  
-- Drop & Recreate Database
IF DB_ID ('DataWarehouse') IS NOT NULL
BEGIN
	DROP DATABASE DataWarehouse
END;
GO
  
-- Create Database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--  Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
