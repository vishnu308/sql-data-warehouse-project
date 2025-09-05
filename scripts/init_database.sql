/* 
============================================================================================

Create Databse and Schemas

============================================================================================

Script Purpose: 
	This scripts create a new database named "DataWarehouse" after checking if it already exists.
	If it is exist, it is dropped and recreated. Additionaly this script setups 3 schemas
	within the database: 'bronze', 'silver' and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database it is exists.
	All data in the database will be permanently deleted. Proced with caution
	and ensure you have proper backup before running this script.
 
*/



USE master;

-- Drop and Recreate the 'Datawarehouse' databse.

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database 

CREATE DATABASE DataWarehouse;
go
USE DataWarehouse;
go 
-- Create Schemas

CREATE SCHEMA bronze;
go

CREATE SCHEMA silver;
go

CREATE  SCHEMA gold;
go


