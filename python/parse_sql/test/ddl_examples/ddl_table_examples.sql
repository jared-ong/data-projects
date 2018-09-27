-- =============================================
-- TABLES
-- =============================================

CREATE TABLE Persons (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255) 
);

--Create the tables and insert the values.  
CREATE TABLE dbo.SUPPLY1 (  
supplyID INT PRIMARY KEY CHECK (supplyID BETWEEN 1 and 150),  
supplier CHAR(50)  
);  
create table dbo.SUPPLY2 (  
supplyID INT PRIMARY KEY CHECK (supplyID BETWEEN 151 and 300),  
supplier CHAR(50)  
);  
 CREATE	 TABLE   
 dbo.SUPPLY3 (  
supplyID INT PRIMARY KEY CHECK (supplyID BETWEEN 301 and 450),  
supplier CHAR(50)  
);  

CREATE TABLE 

dbo.SUPPLY4 (  
supplyID INT PRIMARY KEY CHECK (supplyID BETWEEN 451 and 600),  
supplier CHAR(50)  
);  
GO

CREATE TABLE t1 (a int, b int, c AS a/b);  

DROP TABLE Shippers;

  DROP TABLE [dbo].[Shippers2];

ALTER TABLE Persons
ADD DateOfBirth date;

ALTER 
TABLE
dbo.[t1] 
ADD 
col3 
varchar
(100)

ALTER TABLE Persons
ALTER COLUMN DateOfBirth2 year;

ALTER TABLE dbo.[tbl] ALTER COLUMN col1 varchar(300) NULL;
ALTER TABLE dbo.[tbl] ALTER COLUMN col2 varchar(20) NOT NULL;

ALTER 
TABLE 
dbo.[SUPPLY4] 
ALTER 
COLUMN 
supplier 
varchar(50)
NOT NULL

ALTER TABLE dbo.[tbl] ALTER COLUMN col4 varchar(100)

ALTER TABLE Persons
DROP COLUMN DateOfBirth3;

ALTER TABLE Customer DROP COLUMN Birth_Date;

ALTER TABLE dbo.Customer 
DROP COLUMN Birth_Date2

SELECT CustomerName, ContactName INTO dbo.CustomersBackup2019
FROM Customers;

SELECT Customers.CustomerName, Orders.OrderID
INTO CustomersOrderBackup2020
FROM Customers
LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID;

SELECT * INTO CustomersBackup2017
FROM Customers;

SELECT DISTINCT   
        Insured_Customers.FirstName, Insured_Customers.LastName,   
        Insured_Customers.YearlyIncome, Insured_Customers.MaritalStatus  
INTO Fast_Customers from Insured_Customers INNER JOIN   
(  
        SELECT * FROM CarSensor_Data where Speed > 35   
) AS SensorD  
ON Insured_Customers.CustomerKey = SensorD.CustomerKey  
ORDER BY YearlyIncome;

SELECT * INTO [dbo].[FactResellerSalesXL] ON FG2 FROM [dbo].[FactResellerSales];

SELECT DepartmentID, Name, GroupName, ModifiedDate  
INTO dbo.DepartmentsUsingOpenDataSource  
FROM OPENDATASOURCE('SQLNCLI',  
    'Data Source=server_name;Integrated Security=SSPI')  
    .AdventureWorks2012.HumanResources.Department;  
GO  

-- Specify the remote data source in the FROM clause using a four-part name   
-- in the form linked_server.catalog.schema.object.  
SELECT DepartmentID, Name, GroupName, ModifiedDate  
INTO dbo.Departments  
FROM MyLinkServer.AdventureWorks2012.HumanResources.Department  
GO  
-- Use the OPENQUERY function to access the remote data source.  
SELECT DepartmentID, Name, GroupName, ModifiedDate  
INTO dbo.DepartmentsUsingOpenQuery  
FROM OPENQUERY(MyLinkServer, 'SELECT *  
               FROM AdventureWorks2012.HumanResources.Department');   
GO  

SELECT IDENTITY (int, 100, 5) AS AddressID,   
       a.AddressLine1, a.City, b.Name AS State, a.PostalCode  
INTO Person.USAddress   
FROM Person.Address AS a  
INNER JOIN Person.StateProvince AS b 
  ON a.StateProvinceID = b.StateProvinceID  
WHERE b.CountryRegionCode = N'US'; 

SELECT * INTO dbo.NewProducts  
FROM Production.Product  
WHERE ListPrice > $25   
AND ListPrice < $100;  
GO  

SELECT c.FirstName, c.LastName, e.JobTitle, a.AddressLine1, a.City,   
    sp.Name AS [State/Province], a.PostalCode  
INTO dbo.EmployeeAddresses  
FROM Person.Person AS c  
    JOIN HumanResources.Employee AS e   
    ON e.BusinessEntityID = c.BusinessEntityID  
    JOIN Person.BusinessEntityAddress AS bea  
    ON e.BusinessEntityID = bea.BusinessEntityID  
    JOIN Person.Address AS a  
    ON bea.AddressID = a.AddressID  
    JOIN Person.StateProvince as sp   
    ON sp.StateProvinceID = a.StateProvinceID;  
GO  