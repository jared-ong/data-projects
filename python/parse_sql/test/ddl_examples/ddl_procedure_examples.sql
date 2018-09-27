-- =============================================
-- PROCEDURE
-- =============================================

CREATE PROC What_DB_is_this     
AS   
SELECT DB_NAME() AS ThisDB; 

CREATE PROC[dbo].[What_DB_is_this2]     
AS   
SELECT DB_NAME() AS ThisDB; 

Create PROC What_DB_is_that @ID int   
AS    
SELECT DB_NAME(@ID) AS ThatDB;   

CREATE PROCEDURE dbo.usp_add_kitchen @dept_id int, @kitchen_count int NOT NULL  
WITH EXECUTE AS OWNER, SCHEMABINDING, NATIVE_COMPILATION  
AS  
BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = N'us_english')  

  UPDATE dbo.Departments  
  SET kitchen_count = ISNULL(kitchen_count, 0) + @kitchen_count  
  WHERE id = @dept_id  
END;  
GO  

create    procedure    SelectAllCustomers @City nvarchar(30)
AS
SELECT * FROM Customers WHERE City = @City
GO;

DROP PROCEDURE dbo.uspMyProc;  
GO  

DROP PROC dbo.uspMyProc2;  
GO

DROP PROCEDURE IF EXISTS dbo.uspMyProc3;  
GO  

DROP PROCEDURE dbo.uspGetSalesbyMonth, dbo.uspUpdateSalesQuotes, dbo.uspGetSalesByYear; 

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'MyProc')
DROP PROCEDURE MyProc
GO

IF OBJECT_ID ( 'Purchasing.uspVendorAllInfo', 'P' ) IS NOT NULL   
    DROP PROCEDURE Purchasing.uspVendorAllInfo;  
GO  
CREATE PROCEDURE Purchasing.uspVendorAllInfo  
WITH EXECUTE AS CALLER  
AS  
    SET NOCOUNT ON;  
    SELECT v.Name AS Vendor, p.Name AS 'Product name',   
      v.CreditRating AS 'Rating',   
      v.ActiveFlag AS Availability  
    FROM Purchasing.Vendor v   
    INNER JOIN Purchasing.ProductVendor pv  
      ON v.BusinessEntityID = pv.BusinessEntityID   
    INNER JOIN Production.Product p  
      ON pv.ProductID = p.ProductID   
    ORDER BY v.Name ASC;  
GO 

USE AdventureWorks2012;  
GO  
ALTER PROCEDURE Purchasing.uspVendorAllInfo  
    @Product varchar(25)   
AS  
    SET NOCOUNT ON;  
    SELECT LEFT(v.Name, 25) AS Vendor, LEFT(p.Name, 25) AS 'Product name',   
    'Rating' = CASE v.CreditRating   
        WHEN 1 THEN 'Superior'  
        WHEN 2 THEN 'Excellent'  
        WHEN 3 THEN 'Above average'  
        WHEN 4 THEN 'Average'  
        WHEN 5 THEN 'Below average'  
        ELSE 'No rating'  
        END  
    , Availability = CASE v.ActiveFlag  
        WHEN 1 THEN 'Yes'  
        ELSE 'No'  
        END  
    FROM Purchasing.Vendor AS v   
    INNER JOIN Purchasing.ProductVendor AS pv  
      ON v.BusinessEntityID = pv.BusinessEntityID   
    INNER JOIN Production.Product AS p   
      ON pv.ProductID = p.ProductID   
    WHERE p.Name LIKE @Product  
    ORDER BY v.Name ASC;  
GO  

USE AdventureWorks2012;  
GO  
ALTER PROC Purchasing.uspVendorAllInfo  
    @Product varchar(25)   
AS  
    SET NOCOUNT ON;  
    SELECT LEFT(v.Name, 25) AS Vendor, LEFT(p.Name, 25) AS 'Product name',   
    'Rating' = CASE v.CreditRating   
        WHEN 1 THEN 'Superior'  
        WHEN 2 THEN 'Excellent'  
        WHEN 3 THEN 'Above average'  
        WHEN 4 THEN 'Average'  
        WHEN 5 THEN 'Below average'  
        ELSE 'No rating'  
        END  
    , Availability = CASE v.ActiveFlag  
        WHEN 1 THEN 'Yes'  
        ELSE 'No'  
        END  
    FROM Purchasing.Vendor AS v   
    INNER JOIN Purchasing.ProductVendor AS pv  
      ON v.BusinessEntityID = pv.BusinessEntityID   
    INNER JOIN Production.Product AS p   
      ON pv.ProductID = p.ProductID   
    WHERE p.Name LIKE @Product  
    ORDER BY v.Name ASC;  
GO 
