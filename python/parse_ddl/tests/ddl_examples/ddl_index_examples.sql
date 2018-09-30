-- =============================================
-- INDEXES
-- =============================================

-- Create a nonclustered index on a table or view  
CREATE INDEX i1 ON t1 (col1);  

--Create a clustered index on a table and use a 3-part name for the table  
CREATE CLUSTERED INDEX index1 ON d1.s1.t1 (col1);  


-- Syntax for SQL Server and Azure SQL Database
-- Create a nonclustered index with a unique constraint 
-- on 3 columns and specify the sort order for each column  
CREATE UNIQUE INDEX unique1 ON t1 (col1 DESC, col2 ASC, col3 DESC);  


CREATE UNIQUE CLUSTERED INDEX Idx1 ON t1(c);  
INSERT INTO t1 VALUES (1, 0);  

CREATE INDEX IX_VendorID ON ProductVendor (VendorID);  
CREATE INDEX IX_VendorID ON dbo.ProductVendor (VendorID DESC, Name ASC, Address DESC);  
CREATE INDEX IX_VendorID ON Purchasing..ProductVendor (VendorID);  

CREATE NONCLUSTERED INDEX IX_SalesPerson_SalesQuota_SalesYTD ON Sales.SalesPerson (SalesQuota, SalesYTD);

CREATE INDEX IX_FF ON dbo.FactFinance ( FinanceKey ASC, DateKey ASC );  

--Rebuild and add the OrganizationKey  
CREATE INDEX IX_FF ON dbo.FactFinance ( FinanceKey, DateKey, OrganizationKey DESC)  
WITH ( DROP_EXISTING = ON );  


-- Create a nonclustered index called IX_ProductVendor_VendorID   
-- on the Purchasing.ProductVendor table using the BusinessEntityID column.   
CREATE NONCLUSTERED INDEX IX_ProductVendor_VendorID   
    ON Purchasing.ProductVendor (BusinessEntityID);   
GO  


--Create a clustered index on the PRIMARY filegroup if the index does not exist.  
CREATE UNIQUE CLUSTERED INDEX  
    AK_BillOfMaterials_ProductAssemblyID_ComponentID_StartDate   
        ON Production.BillOfMaterials (ProductAssemblyID, ComponentID,   
        StartDate)  
    ON 'PRIMARY';  
GO  

CREATE CLUSTERED INDEX cci_SimpleTable ON SimpleTable (ProductKey);  


--Columnstore indexes
CREATE CLUSTERED COLUMNSTORE INDEX cci_FactInternetSales2  
ON dbo.FactInternetSales2;  

CREATE CLUSTERED COLUMNSTORE INDEX cci_SimpleTable  
ON SimpleTable  
WITH (DROP_EXISTING = ON);  


-- Find an existing index named IX_ProductVendor_VendorID and delete it if found.   
IF EXISTS (SELECT name FROM sys.indexes  
            WHERE name = N'IX_ProductVendor_VendorID')   
    DROP INDEX IX_ProductVendor_VendorID ON Purchasing.ProductVendor;   
GO  

DROP INDEX IX_ProductVendor_BusinessEntityID   
    ON Purchasing.ProductVendor;  
GO  
DROP INDEX PXML_ProductModel_CatalogDescription   
    ON Production.ProductModel;  

DROP INDEX  
    IX_PurchaseOrderHeader_EmployeeID ON Purchasing.PurchaseOrderHeader,  
    IX_Address_StateProvinceID ON Person.Address;  
GO  

DROP INDEX AK_BillOfMaterials_ProductAssemblyID_ComponentID_StartDate   
    ON Production.BillOfMaterials WITH (ONLINE = ON, MAXDOP = 2);  
GO  

