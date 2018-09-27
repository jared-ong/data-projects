-- =============================================
-- TYPES
-- =============================================

CREATE TYPE SSN  
FROM varchar(11) NOT NULL ;  

CREATE TYPE [schema].[LocationTableType] AS TABLE   
    ( LocationName VARCHAR(50)  
    , CostRate INT );  
GO  

CREATE TYPE dbo.StateTbl AS TABLE
( StateID INT
, StateCode VARCHAR(2)
, StateName VARCHAR(200)
)
;

ALTER TYPE SSN  
FROM varchar(11) NOT NULL ;  

ALTER TYPE [schema].[LocationTableType] AS TABLE   
    ( LocationName VARCHAR(50)  
    , CostRate INT );  
GO  

ALTER TYPE dbo.StateTbl AS TABLE
( StateID INT
, StateCode VARCHAR(2)
, StateName VARCHAR(200)
)
;

DROP TYPE ssn ;  

DROP TYPE IF EXISTS ssn ;  

DROP TYPE [dbo].[ssn] ;  