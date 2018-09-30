# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 02:33:15 2018

@author: jong
"""


import re

# Test 1
ddl_string = ("CREATE PROCEDURE[dbo].[TestspRoleActionFetchLastUpdated] AS\
\
SELECT MAX(LastUpdated) AS LastUpdated FROM(\
    SELECT MAX(Updated) AS LastUpdated FROM RoleActions WITH(NOLOCK)\
  	UNION ALL\
  	SELECT MAX(DeletedDate) AS LastUpdated FROM DeletedRoleActions WITH(NOLOCK)\
) x\
GO")
ddl_string = re.sub(r'\bPROCEDURE\b', '', ddl_string, flags=re.I)
print(ddl_string)

# Test 2
ddl_string = "CREATE NONCLUSTERED INDEX WORD alter"
ddl_string = re.sub(r'\bNONCLUSTERED\b', '', ddl_string, flags=re.IGNORECASE)
ddl_string = re.sub(r'\bCLUSTERED\b', '', ddl_string, flags=re.IGNORECASE)
ddl_string = re.sub(r'\bCREATEb', '', ddl_string, flags=re.IGNORECASE)
ddl_string = re.sub(r'\bALTER\b', '', ddl_string, flags=re.IGNORECASE)
ddl_string = re.sub(r'\bDROP\b', '', ddl_string, flags=re.IGNORECASE)
ddl_string = re.sub(r'\bSP_RENAME\b', '', ddl_string, flags=re.IGNORECASE)
print(ddl_string)

ddl_string = "alter createprocedure"
object_action = re.search(r"(\bcreate\b|\balter\b|\bdrop\b|\bsp_rename\b)",
                          ddl_string,
                          re.I)
object_action = object_action.group(0).upper()
ddl_string = re.sub(r'\b%s\b' % object_action, '',
                    ddl_string,
                    flags=re.IGNORECASE)

print(object_action)
print(ddl_string)
