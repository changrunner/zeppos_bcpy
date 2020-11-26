# zeppos_bcpy

## purpose
Python library to implement microsoft sql server bulk copy (bcp)


## Requirements
#### When deployed and development
- Microsoft BCP.exe (https://go.microsoft.com/fwlink/?linkid=2142258)
- Microsoft SQLCMD.exe (https://www.microsoft.com/en-us/download/details.aspx?id=53591)
- Microsoft ODBC driver version 17 (https://www.microsoft.com/en-us/download/details.aspx?id=56567)

#### For Development so unitest run
- Microsoft Sql Server Express (localhost/sqlexpress)
- Microsoft ODBC driver 17 (It must be this version)

## Dataflow design
- Pandas Dataframe to Microsoft Sql Server
```
Pandas Dataframe -> Temp csv file -> BCP tool -> Microsoft Sql Server
```

## References
BCP: https://www.sqlshack.com/bcp-bulk-copy-program-command-in-action/