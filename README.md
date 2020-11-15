# zeppos_bcpy

## purpose
Python library to implement microsoft sql server bulk copy (bcp)


## Requirements
#### When deployed and development
* Microsoft BCP.exe 
- Microsoft SQLCMD.exe
#### For Development so unitest run
- Microsoft Sql Server Express (localhost/sqlexpress)

## Dataflow design
- Pandas Dataframe to Microsoft Sql Server
```
Pandas Dataframe -> Temp csv file -> BCP tool -> Microsoft Sql Server
```

## References
BCP: https://www.sqlshack.com/bcp-bulk-copy-program-command-in-action/