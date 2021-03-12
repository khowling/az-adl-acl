## Extract ADL ACLs

Command line tool traverses the Azure Datalake filesystem to export all file and directory acls from within the specified directory

This tool maintains its state, this enables it to be re-startable.  If the tool was interrupted or terminated before finishing, to continue a run, be sure to use the '-continue' option, then if the tool stops part way through, it will restart where it left off. If this is not provided, the new invocation will clear down the state and output files and start again.


NOTE: For very large Datalakes (>10million objects), use the '-dir' option to limit the data produced, and you can start multiple instances of the tool for scale, but NOT in the same folder.

If running the tool on a Azure VM, its recommended to ensure the VM is in the same region as the DataLake, and you select a machine with fast local IO (ephemeral VM is ideal), for example : `Standard D4s v3`

## Prepreqs

 * nodejs V14 (TLS)

## Usage

NOTE: ensure you install dependencies before running: ```$ npm i```

```
npx ts-node ./index.ts -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ] [-dir <starting directory] [-continue]

    -continue   :  Continue last run from where it left off
    -dir        :  Starting point for ACL extraction (default "/")
```

For large runs, recommended running in background using `nohup`, for example:
```
BACKGROUND=true nohup npx ts-node ./index.ts  \
  -a lakename
  -f filesystem
  -sas "xxxxxxxxxxxxxxxxxxxxxxx"     
  -dir /test &
```

## Output

Currently, the only supported output is local CSV files.  Work-in-progress to stream the data to Blob

 ### `paths.csv` 
 
 Contains the output of this `listPaths` API call for each object in the datalake under the passed in directory: https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/listpaths

        ```
        isDirectory,Filepath,Path,Name,Owner,Group
        true,dir1,,dir1,$superuser,$superuser
        true,fivedirtop0,,fivedirtop0,$superuser,$superuser
        true,fivedirtop1,,fivedirtop1,$superuser,$superuser
        true,fivedirtop2,,fivedirtop2,$superuser,$superuser
        true,fivedirtop3,,fivedirtop3,$superuser,$superuser
        true,fivedirtop4,,fivedirtop4,$superuser,$superuser
        ```
### `acls.csv` 

Contains the output of this `getAccessControl` API for each object in the datalake under the passed in directory: https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/getproperties

        ```
        Filepath,Type,Entity,Read,Write,Execute
        dir1,user,,true,true,true
        dir1,group,,true,false,true
        dir1,other,,false,false,false
        fivedirtop0,user,,true,true,true
        fivedirtop0,group,,true,false,true
        fivedirtop0,other,,false,false,false
        fivedirtop1,user,,true,true,true
        dir1/newfile1615214993252,user,,true,true,false
        ```
  
### `errors.csv`

The program will implement a backoff-retry policy for calls to Azure, but if that fails 4 times, we will write an error to this file.  
