## Extract ADL ACLs

Command line tool traverses the Azure Datalake filesystem to export all file and directory acls from within the specified directory

This tool maintains its state, this enables it to be re-startable.  If the tool was interrupted or terminated before finishing, to continue a run, be sure to use the '-continue' option, then if the tool stops part way through, it will restart where it left off. If this is not provided, the new invocation will clear down the state and output files and start again.


NOTE: Recomendations for large Datalakes (>10million objects):
 * Use the '-dir' option to limit the data produced, and you can start multiple instances of the tool for scale, but NOT in the same folder.
 * If running the tool on a Azure VM, its recommended to ensure the VM is in the same region as the DataLake, and you select a machine with fast local IO (ephemeral VM is ideal), for example : `Standard D4s v3`
 * If writing to Blob, use Premium Storage Account in the same region
 * Use `nohup` as example it the usage section

## Prepreqs

 * Nodejs V14 (LTS) or above

## Usage

First, install dependencies & complile typescript:

        ```
        npm i
        npm run-script build
        ```

```
node ./out/index.js -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ] [ -storestr <connection str>  -storecontainer <blob container> ] [-dir <starting directory] [-continue]

    -storestr & -storecontainer   :  Azure Blob Storage Account connection string and container (if not provided, output to local dir)
    -continue                     :  Continue last run from where it left off
    -dir                          :  Starting point for ACL extraction (default "/")
```

For large runs, recommended running in background using `nohup`, for example:
```
BACKGROUND=true nohup node ./out/index.js  \
  -a mylake \
  -f filesystem \
  -sas "lake sas"  \
  -dir /start/dir \
  -storestr "DefaultEndpointsProtocol=https;AccountName=xxxxxx;AccountKey=xxxxxx" \
  -storecontainer "output"
```

## Output

Output supported to either local files or Blob container, both in CSV format

 ### `paths.csv` 
 
 Contains the output of this `listPaths` API call for each object in the datalake under the passed in directory: https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/filesystem/listpaths

        ```
        isDirectory,Name,Owner,Group,OnwerPermissions,GroupPermissions,ExecutePermissions
        true,"dir1",$superuser,$superuser,RRR,R-R,---
        true,"tendirtop8",$superuser,$superuser,RRR,R-R,---
        true,"tendirtop9",$superuser,$superuser,RRR,R-R,---
        false,"dir1/newfile1615214993252",$superuser,$superuser,RR-,R--,---
        false,"dir1/newfile1615215601371",$superuser,$superuser,RR-,R--,---
        false,"dir1/newfile1615216127229",$superuser,$superuser,RR-,R--,---
        true,"fivedirtop1/fivedirmiddle0",$superuser,$superuser,RRR,R-R,---
        true,"fivedirtop1/fivedirmiddle1",$superuser,$superuser,RRR,R-R,---
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
