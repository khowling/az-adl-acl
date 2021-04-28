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

        ```
        # Using Ubuntu
        curl -fsSL https://deb.nodesource.com/setup_14.x | sudo -E bash -
        sudo apt-get install -y nodejs
        ```

 * Clone repo

        ```
        git clone https://github.com/khowling/az-adl-acl.git
        cd az-adl-acl
        ```
## Usage

First, install dependencies & complile typescript:

        ```
        npm i
        npm run build
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
        true,"root100k/dir100k-0",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-1",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-10",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-11",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-12",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-13",$superuser,$superuser,rwx,r-x,rw-
        true,"root100k/dir100k-14",$superuser,$superuser,rwx,r-x,rw-
        ```
### `acls.csv` 

Contains the output of this `getAccessControl` API for each object in the datalake under the passed in directory: https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/getproperties

        ```
        Filepath,DefaultScope,Type,Entity,Read,Write,Execute
        "root100k/dir100k-0",false,user,,true,true,true
        "root100k/dir100k-0",false,group,,true,false,true
        "root100k/dir100k-0",false,other,,true,true,false
        "root100k/dir100k-1",false,user,,true,true,true
        "root100k/dir100k-1",false,group,,true,false,true
        "root100k/dir100k-1",false,other,,true,true,false
        "root100k/dir100k-10",false,user,,true,true,true
        "root100k/dir100k-10",false,group,,true,false,true
        "root100k/dir100k-10",false,other,,true,true,false
        ```
  
### `errors.csv`

The program will implement a backoff-retry policy for calls to Azure, but if that fails 4 times, we will write an error to this file.  
