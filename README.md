# Extract Azure DataLake Access Control Lists

This simple tool helps you audit who has access to files & directories in your [Azure Datalake](https://azure.microsoft.com/solutions/data-lake/). It traverses the provided filesystem & exports all file and directory [Access control lists](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-access-control#about-acls) into a structured files.  These files can then can imported into [PowerBI](https://powerbi.microsoft.com/) (or any analytics tool) to perform ad-hoc query who has access to what. 

## How it works

The tool uses the [Azure Storage File Data Lake client library for JavaScript](https://github.com/Azure/azure-sdk-for-js/tree/master/sdk/storage/storage-file-datalake), to list all the paths (directories and files) under the specified file system, and for each returned path, it requests the access control data using [this](https://docs.microsoft.com/javascript/api/@azure/storage-file-datalake/datalakefileclient?view=azure-node-latest#getAccessControl_PathGetAccessControlOptions_) method.

As this tool is typically long-running, in-order to make the tool re-startable in-case the tool is unterrupted, it maintains its progress in a local embedded [database](https://github.com/google/leveldb) automatically created in the local directory where the tool runs.  

If the tool was interrupted or terminated before finishing, to continue a run, use the ```-continue``` command line option. Then if the tool stops part way through, it will restart where it left off. If this option is not provided, the new invocation will clear down any existing state and start again.

## Recommendations

When running the tool across large Datalakes (>1million objects), its __strongly adviced__ to:

 * Use the ```-dir``` option to limit the data produced.  
 * You can start multiple instances of the tool for scale with difference, non-overlapping starting ```-dir```, but NOT in the same folder (due to the local database).
 * It is recommended to run the tool on a ```Azure VM```, ensuring the VM is in the SAME REGION as the DataLake. Select a VM with fast local IO (ephemeral VM is ideal), for example : ```Standard D4s v3```
 * If writing to Blob, use ```Premium Storage Account```, again, in the same region
 * Use `nohup` as example it the usage section



## Install

### Install Nodejs V14 (LTS) or above
 
* Ubunutu

        
        # Using Ubuntu
        curl -fsSL https://deb.nodesource.com/setup_14.x | sudo -E bash -
        sudo apt-get install -y nodejs
             

* Windows
        
        Follow instructions [here](https://nodejs.org/)

### Clone repo

        
        git clone https://github.com/khowling/az-adl-acl.git
        cd az-adl-acl
        

### Install dependencies & complile typescript:

        npm i
        npm run build


## Usage

```
node ./out/index.js \
        -a <ADL account name> \
        -f <ADL filesystem name> [-sas <SAS> | -key <key> ] \
        [ -storestr <connection str>  -storecontainer <blob container> ] \
        [-dir <starting directory] \
        [-continue]

NOTE:
    -storestr & -storecontainer   :  Azure Blob Storage Account connection string and container (if not provided, output to local dir)
    -continue                     :  Continue last run from where it left off
    -dir                          :  Starting point for ACL extraction (default "/")
```

__Example:__

NOTE: For large runs, recommended running in background using `nohup`

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
 
 Contains the output of the `listPaths` SDK call for each object in the datalake:

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

Contains the output of the `getAccessControl` ASK for each object in the datalake:

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

The SDK will implement a backoff-retry policy for calls to Azure, but if that fails 4 times, we will write an error to this file.

Errors can be retried, by re-running the tool and using the ```-dir``` option to scope just each errors
