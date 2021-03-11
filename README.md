## Extract ADL ACLs

Command line tool traverses Azure Datalake filesystem to export all file and directory acls from the specified directory

This tool maintains its state, that enables it to be re-startable.  On the FIRST run, be sure to use the '-R' reset, option, then if the tool stops part way through, you can restart it just by missing off the -R. (Reset will clear down the state and output files and start again)


NOTE: For very large Datalakes (>10million objects), use the '-dir' option to limit the data produced, and you can start multiple instances of the tool for scale, but NOT in the same folder.


## Usage


npx ts-node ./index.ts -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ] [-dir <starting directory] [-R]

    -R   :  Reset, clear the current run progress & start from begining
    -dir :  Starting point for ACL extraction (default "/")


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