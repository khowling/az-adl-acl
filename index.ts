const fs = require('fs')


import { Atomic } from './atomic'
import {
    DataLakeServiceClient,
    AccountSASPermissions,
    StorageRetryOptions,
    StorageRetryPolicyType,
    Pipeline,
    StoragePipelineOptions,
    StorageSharedKeyCredential
} from "@azure/storage-file-datalake"

import { setLogLevel } from '@azure/logger'


async function createFiles(fileSystemClient, seed, num) {
    let i = 0
    for (let topdir of [...Array(num).keys()].map(n => `${seed}dirtop${n}`)) {

        for (let middle of [...Array(num).keys()].map(n => `${seed}dirmiddle${n}`)) {

            for (let file of [...Array(num).keys()].map(n => `${seed}file${n}`)) {
                process.stdout.cursorTo(0)
                process.stdout.write(`Creating files... ${i++}`)
                await fileSystemClient.getFileClient(`${topdir}/${middle}/${file}`).create()

                // Create a file
                //await fileClient.append('content', 0, content.length);
                //const flushFileResponse = await fileClient.flush(content.length);
                //console.log(`Upload file ${fileName} successfully`, flushFileResponse.requestId);
            }
        }
    }
    console.log('\ndone')
}

async function writePaths(fileSystemClient) {

    await fs.promises.writeFile('./paths.csv', 'isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
    let iter = await fileSystemClient.listPaths({ path: "/", recursive: true });

    let file = 0
    let dir = 0
    for await (const path of iter) {
        if (path.isDirectory) dir++; else file++;
        process.stdout.cursorTo(0)
        process.stdout.write(`paths... file=${file} dir=${dir} `)
        const lastidx = path.name.lastIndexOf('/')

        await fs.promises.appendFile('./paths.csv', `${path.isDirectory},${path.name},${lastidx > 0 ? path.name.substr(0, lastidx) : ''},${lastidx > 0 ? path.name.substr(lastidx + 1) : path.name},${path.owner},${path.group}` + "\n")

    }
    console.log('done')
}


async function getALCs(fileSystemClient, release, fileinfo) {

    const [isDirectory, path] = fileinfo.split(',')
    //console.log(`getALCs: path=${path}, isDirectory=${isDirectory}`)
    if (isDirectory === 'true') {
        const dpermissions = await fileSystemClient.getDirectoryClient(path).getAccessControl();
        dpermissions.acl.forEach(async p => {
            await fs.promises.appendFile('./acls.csv', `${path},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}` + "\n")
        })
    } else {
        const fpermissions = await fileSystemClient.getFileClient(path).getAccessControl();
        fpermissions.acl.forEach(async p => {
            await fs.promises.appendFile('./acls.csv', `${path},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}` + "\n")
        })

    }
    release()

}
async function main() {

    const split = process.env.SPLIT || 2
    let opt: StoragePipelineOptions
    /*
        // https://github.com/Azure/azure-sdk-for-js/blob/52d621342a9094d7af0b38eed9476af1a451070d/sdk/storage/storage-file-datalake/src/Pipeline.ts#L157
        // https://azuresdkdocs.blob.core.windows.net/$web/javascript/azure-storage-file-datalake/12.3.1/interfaces/storagepipelineoptions.html
    
        setLogLevel('warning');
        opt = {
            userAgentOptions: {},
            keepAliveOptions: {
                enable: true
            },
            retryOptions: {
                retryPolicyType: StorageRetryPolicyType.FIXED,
                maxTries: 1
            },
            // https://github.com/Azure/azure-sdk-for-js/blob/52d621342a9094d7af0b38eed9476af1a451070d/sdk/core/core-http/src/nodeFetchHttpClient.ts
            httpClient: {
    
            }
        }
    */

    const filename = process.argv[2]

    const accountName = process.env.ACCOUNTNAME,
        fileSystemName = process.env.FILESYSTEMNAME,
        adlurl = `https://${accountName}.dfs.core.windows.net` + (process.env.SASKEY ? `?${process.env.SASKEY}` : ''),
        serviceClient = new DataLakeServiceClient(adlurl, process.env.ADLKEY ? new StorageSharedKeyCredential(accountName, process.env.ADLKEY) : undefined, opt)

    //console.log("sas token" + serviceClient.generateAccountSasUrl(new Date(Date.now() + (3600 * 1000 * 24)), AccountSASPermissions.parse('rwlacup'), "s"))

    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)


    if (!filename) {
        writePaths(fileSystemClient)
    } else {

        const mutex = new Atomic(50)
        const fileStream = fs.createReadStream(filename)

        await fs.promises.writeFile('./acls.csv', 'Filepath,Type,Entity,Read,Write,Execute' + "\n")
        const rl = require('readline').createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });
        // Note: we use the crlfDelay option to recognize all instances of CR LF
        // ('\r\n') in input.txt as a single line break.
        let line = 0
        for await (const fileline of rl) {
            // Each line in input.txt will be successively available here as `line`.
            line++
            process.stdout.cursorTo(0)
            process.stdout.write(`acls... ${line}`)

            if (line === 1) continue; // header

            let release = await mutex.aquire()
            getALCs(fileSystemClient, release, fileline)

        }
        console.log('done')
    }

}

main()