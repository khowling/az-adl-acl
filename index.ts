const fs = require('fs')


import { Atomic } from './atomic'
import {
    DataLakeServiceClient,
    AccountSASPermissions,
    StorageRetryOptions,
    StorageRetryPolicyType,
    Pipeline,
    ListPathsOptions,
    DataLakeFileSystemClient,
    StoragePipelineOptions,
    StorageSharedKeyCredential
} from "@azure/storage-file-datalake"

import {
    BlobServiceClient,
    ContainerClient,
    AppendBlobClient
} from '@azure/storage-blob'

import { setLogLevel } from '@azure/logger'

/*
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

async function writePaths(fileSystemClient: DataLakeFileSystemClient) {

    await writePathHeader()
    let iter = await fileSystemClient.listPaths({ path: "/", recursive: true });

    let file = 0, dir = 0
    for await (const path of iter) {
        if (path.isDirectory) dir++; else file++;
        process.stdout.cursorTo(0)
        process.stdout.write(`paths... file=${file} dir=${dir} `)
        await writePathline(path)

    }
    console.log('done')
}



// RECURSION MEMORY ISSUES At LARGE SCALE !!
// FATAL ERROR: Ineffective mark-compacts near heap limit Allocation failed - JavaScript heap out of memory
async function writePathsRecurcive(fileSystemClient: DataLakeFileSystemClient) {

    await writePathHeader()
    let topdirs = 0, topfiles = 0
    const mutex = new Atomic(50)

    async function mylistPaths(fileSystemClient, path, release): Promise<{ childPaths: Array<string>, dirs: number, files: number }> {
        //console.log(`listPaths level=${level} path=${path}`)
        const childPaths: Array<string> = []
        const paths = await fileSystemClient.listPaths({ path: path, recursive: false } as ListPathsOptions)

        let dirs = 0, files = 0

        for await (const path of paths) {
            if (path.isDirectory) {
                dirs++
                childPaths.push(path.name)
            } else {
                files++
            }
            await writePathline(path)
        }
        release()
        return { childPaths, dirs, files }
    }

    async function processesChildren({ childPaths, dirs, files }) {
        topdirs = topdirs + dirs; topfiles = topfiles + files
        process.stdout.cursorTo(0)
        process.stdout.write(`top paths... file=${topfiles} dir=${topdirs} `)
        //console.log(`processesChildren level=${level} dirs=${childPaths.length}`)
        for (const path of childPaths) {
            let release = await mutex.aquire()
            mylistPaths(fileSystemClient, path, release).then(processesChildren)
        }
    }

    await processesChildren({ childPaths: ["/"], dirs: 0, files: 0 })
    console.log('done')

}



async function getALCs(fileSystemClient: DataLakeFileSystemClient, release, fileinfo: string) {

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

*/

const { Transform } = require('stream');
class PathsCSV extends Transform {
    private _donehead = false

    _transform(chunk, encoding, cb) {
        try {
            if (!this._donehead) {
                this.push('isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
                this._donehead = true
            }
            const path = pathType.fromBuffer(chunk)

            const lastidx = path.name.lastIndexOf('/')
            this.push(`${path.isDirectory},${path.name},${lastidx > 0 ? path.name.substr(0, lastidx) : ''},${lastidx > 0 ? path.name.substr(lastidx + 1) : path.name},${path.owner},${path.group}` + "\n")

            cb()
        } catch (e) {
            cb(new Error(`chunk ${chunk} is not a json: ${e}`));
        }
    }
}

class ACLsCSV extends Transform {
    private _donehead = false

    _transform(chunk, encoding, cb) {
        try {
            if (!this._donehead) {
                this.push('Filepath,Type,Entity,Read,Write,Execute' + "\n")
                this._donehead = true
            }
            const { path, acls } = aclType2.fromBuffer(chunk)
            for (let a of acls) {
                this.push(`${path},${a.accessControlType},${a.entityId},${a.permissions.read},${a.permissions.write},${a.permissions.execute}` + "\n")
            }

            cb()
        } catch (e) {
            cb(new Error(`chunk ${chunk} is not a json: ${e}`));
        }
    }
}


const avro = require('avsc');
const level = require('level')
var sub = require('subleveldown')
import { JobManager, JobStatus, JobReturn, JobData, JobTask, JobRunningData } from './jobmanager'

const pathType = avro.Type.forValue({
    name: "dir1",
    isDirectory: true,
    lastModified: new Date(),
    owner: "$superuser",
    group: "$superuser",
    permissions: {
        owner: {
            read: true,
            write: true,
            execute: true
        },
        group: {
            read: true,
            write: false,
            execute: true
        },
        other: {
            read: false,
            write: false,
            execute: false
        },
        stickyBit: false,
        extendedAcls: false
    },
    creationTime: "132596885753691420",
    etag: "0x8D8E2416422FEF2"
})

const aclType2 = avro.Type.forValue({
    path: "dir1",
    acls: [
        {
            "defaultScope": false,
            "accessControlType": "user",
            "entityId": "",
            "permissions": {
                "read": true,
                "write": true,
                "execute": false
            }
        },
        {
            "defaultScope": false,
            "accessControlType": "user",
            "entityId": "",
            "permissions": {
                "read": true,
                "write": true,
                "execute": false
            }
        }
    ]
})


class WriteAppendBlobs {

    private blobs
    private containerClient
    private cachedClients = {}
    private _mutexs = {}


    constructor(db, connectionStr: string, container: string) {

        this.blobs = sub(db, 'blobs', { valueEncoding: 'json' })
        const blobServiceClient = BlobServiceClient.fromConnectionString(connectionStr);
        this.containerClient = blobServiceClient.getContainerClient(container)
    }

    private async getClient(blob: string, length: number): Promise<AppendBlobClient> {
        const cc = this.cachedClients[blob]
        let { seq, block, size, path } = await this.blobs.get(blob)
        // Each block in an append blob can be a different size, up to a maximum of 4 MB, and an append blob can include up to 50,000 blocks. 
        if (block > 40000 || size + length > 150000000000) {
            seq++; block = 0; size = length
            this.cachedClients[blob] = this.containerClient.getAppendBlobClient(`${path}.${seq}.csv`)
            await this.cachedClients[blob].createIfNotExists()
        } else {
            block = block + 1; size = size + length
            if (!this.cachedClients[blob]) {
                this.cachedClients[blob] = this.containerClient.getAppendBlobClient(`${path}.${seq}.csv`)
                await this.cachedClients[blob].createIfNotExists()
            }
        }
        await this.blobs.put(blob, { seq, block, size, path })
        return this.cachedClients[blob]
    }

    async init(blobs: string[], continueRun: boolean) {
        for (const b of blobs) {
            this._mutexs[b] = new Atomic(1)
        }

        if (!continueRun) {
            const d = new Date()
            for (const b of blobs) {
                await this.blobs.put(b, { seq: 0, block: 0, size: 0, path: `${d.toISOString()}/${b}` })
            }
        }
    }

    async write(blob: string, body: string) {
        let release = await this._mutexs[blob].aquire()
        const c = await this.getClient(blob, body.length)
        await c.appendBlock(body, body.length)
        release()

    }


}

async function writePathsRestartableConcurrent(db, fileSystemClient: DataLakeFileSystemClient, concurrency: number, startDir: string, continueRun: boolean, azBlob?: WriteAppendBlobs) {


    if (!continueRun) {
        if (azBlob) {
            await azBlob.init(['paths', 'acls', 'errors'], continueRun)
            await azBlob.write('paths', 'isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
            await azBlob.write('acls', 'Filepath,Type,Entity,Read,Write,Execute' + "\n")
            await azBlob.write('errors', 'TaskSequence,TaskType,Path,Error' + "\n")
        } else {
            await fs.promises.writeFile('./paths.csv', 'isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
            await fs.promises.writeFile('./acls.csv', 'Filepath,Type,Entity,Read,Write,Execute' + "\n")
            await fs.promises.writeFile('./errors.csv', 'TaskSequence,TaskType,Path,Error' + "\n")
        }
    }


    const paths_q = new JobManager(db, concurrency, async function (seq: number, d: JobData, r: JobRunningData): Promise<JobReturn> {
        let currentBatch = 0
        try {
            if (d.task === JobTask.ListPaths) {
                for await (const response of fileSystemClient.listPaths({ path: d.path, recursive: false }).byPage({ maxPageSize: 10000 })) {
                    if (response.pathItems) {
                        let dirs = 0, files = 0, acls = 0, errors = 0
                        let newJobs: Array<JobData> = []
                        for (const path of response.pathItems) {

                            newJobs.push({ task: JobTask.GetACLs, path: path.name, isDirectory: path.isDirectory })
                            if (path.isDirectory) {
                                dirs++
                                newJobs.push({ task: JobTask.ListPaths, path: path.name, isDirectory: path.isDirectory })
                            } else {
                                files++
                            }
                            //await pathsdb.put(path.name, pathType.toBuffer(path))                        
                        }

                        // 
                        if (currentBatch >= r.completedBatches) {

                            const body = response.pathItems.map(path => {
                                const lastidx = path.name.lastIndexOf('/')
                                return `${path.isDirectory},${path.name},${lastidx > 0 ? path.name.substr(0, lastidx) : ''},${lastidx > 0 ? path.name.substr(lastidx + 1) : path.name},${path.owner},${path.group}`
                            }).join("\n") + "\n"

                            if (azBlob) {
                                await azBlob.write('paths', body)
                            } else {
                                await fs.promises.appendFile('./paths.csv', body)
                            }

                            await paths_q.runningCompleteBatch(seq, currentBatch++, { files, acls, errors, dirs }, newJobs)
                            //}
                        }

                    }
                }
                return { seq, status: JobStatus.Success }

            } else if (d.task === JobTask.GetACLs) {

                const permissions = d.isDirectory ? await fileSystemClient.getDirectoryClient(d.path).getAccessControl() : await fileSystemClient.getFileClient(d.path).getAccessControl()
                let acls = permissions.acl.length

                const body = permissions.acl.map(p => `${d.path},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}`).join("\n") + "\n"

                if (azBlob) {
                    await azBlob.write('acls', body)
                } else {
                    await fs.promises.appendFile('./acls.csv', body)
                }
                return { seq, status: JobStatus.Success, metrics: { files: 0, dirs: 0, errors: 0, acls } }
            } else {
                throw new Error(`Unknown JobTask ${d.task}`)
            }

        } catch (err) {
            const body = `${seq},${d.task},${d.path},${JSON.stringify(err)}` + "\n"
            if (azBlob) {
                await azBlob.write('errors', body)
            } else {
                await fs.promises.appendFile('./errors.csv', body)
            }
            console.error(`Job Error: seq=${seq}, task=${d.task}, err=${JSON.stringify(err)}`)
            return { seq, status: JobStatus.Error, metrics: { files: 0, dirs: 0, errors: 1, acls: 0 } }
        }
    })

    await paths_q.start(continueRun)
    if (!continueRun) {

        //await pathsdb.clear()
        console.log(`Seeding path startDir=${startDir}`)
        await paths_q.submit({ task: JobTask.ListPaths, path: startDir, isDirectory: true })
    }
    await paths_q.finishedSubmitting()


    /*
        const aclsdb = sub(db, 'acls', { valueEncoding: 'binary' })
        const acls_q = new JobManager("acls", db, 50, async function (seq: number, path: string): Promise<JobReturn> {
            try {
                const val = await pathsdb.get(path)
                const p = pathType.fromBuffer(val)
    
                let dirs = p.isDirectory ? 1 : 0, files = p.isDirectory ? 0 : 1
                const permissions = p.isDirectory ? await fileSystemClient.getDirectoryClient(path).getAccessControl() : await fileSystemClient.getFileClient(path).getAccessControl()
                //for (let a of permissions.acl) {
                await aclsdb.put(path, aclType2.toBuffer({ path, acls: permissions.acl }))
                //}
    
                return { seq, status: JobStatus.Success, metrics: { dirs, files } }
            } catch (e) {
                console.error(e)
                process.exit(1)
            }
        })
    
        await acls_q.start(reset)
        if (reset) {
            await aclsdb.clear()
            await new Promise((res, rej) => {
                let cnt = 0
                const feed = pathsdb.createKeyStream().on('data', async d => {
                    cnt++
                    await acls_q.submit(d)
                })
                feed.on('end', () => {
                    console.log(`finish queueing ${cnt} acl jobs`)
                    res(cnt)
                })
            })
    
        }
    
        await acls_q.finishedSubmitting()
    
        await new Promise((res, rej) => {
            console.log('Creating "paths.csv" file...')
            const pfile = pathsdb.createValueStream().pipe(new PathsCSV()).pipe(fs.createWriteStream('./paths.csv'))
            pfile.on('finish', () => {
                console.log('finished  "paths.csv"')
                res(true)
            })
        })
    
        await new Promise((res, rej) => {
            console.log('Creating "acls.csv" file...')
            const afile = aclsdb.createValueStream().pipe(new ACLsCSV()).pipe(fs.createWriteStream('./acls.csv'))
            afile.on('finish', () => {
                console.log('finished "acls.csv"')
                res(true)
            })
        })
    */
    db.close(() => console.log('closing job manager'))
}

function args() {

    function usage(e?) {
        if (e) console.log(e)
        console.log('usage:')
        console.log(`    ${process.argv[0]} ${process.argv[1]} -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ]  [ -storestr <connection str>  -storecontainer <blob container> ] [-dir <starting directory] [-continue]`)
        console.log('')
        console.log(`   -storestr & -storecontainer   :  Azure Blob Storage Account connection string and container (if not, write to local fs)`)
        console.log(`   -continue                     :  Reset, clear the current run progress & start from begining`)
        console.log(`   -dir                          :  Starting point for ACL extraction (default "/")`)
        process.exit(1)
    }

    let argIdx = 2
    let nextparam
    let opts = []
    opts['concurrency'] = 128
    opts['continue'] = false
    opts['dir'] = "/"
    while (argIdx < process.argv.length) {
        switch (process.argv[argIdx]) {
            case '-storestr':
                nextparam = 'storeConnectionStr'
                break
            case '-storecontainer':
                nextparam = 'storeContainer'
                break
            case '-a':
                nextparam = 'accountName'
                break
            case '-c':
                nextparam = 'concurrency'
                break
            case '-f':
                nextparam = 'filesystemName'
                break
            case '-sas':
                nextparam = 'sas'
                break
            case '-key':
                nextparam = 'key'
                break
            case '-dir':
                nextparam = 'dir'
                break
            case '-continue':
                opts['continue'] = true
                break
            default:
                if (/^-/.test(process.argv[argIdx])) {
                    usage(`unknown argument ${process.argv[argIdx]}`)
                } else if (nextparam) {
                    opts[nextparam] = process.argv[argIdx]
                } else {
                    usage(`unknown argument ${process.argv[argIdx]}`)
                }
                nextparam = null
        }
        argIdx++
    }
    if (!(opts['accountName'] && opts['filesystemName'] && (opts['sas'] || opts['key']))) {
        usage()
    }

    if ((opts['storeConnectionStr'] && !opts['storeContainer']) || (opts['storeContainer'] && !opts['storeConnectionStr'])) {
        console.log('Need both -storestr & -storecontainer to output to Azure Storage ')
        usage()
    }
    return opts
}


async function main() {
    const opts = args()
    let opt: StoragePipelineOptions
    /*
        // https://github.com/Azure/azure-sdk-for-js/blob/52d621342a9094d7af0b38eed9476af1a451070d/sdk/storage/storage-file-datalake/src/Pipeline.ts#L157
        // https://azuresdkdocs.blob.core.windows.net/$web/javascript/azure-storage-file-datalake/12.3.1/interfaces/storagepipelineoptions.html
        setLogLevel('warning');
        opt = {
            userAgentOptions: {},
            keepAliveOptions: { enable: true },
            retryOptions: {
                retryPolicyType: StorageRetryPolicyType.FIXED,
                maxTries: 1
            },
            // https://github.com/Azure/azure-sdk-for-js/blob/52d621342a9094d7af0b38eed9476af1a451070d/sdk/core/core-http/src/nodeFetchHttpClient.ts
            httpClient: { }
        }
    */

    const accountName = opts['accountName'],
        fileSystemName = opts['filesystemName'],
        adlurl = `https://${accountName}.dfs.core.windows.net` + (opts['sas'] ? `?${opts['sas']}` : ''),
        serviceClient = new DataLakeServiceClient(adlurl, opts['key'] ? new StorageSharedKeyCredential(accountName, opts['key']) : undefined, opt)


    const db = level('./mydb')
    let azStore
    if (opts['storeConnectionStr'] && opts['storeContainer']) {
        azStore = new WriteAppendBlobs(db, opts['storeConnectionStr'], opts['storeContainer'])
    }

    //console.log("sas token" + serviceClient.generateAccountSasUrl(new Date(Date.now() + (3600 * 1000 * 24)), AccountSASPermissions.parse('rwlacup'), "s"))

    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)
    await writePathsRestartableConcurrent(db, fileSystemClient, opts['concurrency'], opts['dir'], opts['continue'], azStore)
}

main()