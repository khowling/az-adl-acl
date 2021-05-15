const fs = require('fs')
var lexint = require('lexicographic-integer');

import { Atomic } from './atomic'
import {
    DataLakeServiceClient,
    DataLakeFileSystemClient,
    StoragePipelineOptions,
    StorageSharedKeyCredential,
    PathPermissions
} from "@azure/storage-file-datalake"

import {
    BlobServiceClient,
    ContainerClient,
    AppendBlobClient
} from '@azure/storage-blob'

import { setLogLevel } from '@azure/logger'

import level, { LevelUp } from 'levelup'
var leveldown = require('leveldown')
import sub from 'subleveldown'

import { JobManager, JobStatus, JobReturn, JobData, JobTask } from './jobmanager'

// Manage writing output to Azure Blob
class WriteAppendBlobs {

    private _useCache: boolean
    private _db: LevelUp
    private blobs: LevelUp
    private containerClient: ContainerClient
    private cachedClients: { [key: string]: AppendBlobClient } = {}
    private cachedContent: { [key: string]: LevelUp } = {}
    private _mutexs: { [key: string]: Atomic } = {}

    private static APPEND_BLOCK_SIZE = 4000000 // 4194304

    constructor(db: LevelUp, connectionStr: string, container: string) {
        this._useCache = true
        this._db = db
        this.blobs = sub(db, 'blobs', { valueEncoding: 'json' })
        const blobServiceClient = BlobServiceClient.fromConnectionString(connectionStr);
        this.containerClient = blobServiceClient.getContainerClient(container)
    }

    // if no body, just flush
    async write(blob: string, body?: string): Promise<void> {

        const length = body ? body.length : 0

        let release = await this._mutexs[blob].aquire()

        let { cacheSeq, cacheSize, blobNumber, blockNumber/*, blockSize*/, path } = await this.blobs.get(blob)

        // write to azure block, if using cache and cachesize > APPEND_BLOCK_SIZE, or flushing cache (length ===0), or no cache
        if (cacheSize + length > WriteAppendBlobs.APPEND_BLOCK_SIZE || length === 0 || !this._useCache) {

            // Each blockNumber in an append blob can be a different size, up to a maximum of 4 MB, and an append blob can include up to 50,000 blocks. 
            if (blockNumber > 49990 /*|| blockSize + length > 150000000000*/) {
                // current Blob is at maximum size, so create a new blob client
                blobNumber++; blockNumber = 0; /* blockSize = length*/
                this.cachedClients[blob] = this.containerClient.getAppendBlobClient(`${path}.${blobNumber}.csv`)
                await this.cachedClients[blob].createIfNotExists()
            } else {
                // just return cached client.
                blockNumber = blockNumber + 1; /*blockSize = blockSize + length*/
                if (!this.cachedClients[blob]) {
                    this.cachedClients[blob] = this.containerClient.getAppendBlobClient(`${path}.${blobNumber}.csv`)
                    await this.cachedClients[blob].createIfNotExists()
                }
            }

            // got blob client to write to
            const writestr: string | undefined = this._useCache ?
                await new Promise((res, rej) => {
                    const buff: string[] = []
                    this.cachedContent[blob].createValueStream({ lt: cacheSeq })
                        .on('data', d => buff.push(d))
                        .on('end', () => {
                            res(buff.join(''))
                        })
                }) : body

            if (writestr) {
                this.cachedClients[blob].appendBlock(writestr, writestr.length)
                cacheSeq = 0; cacheSize = 0
            }
        }

        // add body to cache
        if (this._useCache && length > 0) {
            await this.cachedContent[blob].put(cacheSeq++, body)
            cacheSize = cacheSize + length
        }

        await this.blobs.put(blob, { cacheSeq, cacheSize, blobNumber, blockNumber/*, blockSize*/, path })
        release()
    }

    async init(blobs: string[], useCache: boolean, continueRun: boolean) {
        this._useCache = useCache

        for (const b of blobs) {
            this._mutexs[b] = new Atomic(1)
            if (this._useCache) {
                this.cachedContent[b] = sub(this._db, `cache${b}`, {
                    keyEncoding: {
                        type: 'lexicographic-integer',
                        encode: (n) => lexint.pack(n, 'hex'),
                        decode: lexint.unpack,
                        buffer: false
                    },
                    valueEncoding: 'utf8'
                })
            }
        }

        if (!continueRun) {
            const d = new Date()
            for (const b of blobs) {
                await this.blobs.put(b, { cacheSeq: 0, cacheSize: 0, blobNumber: 0, blockNumber: 0, blockSize: 0, path: `${d.toISOString()}/${b}` })
            }
        }
    }
}


const PATH_HEADER = 'isDirectory,Name,Owner,Group,OnwerPermissions,GroupPermissions,ExecutePermissions' + "\n"
const ACL_HEADER = 'Filepath,DefaultScope,Type,Entity,Read,Write,Execute' + "\n"
const ERR_HEADER = 'TaskSequence,TaskType,Path,Error' + "\n"

// Main Logic to call and process results from the DataLake API
async function writePathsRestartableConcurrent(db: LevelUp, fileSystemClient: DataLakeFileSystemClient, concurrency: number, startDir: string, continueRun: boolean, azBlob?: WriteAppendBlobs) {

    if (azBlob) {
        await azBlob.init(['paths', 'acls', 'errors'], true, continueRun)
    }

    if (!continueRun) {
        if (azBlob) {
            await azBlob.write('paths', PATH_HEADER)
            await azBlob.write('acls', ACL_HEADER)
            await azBlob.write('errors', ERR_HEADER)
        } else {
            await fs.promises.writeFile('./paths.csv', PATH_HEADER)
            await fs.promises.writeFile('./acls.csv', ACL_HEADER)
            await fs.promises.writeFile('./errors.csv', ERR_HEADER)
        }
    }


    const paths_q = new JobManager(db, concurrency, async function (seq: number, d: JobData): Promise<JobReturn> {
        let batchCompleteIdx = 0
        let metrics = { dirs: 0, files: 0, acls: 0, errors: 0 }
        const status = JobStatus.Success
        try {
            if (d.task === JobTask.ListPaths) {
                for await (const response of fileSystemClient.listPaths({ path: d.path, recursive: false }).byPage({ maxPageSize: 10000 })) {
                    if (response.pathItems) {

                        let newJobs: Array<JobData> = []
                        for (const path of response.pathItems) {

                            newJobs.push({ task: JobTask.GetACLs, path: path.name as string, isDirectory: path.isDirectory as boolean, completedBatches: 0 })
                            if (path.isDirectory) {
                                metrics.dirs++
                                newJobs.push({ task: JobTask.ListPaths, path: path.name as string, isDirectory: path.isDirectory, completedBatches: 0 })
                            } else {
                                metrics.files++
                            }
                        }

                        if (batchCompleteIdx >= d.completedBatches) {

                            const body = response.pathItems.map(path => {
                                const { owner, group, other } = path.permissions as PathPermissions
                                return `${path.isDirectory},"${path.name}",${path.owner},${path.group},${owner.read ? 'r' : '-'}${owner.write ? 'w' : '-'}${owner.execute ? 'x' : '-'},${group.read ? 'r' : '-'}${group.write ? 'w' : '-'}${group.execute ? 'x' : '-'},${other.read ? 'r' : '-'}${other.write ? 'w' : '-'}${other.execute ? 'x' : '-'}`
                            }).join("\n") + "\n"

                            if (azBlob) {
                                await azBlob.write('paths', body)
                            } else {
                                await fs.promises.appendFile('./paths.csv', body)
                            }

                            await paths_q.runningCompleteBatch({ seq, updateJobData: { ...d, completedBatches: batchCompleteIdx + 1 } as JobData, status, metrics, newJobs })

                        }
                        metrics = { dirs: 0, files: 0, acls: 0, errors: 0 }
                        batchCompleteIdx++

                    }
                }
                return { seq, status, metrics }

            } else if (d.task === JobTask.GetACLs) {

                const permissions = d.isDirectory ? await fileSystemClient.getDirectoryClient(d.path).getAccessControl(/*{ userPrincipalName: true }*/) : await fileSystemClient.getFileClient(d.path).getAccessControl(/*{ userPrincipalName: true }*/)
                metrics.acls = permissions.acl.length

                const body = permissions.acl.map(p => `"${d.path}",${p.defaultScope},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}`).join("\n") + "\n"

                if (azBlob) {
                    await azBlob.write('acls', body)
                } else {
                    await fs.promises.appendFile('./acls.csv', body)
                }
                return { seq, status, metrics }
            } else {
                throw new Error(`Unknown JobTask ${d.task}, program error`)
            }

        } catch (err) {
            const body = `${seq},${d.task},${d.path},${JSON.stringify(err)}` + "\n"
            if (azBlob) {
                await azBlob.write('errors', body)
            } else {
                await fs.promises.appendFile('./errors.csv', body)
            }
            console.error(`Job Error: seq=${seq}, task=${d.task}, err=${JSON.stringify(err)}`)
            metrics.errors++
            return { seq, status: JobStatus.Error, metrics }
        }
    })

    await paths_q.start(continueRun)
    if (!continueRun) {

        //await pathsdb.clear()
        console.log(`Seeding path startDir=${startDir}`)
        await paths_q.submit({ task: JobTask.ListPaths, path: startDir, isDirectory: true, completedBatches: 0 })
    }
    await paths_q.finishedSubmitting()

    // flushing cached data to blobs
    if (azBlob) {
        await azBlob.write('paths')
        await azBlob.write('acls')
        await azBlob.write('errors')
    }

    db.close(() => console.log('closing job manager'))
}

type ProgramArgKeys = 'concurrency' | 'continue' | 'dir' | 'storeConnectionStr' | 'storeContainer' | 'accountName' | 'filesystemName' | 'sas' | 'key';

function args() {

    function usage(e?: string) {
        if (e) console.log(e)
        console.log('usage:')
        console.log(`    ${process.argv[0]} ${process.argv[1]} -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ]  [ -storestr <connection str>  -storecontainer <blob container> ] [-dir <starting directory] [-continue]`)
        console.log('')
        console.log(`   -storestr & -storecontainer   :  Azure Blob Storage Account connection string and container (if not, write to local fs)`)
        console.log(`   -continue                     :  Reset, clear the current run progress & start from begining`)
        console.log(`   -dir                          :  Starting point for ACL extraction (default "/")`)
        process.exit(1)
    }

    let opts: { [index in ProgramArgKeys]?: string } = {
        concurrency: '128',
        continue: 'false',
        dir: '/'
    }

    let argIdx: number = 2, nextparam: ProgramArgKeys | null = null
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
                opts['continue'] = 'true'
                break
            default:
                const value = process.argv[argIdx]
                if (/^-/.test(value)) {
                    usage(`unknown argument ${value}`)
                } else if (nextparam) {
                    if ('concurrency' === nextparam && isNaN(value as any)) {
                        usage(`-c requires a number`)
                    }
                    opts[nextparam] = value
                } else {
                    usage(`unknown argument ${value}`)
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

    const accountName = opts['accountName'] as string,
        fileSystemName = opts['filesystemName'] as string,
        adlurl = `https://${accountName}.dfs.core.windows.net` + (opts['sas'] ? `?${opts['sas']}` : ''),
        serviceClient = new DataLakeServiceClient(adlurl, opts['key'] ? new StorageSharedKeyCredential(accountName, opts['key']) : undefined /*, opt*/)


    const db = level(leveldown('./mydb'))
    let azStore
    if (opts['storeConnectionStr'] && opts['storeContainer']) {
        azStore = new WriteAppendBlobs(db, opts['storeConnectionStr'], opts['storeContainer'])
    }
    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)
    await writePathsRestartableConcurrent(db, fileSystemClient, Number(opts['concurrency']), opts['dir'] as string, opts['continue'] === 'true', azStore)
}

main()