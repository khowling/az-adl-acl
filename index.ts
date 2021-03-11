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

async function writePathHeader() {
    await fs.promises.writeFile('./paths.csv', 'isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
}

async function writePathline(path) {
    const lastidx = path.name.lastIndexOf('/')
    await fs.promises.appendFile('./paths.csv', `${path.isDirectory},${path.name},${lastidx > 0 ? path.name.substr(0, lastidx) : ''},${lastidx > 0 ? path.name.substr(lastidx + 1) : path.name},${path.owner},${path.group}` + "\n")
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
import { JobManager, JobStatus, JobReturn } from './jobmanager'

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


async function writePathsRestartableConcurrent(fileSystemClient: DataLakeFileSystemClient, startDir: string, reset: boolean) {

    var db = level('./mydb')

    const pathsdb = sub(db, 'paths', { valueEncoding: 'binary' })
    const paths_q = new JobManager("paths", db, 50, async function (seq: number, path: string): Promise<JobReturn> {
        try {
            const childPaths: Array<string> = []
            const paths = await fileSystemClient.listPaths({ path, recursive: false } as ListPathsOptions)

            let dirs = 0, files = 0
            for await (const path of paths) {
                if (path.isDirectory) {
                    dirs++
                    childPaths.push(path.name)
                } else {
                    files++
                }
                const val = pathType.toBuffer(path)
                await pathsdb.put(path.name, val)
            }
            return { seq, status: JobStatus.Success, metrics: { dirs, files }, newJobs: childPaths }
        } catch (e) {
            console.error(e)
            process.exit(1)
        }
    })

    await paths_q.start(reset)
    if (reset) {

        await pathsdb.clear()
        console.log(`Seeding path startDir=${startDir}`)
        await paths_q.submit(startDir)
    }
    await paths_q.finishedSubmitting()



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

    db.close(() => console.log('closing job manager'))
}

function args() {

    function usage(e?) {
        if (e) console.log(e)
        console.log('usage:')
        console.log(`    ${process.argv[0]} ${process.argv[1]} -a <ADL account name> -f <ADL filesystem name> [-sas <SAS> | -key <key> ] [-dir <starting directory] [-R]`)
        console.log(`         -R   :  Reset, clear the current run progress & start from begining`)
        console.log(`         -dir :  Starting point for ACL extraction (default "/")`)
        process.exit(1)
    }

    let argIdx = 2
    let nextparam
    let opts = []
    opts['reset'] = false
    opts['dir'] = "/"
    while (argIdx < process.argv.length) {
        switch (process.argv[argIdx]) {
            case '-a':
                nextparam = 'accountName'
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
            case '-R':
                opts['reset'] = true
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

    //console.log("sas token" + serviceClient.generateAccountSasUrl(new Date(Date.now() + (3600 * 1000 * 24)), AccountSASPermissions.parse('rwlacup'), "s"))

    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)
    await writePathsRestartableConcurrent(fileSystemClient, opts['dir'], opts['reset'])
}

main()