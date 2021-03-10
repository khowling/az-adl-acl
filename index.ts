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


const { Transform } = require('stream');
class PathsCSV extends Transform {
    private _donehead = false

    _transform(chunk, encoding, cb) {
        try {
            if (!this._donehead) {
                this.push('isDirectory,Filepath,Path,Name,Owner,Group' + "\n")
                this._donehead = true
            }
            const path = JSON.parse(chunk.toString('utf8'))
            //console.log(path)

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
            const a = JSON.parse(chunk.toString('utf8'))
            this.push(`${a.path},${a.accessControlType},${a.entityId},${a.permissions.read},${a.permissions.write},${a.permissions.execute}` + "\n")

            cb()
        } catch (e) {
            cb(new Error(`chunk ${chunk} is not a json: ${e}`));
        }
    }
}



const level = require('level')
var sub = require('subleveldown')
import { JobManager, JobStatus, JobReturn } from './jobmanager'


async function writePathsRestartableConcurrent(fileSystemClient: DataLakeFileSystemClient) {

    var db = level('./mydb')
    const reset = process.argv[2] === '-reset'

    const pathsdb = sub(db, 'paths', { valueEncoding: 'json' })
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
                await pathsdb.put(path.name, JSON.stringify(path))
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
        await paths_q.submit("/")
    }
    await paths_q.finishedSubmitting()



    const aclsdb = sub(db, 'acls', { valueEncoding: 'json' })
    const acls_q = new JobManager("acls", db, 50, async function (seq: number, path: string): Promise<JobReturn> {
        try {
            const p = JSON.parse(await pathsdb.get(path))

            let dirs = p.isDirectory ? 1 : 0, files = p.isDirectory ? 0 : 1
            const permissions = p.isDirectory ? await fileSystemClient.getDirectoryClient(path).getAccessControl() : await fileSystemClient.getFileClient(path).getAccessControl()
            for (let a of permissions.acl) {
                await aclsdb.put(`${path}${a.accessControlType}${a.entityId}`, JSON.stringify({ ...a, path }))
            }

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


    console.log('done, creating "paths.csc" file...')
    const pfile = pathsdb.createValueStream().pipe(new PathsCSV()).pipe(fs.createWriteStream('./paths.csv'))
    pfile.on('finish', () => {
        console.log('done')
    })

    console.log('creating "acls.csc" file...')
    const afile = aclsdb.createValueStream().pipe(new ACLsCSV()).pipe(fs.createWriteStream('./acls.csv'))
    afile.on('finish', () => {
        console.log('done')
        db.close(() => console.log('closing job manager'))
    })

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

    const filename = 'test'//process.argv[2]

    const accountName = process.env.ACCOUNTNAME,
        fileSystemName = process.env.FILESYSTEMNAME,
        adlurl = `https://${accountName}.dfs.core.windows.net` + (process.env.SASKEY ? `?${process.env.SASKEY}` : ''),
        serviceClient = new DataLakeServiceClient(adlurl, process.env.ADLKEY ? new StorageSharedKeyCredential(accountName, process.env.ADLKEY) : undefined, opt)

    //console.log("sas token" + serviceClient.generateAccountSasUrl(new Date(Date.now() + (3600 * 1000 * 24)), AccountSASPermissions.parse('rwlacup'), "s"))

    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName)


    if (true) {
        await writePathsRestartableConcurrent(fileSystemClient)
        //await writePathsRecurcive(fileSystemClient)
        //await writePaths(fileSystemClient)
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