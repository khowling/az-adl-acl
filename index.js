const { DataLakeServiceClient,
    StorageSharedKeyCredential } = require("@azure/storage-file-datalake");
const fs = require('fs')

async function createFiles(fileSystemClient, seed, num) {
    let i = 0
    for (topdir of [...Array(num).keys()].map(n => `${seed}dirtop${n}`)) {

        for (middle of [...Array(num).keys()].map(n => `${seed}dirmiddle${n}`)) {

            for (file of [...Array(num).keys()].map(n => `${seed}file${n}`)) {
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


async function main() {

    const accountName = process.env.ACCOUNTNAME, fileSystemName = process.env.FILESYSTEMNAME, serviceClient = new DataLakeServiceClient(`https://${accountName}.dfs.core.windows.net` + process.env.SASKEY)

    const fileSystemClient = serviceClient.getFileSystemClient(fileSystemName);
    /* try {
        await fileSystemClient.create();
    } catch (e) {
        //console.log(e)
    }*/
    //await createFiles(fileSystemClient, 'five', 5)

    await fs.promises.writeFile('./paths.csv', 'isDirectory,Filepath,Path,Name,Owner,Group' + "\n")

    await fs.promises.writeFile('./acls.csv', 'Filepath,Type,Entity,Read,Write,Execute' + "\n")

    let iter = await fileSystemClient.listPaths({ path: "/", recursive: true });

    let file = 0
    let dir = 0
    for await (const path of iter) {
        if (path.isDirectory) dir++; else file++;
        process.stdout.cursorTo(0)
        process.stdout.write(`iter... file=${file} dir=${dir} `)
        const lastidx = path.name.lastIndexOf('/')

        await fs.promises.appendFile('./paths.csv', `${path.isDirectory},${path.name},${lastidx > 0 ? path.name.substr(0, lastidx) : ''},${lastidx > 0 ? path.name.substr(lastidx + 1) : path.name},${path.owner},${path.group}` + "\n")

        if (path.isDirectory) {
            const dpermissions = await fileSystemClient.getDirectoryClient(path.name).getAccessControl();
            dpermissions.acl.forEach(async p => {
                await fs.promises.appendFile('./acls.csv', `${path.name},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}` + "\n")
            })
        } else {
            const fpermissions = await fileSystemClient.getFileClient(path.name).getAccessControl();
            fpermissions.acl.forEach(async p => {
                await fs.promises.appendFile('./acls.csv', `${path.name},${p.accessControlType},${p.entityId},${p.permissions.read},${p.permissions.write},${p.permissions.execute}` + "\n")
            })

        }

    }
    console.log('done')

}

main()