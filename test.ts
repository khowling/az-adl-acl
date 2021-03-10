
const level = require('level')
const fs = require('fs')

import { JobManager, JobStatus, JobReturn } from './jobmanager'


const { Transform } = require('stream');
class ToCSV extends Transform {

    _transform(chunk, encoding, cb) {
        try {
            const out: JobReturn = JSON.parse(chunk.toString('utf8'))
            if (out.status === JobStatus.Success) {
                this.push(`${out.payload},ok` + "\n")
            }
            cb()
        } catch (e) {
            cb(new Error(`chunk ${chunk} is not a json: ${e}`));
        }
    }
}



function processData(subdb, filename): Promise<Array<number>> {
    return new Promise((res, rej) => {

        let ret: Array<number> = []
        const p = subdb
        p.on('data', (d) => {
            //console.log(`_getRunning ${parseInt(d)}`)
            ret.push(parseInt(d))
        })
        p.on('finish', () => res(ret))
        p.on('error', (e) => rej(e))

    })

}


async function main() {
    var db = level('./mydb')

    async function worker(seq: number, d: string): Promise<JobReturn> {
        //console.log(`worker processing seq=${seq} data=${d}`)
        const payload = `${seq},${d}`
        const status = JobStatus.Success

        await new Promise(resolve => setTimeout(resolve, 400))
        //console.log(`worker processing done seq=${seq} data=${d}`)
        return { seq, status, payload, newJobs: null }
    }


    const q = new JobManager(db, 5, worker)

    const reset = process.argv[2] === '-reset'
    await q.start(reset)

    if (reset) {
        for (let i = 0; i < 100; i++) {
            await q.submit(`/d${i}`)
        }
    }

    //console.log('main - finishedSubmitting')
    await q.finishedSubmitting()

    const s = q.complete.createValueStream().pipe(new ToCSV()).pipe(fs.createWriteStream('./testout.csv'))
    s.on('finish', () => {
        console.log('done')
        db.close(() => console.log('leveldb closed'))
    })


    //console.log('main - end')

    //s.on('finish', () => console.log('done'))
    //await processData(q.complete, './testout.csv')

}

main()