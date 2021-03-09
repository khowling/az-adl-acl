var sub = require('subleveldown')
var level = require('level')





const { Writable } = require('stream');
import { Atomic } from './atomic'

class MyWritable extends Writable {
    constructor(options) {
        // Calls the stream.Writable() constructor.
        super(options);
        // ...
    }

    // All Writable stream implementations must provide a writable._write() and/or writable._writev() method to send data to the underlying resource.
    _write(chunk, encoding, callback) {
        //if (chunk.toString().indexOf('a') >= 0) {
        //  callback(new Error('chunk is invalid'));
        //} else {

        console.log(chunk.toString())

        callback();
        //}
    }

}

class PersistantJobQueue {

    private db
    private queue
    private control
    private limit
    private workerFn
    private mutex


    constructor(db, concurrency, workerfn) {
        this.db = db
        this.limit = concurrency
        this.workerFn = workerfn
        this.mutex = new Atomic(1)
    }

    async start(restart: boolean = false) {

        this.queue = sub(this.db, 'queue', { valueEncoding: 'json' })
        this.control = sub(this.db, 'control', { valueEncoding: 'json' })

        if (!restart) {
            await this.queue.clear()

            await this.control.put(0, {
                nextSequence: 0,
                nextToRun: 0,
                numberCompleted: 0
            })
        }

        setInterval(async () => {
            let { nextSequence, nextToRun, numberCompleted } = await this.control.get(0)
            console.log(`PersistantJobQueue nextSequence=${nextSequence} nextToRun=${nextToRun} numberCompleted=${numberCompleted}`)
        }, 1000)
    }



    async addWork(data) {

        const checkRunNext = (async function (newWorkData, incCompleted: boolean = false) {

            let release = await this.mutex.aquire()

            let { nextSequence, nextToRun, numberCompleted } = await this.control.get(0)

            let newWorkNext = newWorkData && nextToRun === nextSequence

            if (incCompleted) {
                numberCompleted++
            }
            if (newWorkData) {
                await this.queue.put(nextSequence, newWorkData)
                nextSequence++
            }

            // if we have NOT ran everything in the buffer && we are not at the maximum concurrency
            while (nextToRun < nextSequence && nextToRun - numberCompleted < this.limit) {
                if (newWorkNext) {
                    this.workerFn(newWorkData).then(donefn, errfn)
                    nextToRun++
                } else {
                    this.workerFn(await this.queue.get(nextToRun)).then(donefn, errfn)
                    nextToRun++
                }
            }

            await this.control.put(0, { nextSequence, nextToRun, numberCompleted })

            release()

        }).bind(this)

        function errfn(e) {
            console.error(e)
            checkRunNext(null, true)
        }

        function donefn() {
            checkRunNext(null, true)
        }

        checkRunNext(data)
    }
}

/*
async function main_old() {

    const jobqueue = sub(db, 'test', { valueEncoding: 'string' })
    await jobqueue.clear()

    const ws = new MyWritable({})
    jobqueue.createReadStream({ keys: true, values: false }).pipe(ws)

    for (let i = 0; i < 50; i++) {
        await jobqueue.put(`/${i}`, { status: 0 })
    }

    ws.once('finish', () => console.log('finish done'))


}
*/

async function main() {
    var db = level('./mydb')
    const q = new PersistantJobQueue(db, 5, (d) => {
        //console.log(`worker processing data=${d}`)
        return new Promise(resolve => setInterval(resolve, d))
    }
    )
    await q.start()

    for (let i = 0; i < 100; i++) {
        await q.addWork(1500 - (i * 10))
    }
}

main()