var sub = require('subleveldown')
import { Atomic } from './atomic'

const { Transform } = require('stream');

class MissingSequence extends Transform {

    private expected = 0
    private nextToRun

    constructor(nextToRun: number, options?) {
        super(options);
        this.nextToRun = nextToRun
    }

    _transform(chunk, encoding, cb) {
        const seq = parseInt(chunk)
        if (isNaN(seq)) {
            cb(new Error(`chunk ${chunk} is not a number`));
        } else if (seq < this.expected) {
            cb(new Error(`chunk ${seq} is not in sequence (expected ${this.expected})`));
        } else {
            //console.log(`_transform: seq=${seq} this.expected=${this.expected} this.nextToRun=${this.nextToRun}`)
            if (seq < this.nextToRun) {

                if (seq === this.expected) {
                    this.expected = seq + 1
                } else if (seq > this.expected) {
                    for (let i = this.expected; i < seq; i++) {
                        this.push(i.toString())
                    }
                    this.expected = seq + 1
                }
            }

            cb();
        }
    }

    _flush(cb) {
        //console.log(`_flush: this.expected=${this.expected} this.nextToRun=${this.nextToRun}`)
        if (this.expected < this.nextToRun) {
            for (let i = this.expected; i < this.nextToRun; i++) {
                //console.log(`_flush: pushing ${i}`)
                this.push(i.toString())
            }
        }
        cb();
    }
}

export interface JobData {
    task: JobTask,
    path: string,
    isDirectory: boolean
}

export enum JobTask {
    ListPaths,
    GetACLs
}

export interface JobReturn {
    status: JobStatus;
    metrics: {
        dirs: number;
        files: number;
        acls: number;
    };
    seq: number;
    // Has the Job queued holding jobs?
    holdingBatches?: number;
}

export enum JobStatus {
    Success,
    Error
}

const avro = require('avsc');
const queueJobData = avro.Type.forValue({
    task: 0,
    path: "/",
    isDirectory: true
})
const holdingJobData = avro.Type.forValue([
    {
        task: 0,
        path: "/",
        isDirectory: true
    },
    {
        task: 0,
        path: "/",
        isDirectory: true
    }
])

export class JobManager {

    private _name
    private _db

    // sub leveldbs
    // main job queue
    private _queue

    // holdingqueue - to allow the workerfn to write new jobs before returning.
    private _holdingqueue
    // record of completed tasks
    private _complete
    // control state
    private _control


    private _limit
    private _workerFn
    private _mutex
    private _finishedPromise
    private _nomore


    constructor(name: string, db, concurrency: number, workerfn: (seq: number, d: JobData) => Promise<JobReturn>) {
        this._name = name
        this._db = db
        this._limit = concurrency
        this._workerFn = workerfn
        this._mutex = new Atomic(1)
    }

    get complete() {
        return this._complete
    }

    async finishedSubmitting() {
        this._nomore = true
        return await this._finishedPromise
    }

    // Needed for levelDB sequence key order :()
    static inttoKey(i) {
        return ('0000000000' + i).slice(-10)
    }

    async start(reset: boolean = false) {
        // no job submissio processing
        let release = await this._mutex.aquire()

        this._nomore = false
        this._complete = sub(this._db, `${this._name}_complete`, { valueEncoding: 'binary' })
        this._queue = sub(this._db, `${this._name}_queue`, { valueEncoding: 'binary' })
        this._holdingqueue = sub(this._db, `${this._name}_holdingqueue`, { valueEncoding: 'binary' })
        this._control = sub(this._db, `${this._name}_control`, { valueEncoding: 'json' })

        if (reset) {
            console.log(`JobManager (${this._name}): Starting concurrency=${this._limit} (with reset)`)
            await this.complete.clear()
            await this._queue.clear()
            await this._holdingqueue.clear()

            await this._control.put(0, {
                nextSequence: 0,
                nextToRun: 0,
                numberCompleted: 0,
                holdingBatches: 0,
                metrics: {
                    dirs: 0,
                    files: 0,
                    acls: 0
                }
            })
        } else {
            const { nextSequence, nextToRun, numberCompleted } = await this._control.get(0)
            const running = await this._getRunning(nextToRun)
            console.log(`JobManager (${this._name}): continuing from :  nextToRun=${nextToRun}, numberCompleted=${numberCompleted}. Restarting running processes ${running.join(',')}...`)

            for (let runningseq of running) {
                await this.runit(runningseq, queueJobData.fromBuffer(await this._queue.get(runningseq)))
            }

        }
        release()

        this._finishedPromise = new Promise(resolve => {
            const interval = setInterval(async () => {
                const { nextSequence, nextToRun, numberCompleted, holdingBatches, metrics } = await this._control.get(0)
                const log = `(${this._name}) files=${metrics.files} dirs=${metrics.dirs} acls=${metrics.acls} (holdingBatches=${holdingBatches} nextSequence=${nextSequence} nextToRun=${nextToRun} numberCompleted=${numberCompleted})`
                if (!process.env.BACKGROUND) {
                    process.stdout.cursorTo(0); process.stdout.write(log)
                } else {
                    console.log(log)
                }
                if (nextSequence === numberCompleted && this._nomore) {
                    console.log()
                    clearInterval(interval)
                    resolve(true)
                }
            }, 1000)
        })
    }

    private _getRunning(nextToRun: number): Promise<Array<number>> {
        return new Promise((res, rej) => {

            let ret: Array<number> = []
            const p = this.complete.createKeyStream().pipe(new MissingSequence(nextToRun))

            p.on('data', (d) => {
                //console.log(`_getRunning ${parseInt(d)}`)
                ret.push(parseInt(d))
            })
            p.on('finish', () => res(ret))
            p.on('error', (e) => rej(e))

        })

    }


    private async checkRun(newWorkData?: JobData, completed?: JobReturn) {
        //console.log(`checkRun - aquire newWorkData=${newWorkData} completed=${JSON.stringify(completed)}`)
        let release = await this._mutex.aquire()
        let { nextSequence, nextToRun, numberCompleted, metrics, holdingBatches } = await this._control.get(0)

        let newWorkNext = newWorkData && nextToRun === nextSequence

        if (completed) {

            metrics = {
                dirs: metrics.dirs + completed.metrics.dirs,
                files: metrics.files + completed.metrics.files,
                acls: metrics.acls + completed.metrics.acls
            }

            if (completed.holdingBatches && completed.holdingBatches > 0) {
                // MOVE holding into _queue
                for (let i = 0; i < completed.holdingBatches; i++) {
                    const holdingkey = JobManager.inttoKey(completed.seq) + '-' + i
                    //console.log(`moving holdingBatches holdingkey=${holdingkey}...`)
                    const jobBatch = holdingJobData.fromBuffer(await this._holdingqueue.get(holdingkey))
                    await this._queue.batch(jobBatch.map(j => { return { type: 'put', key: nextSequence++, value: queueJobData.toBuffer(j) } }))
                    await this._holdingqueue.del(holdingkey)
                    holdingBatches--
                }
            }


            //console.log(`completed job : ${JSON.stringify(completed)}`)
            await this._complete.put(JobManager.inttoKey(completed.seq), "1")
            numberCompleted++


        }
        if (newWorkData) {
            await this._queue.put(nextSequence++, queueJobData.toBuffer(newWorkData))
        }

        // if we have NOT ran everything in the buffer && we are not at the maximum concurrency
        while (nextToRun < nextSequence && nextToRun - numberCompleted < this._limit) {
            this.runit(nextToRun, newWorkNext ? newWorkData : queueJobData.fromBuffer(await this._queue.get(nextToRun)))
            nextToRun++
        }

        await this._control.put(0, { nextSequence, nextToRun, numberCompleted, metrics, holdingBatches })
        //console.log(`checkRun - end: nextSequence=${nextSequence} nextToRun=${nextToRun} numberCompleted=${numberCompleted}`)
        release()

    }

    private runit(seq: number, data) {
        this._workerFn(seq, data).then(async (out) => {
            await this.checkRun(null, out)
        }, async (e) => {
            console.error(e)
            //await this.checkRun(null, true)
        })
    }

    async submit(data: JobData) {
        await this.checkRun(data)
    }

    async submitHolding(jobSequence: number, batchIdx: number, data: JobData[]) {

        if (data && data.length > 0) {
            let release = await this._mutex.aquire()

            let ctl = await this._control.get(0)
            ctl.holdingBatches = ctl.holdingBatches + 1
            await this._holdingqueue.put(JobManager.inttoKey(jobSequence) + '-' + batchIdx, holdingJobData.toBuffer(data))
            await this._control.put(0, ctl)

            release()
        }
    }
}
