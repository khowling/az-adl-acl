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


export interface JobReturn {
    status: JobStatus;
    seq: number;
    payload: any;
    newJobs: Array<any>;
}

export enum JobStatus {
    Success,
    Error
}

export class JobManager {

    private _db
    private _queue
    private _complete
    private _control
    private _limit
    private _workerFn
    private _mutex
    private _finishedPromise
    private _nomore


    constructor(db, concurrency: number, workerfn: (seq: number, d: string) => Promise<JobReturn>) {
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
        this._complete = sub(this._db, 'complete', { valueEncoding: 'utf8' })
        this._queue = sub(this._db, 'queue', { valueEncoding: 'json' })
        this._control = sub(this._db, 'control', { valueEncoding: 'json' })

        if (reset) {
            console.log(`JobManager: Start RESET`)
            await this.complete.clear()
            await this._queue.clear()

            await this._control.put(0, {
                nextSequence: 0,
                nextToRun: 0,
                numberCompleted: 0
            })
        } else {
            const { nextSequence, nextToRun, numberCompleted } = await this._control.get(0)
            const running = await this._getRunning(nextToRun)
            console.log(`JobManager: Start nextToRun=${nextToRun}, numberCompleted=${numberCompleted}. Restarting running processes ${running.join(',')}...`)


            for (let runningseq of running) {
                await this.runit(runningseq, await this._queue.get(runningseq))
            }

        }
        release()

        this._finishedPromise = new Promise(resolve => {
            const interval = setInterval(async () => {
                const { nextSequence, nextToRun, numberCompleted } = await this._control.get(0)
                console.log(`PersistantJobQueue nextSequence=${nextSequence} nextToRun=${nextToRun} numberCompleted=${numberCompleted}`)
                if (nextSequence === numberCompleted && this._nomore) {
                    console.log(`JobManager: closing`)
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


    private async checkRun(newWorkData?, completed?: JobReturn) {
        //console.log(`checkRun - aquire newWorkData=${newWorkData} completed=${JSON.stringify(completed)}`)
        let release = await this._mutex.aquire()
        let { nextSequence, nextToRun, numberCompleted } = await this._control.get(0)

        let newWorkNext = newWorkData && nextToRun === nextSequence

        if (completed) {
            //console.log(`completed job : ${JSON.stringify(completed)}`)
            await this._complete.put(JobManager.inttoKey(completed.seq), JSON.stringify({ status: completed.status, payload: completed.payload }))
            numberCompleted++
            if (completed.newJobs && completed.newJobs.length > 0) {
                await this._queue.batch(completed.newJobs.map(f => { return { type: 'put', key: nextSequence++, value: f } }))
            }
        }
        if (newWorkData) {
            await this._queue.put(nextSequence++, newWorkData)
        }

        // if we have NOT ran everything in the buffer && we are not at the maximum concurrency
        while (nextToRun < nextSequence && nextToRun - numberCompleted < this._limit) {
            this.runit(nextToRun, newWorkNext ? newWorkData : await this._queue.get(nextToRun))
            nextToRun++
        }

        await this._control.put(0, { nextSequence, nextToRun, numberCompleted })
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

    async submit(data) {
        await this.checkRun(data)
    }
}
