
var lexint = require('lexicographic-integer');

import { assembleProtocol } from 'avsc/types';
import { Atomic } from './atomic'
import level, { LevelUp } from 'levelup'
import sub from 'subleveldown'
/*
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
*/

export interface ControlData {
    nextSequence: number;
    nextToRun: number;
    numberCompleted: number;
    numberRunning: number;
    metrics: Metrics;
}

export interface JobData {
    task: JobTask;
    path: string;
    isDirectory: boolean;
    completedBatches: number;
}

export enum JobTask { ListPaths, GetACLs }

export interface JobReturn {
    seq: number;
    status: JobStatus;
    updateJobData?: JobData;
    newJobs?: JobData[];
    metrics: Metrics;
}

export interface Metrics {
    dirs: number;
    files: number;
    acls: number;
    errors: number;
}

export enum JobStatus {
    Success,
    Error
}
/*
export interface JobRunningData {
    //status: JobRunningStatus;
    completedBatches: number;
}
export enum JobRunningStatus {
    Started,
    Restart
}
*/
const avro = require('avsc')
/*
const runningJobData = avro.Type.forValue({
    completedBatches: 1
})
*/
const queueJobData = avro.Type.forValue({
    task: JobTask.GetACLs,
    path: "/",
    isDirectory: true,
    completedBatches: 1
})

const assert = require('assert').strict;

export class JobManager {

    private _db

    // sub leveldbs
    // main job queue
    private _queue: LevelUp
    // Allow the workerfn to track progess before returning complete (for restarts to ensure not duplicating).
    //private _running

    // control state
    private _control: LevelUp


    private _limit
    private _workerFn
    private _mutex
    private _finishedPromise: Promise<boolean> | null
    private _nomore: boolean


    constructor(db: LevelUp, concurrency: number, workerfn: (seq: number, d: JobData/*, r: JobRunningData*/) => Promise<JobReturn>) {

        this._db = db
        this._limit = concurrency
        this._workerFn = workerfn
        this._mutex = new Atomic(1)
        this._finishedPromise = null
        this._nomore = false
        this._queue = sub(this._db, 'queue', {
            keyEncoding: {
                type: 'lexicographic-integer',
                encode: (n) => lexint.pack(n, 'hex'),
                decode: lexint.unpack,
                buffer: false
            },
            valueEncoding: 'binary'
        })
        /*
                this._running = sub(this._db, 'running', {
                    keyEncoding: {
                        type: 'lexicographic-integer',
                        encode: (n) => lexint.pack(n, 'hex'),
                        decode: lexint.unpack,
                        buffer: false
                    }, valueEncoding: 'binary'
                })
        */
        this._control = sub(this._db, 'control', { valueEncoding: 'json' })

    }

    async finishedSubmitting() {
        this._nomore = true
        return await this._finishedPromise
    }

    // Needed for levelDB sequence key order :()
    //static inttoKey(i) {
    //    return ('0000000000' + i).slice(-10)
    //}

    async start(continueRun: boolean = false) {
        // no job submissio processing
        let release = await this._mutex.aquire()

        const controlInit: ControlData = {
            nextSequence: 0,
            nextToRun: 0,
            numberCompleted: 0,
            numberRunning: 0,
            metrics: {
                dirs: 0,
                files: 0,
                acls: 0,
                errors: 0
            }
        }

        if (!continueRun) {
            console.log(`JobManager: Starting new run (concurrency=${this._limit})`)
            await this._queue.clear()
            //await this._running.clear()
            await this._control.put(0, controlInit)
        } else {

            const { nextSequence, numberCompleted, numberRunning, nextToRun } = await new Promise((res, rej) => {
                this._control.get(0, async (err: any, value) => {
                    if (err) {
                        if (err.notFound) {
                            console.error(`Cannot continue, no previous run found (make sure you are in the correct directory)`)
                            process.exit()
                        }
                    }
                    res(value)
                })
            })

            // Restart running processes
            console.log(`JobManager: Re-starting: queued=${nextSequence} running=${numberRunning} completed=${numberCompleted} nextToRun=${nextToRun}`)


            // Check there have been running processes
            if (nextToRun > 0) {

                const rkeys: number[] = await new Promise((res, rej) => {
                    let runningKeys: number[] = []
                    // Everything in the _queue with a sequence# < nextToRun should be running (all completed will have been deleted)
                    const feed = this._queue.createKeyStream({ lt: nextToRun }).on('data', d => {
                        runningKeys.push(d)
                    }).on('end', () => {
                        res(runningKeys)
                    })
                })

                //assert.deepStrictEqual(rkeys.length, numberRunning, `JobManager: restarting running jobs error expeccted numberRunning=${numberRunning}, got ${rkeys.length} in queue`)

                if (rkeys.length > 0) {
                    console.log(`JobManager: Restarting : ${rkeys.join(',')}...`)
                    for (let key of rkeys) {
                        await this.runit(key, queueJobData.fromBuffer(await this._queue.get(key))/*, runningJobData.fromBuffer(await this._running.get(key))*/)
                    }
                }
            }

        }
        release()

        this._finishedPromise = new Promise(resolve => {
            const interval = setInterval(async () => {
                const { nextSequence, nextToRun, numberCompleted, numberRunning, metrics } = await this._control.get(0)
                const log = `files=${metrics.files} dirs=${metrics.dirs} acls=${metrics.acls} errors=${metrics.errors} (queued=${nextSequence} running=${numberRunning} completed=${numberCompleted})`
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
    /*
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
    */

    private static addMetrics(base: Metrics, addit: Metrics): Metrics {
        assert.ok(base, "No Metrics provided in Job return, program error")
        if (!addit) return base
        return {
            files: addit.files + base.files,
            dirs: addit.dirs + base.dirs,
            acls: addit.acls + base.acls,
            errors: addit.errors + base.errors,
        }
    }

    private async checkRun(newData?: JobData | null, batchData?: JobReturn | null, completed?: JobReturn) {
        //console.log(`checkRun - aquire newData=${newData} completed=${JSON.stringify(completed)}`)

        let release = await this._mutex.aquire()
        let { nextSequence, nextToRun, numberCompleted, numberRunning, metrics } = await this._control.get(0)

        let newWorkNext = false
        if (newData) {
            newWorkNext = nextToRun === nextSequence
            await this._queue.put(nextSequence++, queueJobData.toBuffer(newData))
        }

        if (batchData) {
            metrics = JobManager.addMetrics(metrics, batchData.metrics)

            // update job data in _queue, so in restart will not re-run completed batches
            if (batchData.updateJobData) {
                await this._queue.put(batchData.seq, queueJobData.toBuffer(batchData.updateJobData))
            }
            // add the newJobs to the _queue
            if (batchData.newJobs && batchData.newJobs.length > 0) {
                await this._queue.batch(batchData.newJobs.map(j => { return { type: 'put', key: nextSequence++, value: queueJobData.toBuffer(j) } }))
            }
        }

        if (completed) {
            metrics = JobManager.addMetrics(metrics, completed.metrics)
            //await this._running.del(completed.seq)
            await this._queue.del(completed.seq)
            numberRunning--
            numberCompleted++
        }

        // if we have NOT ran everything in the buffer && we are not at the maximum concurrency
        while (nextToRun < nextSequence && nextToRun - numberCompleted < this._limit) {
            //const runningData: JobRunningData = { /*status: JobRunningStatus.Started,*/ completedBatches: 0 }
            //await this._running.put(nextToRun, runningJobData.toBuffer(runningData))
            numberRunning++
            this.runit(nextToRun, newWorkNext ? newData : queueJobData.fromBuffer(await this._queue.get(nextToRun))/*, runningData*/)
            nextToRun++
        }

        await this._control.put(0, { nextSequence, nextToRun, numberCompleted, numberRunning, metrics })
        //console.log(`checkRun - end: nextSequence=${nextSequence} nextToRun=${nextToRun} numberCompleted=${numberCompleted}`)
        release()

    }

    private runit(seq: number, data: JobData/*, runningData: JobRunningData*/) {
        this._workerFn(seq, data/*, runningData*/).then(async (value) => {
            await this.checkRun(null, null, value)
        }, async (e) => {
            console.error(e)
            //await this.checkRun(null, true)
        })
    }

    async submit(data: JobData) {
        await this.checkRun(data)
    }

    async runningCompleteBatch(value: JobReturn) {
        await this.checkRun(null, value)
    }

}
