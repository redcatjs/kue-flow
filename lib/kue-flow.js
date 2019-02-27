const Queue = require('./kue')
const Job = require('./queue/job')

const cp = require('child_process')
const fkill = require('fkill')
const debugFactory = require('debug')

const debug = debugFactory('kue-flow')

Object.assign(Queue.prototype, {
  initFlow({
    MACHINE_ID,
    maxFailedTime,
    maxActiveTime,
    maxCompleteTime,
    maxJobs,
    batchSize,
  }){
    this.MACHINE_ID = MACHINE_ID

    this.handleKillFlow()
    this.handleNextJob()

    //cleaner
    this.maxFailedTime = maxFailedTime
    this.maxActiveTime = maxActiveTime
    this.maxCompleteTime = maxCompleteTime
    this.maxJobs = maxJobs
    this.batchSize = batchSize

  },
  handleKillFlow(){
    this.waitingKill = {}

    const processKillKey = this.processKillKeyNS()
    const processKilledKey = this.processKilledKeyNS()
    const subscriber = this.redis.createClient()

    subscriber.on('message', async (channel, message)=>{
      switch(channel){
        case processKillKey: {
          const [ mid, pid ] = this.extractMPid(message)
          if(!this.checkMachineId(mid)) return
          let killed,
              error
          try{
            await fkill(pid)
            killed = true
          }
          catch(e){
            killed = false
            error = e
          }
          const killedMessage = {
            killed,
            error,
            mid,
            pid,
          }
          this.client.publish(processKilledKey, JSON.stringify(killedMessage) )
          break
        }
        case processKilledKey: {
          const {
            mid,
            pid,
            killed,
            error
          } = JSON.parse(message)
          if(!this.checkMachineId(mid)) return
          const mpid = [ mid, pid ].join('|')
          if(this.waitingKill[mpid] !== undefined){
            this.waitingKill[mpid](killed, error)
            delete this.waitingKill[mpid]
          }
          break
        }
      }
    })
    subscriber.subscribe(processKillKey)
    subscriber.subscribe(processKilledKey)
  },
  extractMPid(mpid){
    let [ mid, pid ] = mpid.split('|')
    pid = parseInt(pid)
    return [ mid, pid ]
  },
  checkMachineId(mid){
    return this.MACHINE_ID===mid
  },
  handleNextJob(){
    this.on('job complete', async (id, result) => {

      const job = await this.getJobAsync(id)

      const { _nextJob } = job.data

      if(_nextJob){
        let nextJobs = _nextJob
        if(!nextJobs instanceof Array){
          nextJobs = [ nextJobs ]
        }
        nextJobs.forEach(nextJobVars=>{
          const nextJob = this.createJob()
          Object.entries(nextJobVars).forEach(([key, value])=>{
            nextJob[key] = value
          })
          nextJob.save()
        })
      }

    })
  },
  processKillKeyNS(){
    return this.client.getKey('processkill')
  },
  processKilledKeyNS(){
    return this.client.getKey('processkilled')
  },
  /*
  machineKeyNS(prefix){
    return this.client.getKey([prefix, this.MACHINE_ID].join(':'))
  },
  */
  originalProcess: Queue.prototype.process,
  getJob(id, fn){
    this.Job.get(id, fn)
  },
  getJobAsync(id){
    return new Promise((resolve, reject)=>{
      this.getJob(id, function(err, result){
        if(err){
          reject(err)
        }
        else{
          resolve(result)
        }
      })
    })
  },
  uniqueMPidKeyNS(){
    if(!this._uniqueMPidKeyNS){
      this._uniqueMPidKeyNS = this.client.getKey('unique:mpids')
    }
    return this._uniqueMPidKeyNS
  },
  process(...args){
    let taskName,
        parallel,
        processor

    if(args.length===3){
      taskName = args[0]
      parallel = args[1]
      processor = args[2]
    }
    else{
      taskName = args[0]
      processor = args[1]
      parallel = processor.parallel || 1
    }
    return this.originalProcess(taskName, parallel, async function(job, ctx, done){

      let result,
          err = null

      try{
        result = await processor(job, ctx)
      }
      catch(e){
        err = e
      }

      done(err, result)

    })
  },
  _buildIntercaptableProcessor(processor){
    return (job, ctx) => {

      const { data } = job

      if(data._uniqueKillActive){

        const modulePath = 'build/task'
        const moduleArgs = [ job.id ]

        return new Promise((resolve, reject)=>{

          const subprocess = cp.fork(modulePath, moduleArgs)

          let gotMessage

          subprocess.on('message', (msg)=>{
            gotMessage = true
            if(msg.error){
              reject(msg.error)
            }
            else{
              resolve(msg.result)
            }
          })
          subprocess.on('exit', (code)=>{
            if(!gotMessage || code !== 0){
              reject({ code })
            }
            this.client.hdelAsync(this.uniqueMPidKeyNS(), data._unique)
          })

          const { pid } = subprocess
          const mpid = [ this.MACHINE_ID, pid ].join('|')
          this.client.hsetAsync(this.uniqueMPidKeyNS(), data._unique, mpid)

        })

      }
      else{

        return processor(job, ctx)

      }

    }
  },
  async processAsync(...args){

    let taskName,
        parallel,
        processor

    if(args.length===3){
      taskName = args[0]
      parallel = args[1]
      processor = args[2]
    }
    else{
      taskName = args[0]
      processor = args[1]
      parallel = processor.parallel || 1
    }

    const interceptableProcessor = this._buildIntercaptableProcessor(processor)

    this.originalProcess(taskName, parallel, async function(job, ctx, done){

      let result,
          err = null

      try{
        result = await interceptableProcessor(job, ctx)
      }
      catch(e){
        err = e
      }

      done(err, result)

    })

  },
  killMPid(mpid){
    const wait = new Promise((resolve, reject)=>{
      this.waitingKill[mpid] = (killed, error)=>{
        if(killed){
          resolve()
        }
        else{
          reject(error)
        }
      }
    })
    this.client.publish(this.processKillKeyNS(), mpid)
    return wait
  },

  //cleaner from refactored kue-concierge
  isJobExpired(job, state) {
    const now = new Date()
    const created = new Date(parseInt(job.updated_at))
    const age = parseInt(now - created)
    let maxTime
    switch(state) {
      case 'failed':
        maxTime = this.maxFailedTime
        break
      case 'active':
        maxTime = this.maxActiveTime
        break
      case 'complete':
        maxTime = this.maxCompleteTime
        break
    }
    return age > maxTime
  },

  restartStuck() {
    return this.Job.rangeByState('active', 0, this.batchSize, 'asc')
      .then((jobs)=>{
        jobs.forEach((job)=>{
          if (this.isJobExpired(job,'active')) {
            job.log('Restarting job due to inactivity')
            debug('Restarting job ' + job.id + ' due to inactivity')
            job.inactive()
          }
        })
      })
  },

  clearExpiredByType(status) {
    return this.Job.rangeByState(status, 0, this.batchSize, 'asc')
      .then((jobs)=>{
        jobs.forEach((job)=>{
          if (this.isJobExpired(job, status)) {
            debug('Removing job ' + job.id + ' (' + status + ' expired)')
            job.remove()
          }
        })
      })
  },

  _getCountMethod(status) {
    switch (status) {
      case 'complete':
        return this.completeCount()
      case 'failed':
        return this.failedCount()
      case 'active':
        return this.activeCount()
      case 'inactive':
        return this.inactiveCount()

    }
  },

  countJobs(status,threshold) {
    threshold = threshold || 0
    const countJobs = this._getCountMethod(status)
    return countJobs.then(function(count) {
      if (count > threshold) {
        return count - threshold
      } else {
        return 0
      }
    })
  },

  removeBatch(status, batchSize) {
    if (batchSize === 0) {
      return
    }
    return this.Job.rangeByState(status, 0, batchSize, 'asc')
      .then((jobs)=>{
        jobs.forEach((job)=>{
          debug('Removing job ' + job.id + ' (' + status + ')')
          job.remove()
        })
      })
  },

  clearAllByType(status, threshold) {
    return this.countJobs(status, threshold)
      .then((count)=>{
        const batchSize = Math.min(count, this.batchSize)
        return this.removeBatch(status, batchSize)
      })
      .then(()=>{
        return this.countJobs(status, threshold)
      })
      .then((count)=>{
        if (count > 0) {
          return this.clearAllByType(status, threshold)
        } else {
          return
        }
      })
  },

})

Object.assign(Queue.prototype, {
  originalQueueCreateJob: Queue.prototype.createJob,
  createJob(type, data, options = {}){
    const job = this.originalQueueCreateJob(type, data)
    job.setOptions(options)
    return job
  }
})

function overrideJob(Job){
  Object.assign(Job.prototype, {
    originalSave: Job.prototype.save,
    originalRemove: Job.prototype.remove,
    originalProgress: Job.prototype.progress,
    setOptions(options){
      const {
        priority,

        unique,
        uniqueOnce,
        uniqueKillActive,
        uniqueReplace,
        uniqueGlobal,

        nextJob,

      } = options

      if(priority){
        this.priority(priority)
      }

      if(unique){
        this.unique({
          key: unique,
          once: uniqueOnce,
          killActive: uniqueKillActive,
          replace: uniqueReplace,
          global: uniqueGlobal,
        })
      }

      if(nextJob){
        this.nextJob(nextJob)
      }

      return this
    },
    async originalRemoveAsync(id) {
      return new Promise((resolve, reject)=>{
        this.originalRemove(function(err, job){
          if(err){
            reject(err)
          }
          else{
            resolve(job)
          }
        })
      })
    },
    uniqueKeyNS(){
      if(!this._uniqueKeyNS){
        this._uniqueKeyNS = this.client.getKey('unique:jobs')
      }
      return this._uniqueKeyNS
    },
    unique(options = {}, arg2){
      if(typeof options === 'string'){
        const key = options
        options = {
          key,
        }
        if(arg2){
          options = { ...arg2, ...options }
        }
      }

      const {
        key,
        once,
        killActive,
        replace,
        global,
      } = options

      this.uniqueKey(key, global)
      this.uniqueOnce(once)
      this.uniqueKillActive(killActive)
      this.uniqueReplace(replace)

      return this
    },
    uniqueKey(key, global){
      if(global === undefined){
        global = this._uniqueGlobal
      }
      const uniqueKey = global ? key : [this.type, key].join('|')
      this.data._unique = uniqueKey
    },
    uniqueOnce(once){
      if(once !== undefined){
        this._uniqueOnce = once
      }
      return this._uniqueOnce
    },
    uniqueKillActive(killActive){
      if(killActive !== undefined){
        this.data._uniqueKillActive = killActive
      }
      return this.data._uniqueKillActive
    },
    uniqueReplace(replace){
      if(replace !== undefined){
        this._uniqueReplace = replace
      }
      return this._uniqueReplace
    },
    uniqueGlobal(global){
      if(global){
        this._uniqueGlobal = global
      }
      return this._uniqueGlobal
    },
    save(done) {
      this.saveAsync()
        .then(result=>done && done(null, result))
        .catch(err=>done && done(err))
      return this
    },
    async getAsync(id) {
      return new Promise((resolve, reject)=>{
        this.get(id, function(err, job){
          if(err){
            reject(err)
          }
          else{
            resolve(job)
          }
        })
      })
    },
    async removeAsync(id) {
      return new Promise((resolve, reject)=>{
        this.remove(function(err, job){
          if(err){
            reject(err)
          }
          else{
            resolve(job)
          }
        })
      })
    },
    async saveAsync() {
      let job = this
      if (this.data._unique) {
        const id = await this.getUniqueJobId()
        if (id) {
          job = await this.getAsync(id)
          if (job) {
            job.alreadyExist = true

            const {
              _uniqueReplace: uniqueReplace,
              _uniqueOnce: uniqueOnce,
            } = this

            const {
              _uniqueKillActive: uniqueKillActive,
            } = this.data

            switch(job._state){
              case 'inactive':
                if(uniqueReplace){
                  await job.originalRemoveAsync()
                  job = this
                }
                else{
                  return job
                }
                break
              case 'delayed':
                if(uniqueReplace){
                  await job.originalRemoveAsync()
                  job = this
                }
                else{
                  return job
                }
                break
              case 'active':
                if(uniqueKillActive){
                  const mpid = await this.getUniqueJobMPid()
                  if(mpid){
                    try{
                      await this.queue.killMPid(mpid)
                    }
                    catch(e){
                      console.log('error killing job', e)
                    }
                  }
                  else{
                    console.log('error mpid not found for unique job '+job.id+' of type '+job.type)
                  }
                }
                job = this
                break
              case 'complete':
                if(uniqueOnce){
                  return job
                }
                job = this
                break
              case 'failed':
                if(uniqueOnce){
                  return job
                }
                job = this
                break
            }

          }
          else {
            job = this
          }
        }
        // else{
          // await this.saveUniqueJobId()
        // }
      }

      const r = await new Promise((resolve, reject)=>{
        this.originalSave((err)=>{
          if(err){
            reject(err)
          }
          else{
            resolve(job)
          }
        })
      })
      await this.saveUniqueJobId()

      return r
    },
    async getUniqueJobMPid() {
      if(this.data._unique){
        return this.client.hgetAsync(this.queue.uniqueMPidKeyNS(), this.data._unique)
      }
    },
    async getUniqueJobId() {
      if(this.data._unique){
        return this.client.hgetAsync(this.uniqueKeyNS(), this.data._unique)
      }
    },
    async removeUniqueJobId(){
      if(this.data._unique){
        return this.client.hdelAsync(this.uniqueKeyNS(), this.data._unique)
      }
    },
    async saveUniqueJobId() {
      if(this.id && this.data._unique){
        return this.client.hsetAsync(this.uniqueKeyNS(), this.data._unique, this.id)
      }
    },
    remove(done) {
      this.originalRemove(done)
      this.removeUniqueJobId()
      return this
    },
    nextJob(job){
      if(!(job instanceof Array)){
        job = [ job ]
      }
      job = job.map((j)=>j.serializeJob())
      this.data._nextJob = job
      this._nextJob = job
    },
    serializeJob(){
      const pickKeys = [
        'type',
        'data',

        '_priority',

        '_unique',
        '_uniqueOnce',
        '_uniqueReplace',
        '_uniqueGlobal',

        '_nextJob',

        '_max_attempts',
        '_jobEvents',
      ]
      const serializedJob = {}
      for(const key of pickKeys){
        serializedJob[key] = this[key]
      }

      let { _nextJob } = serializedJob
      if(_nextJob){
        if(!(_nextJob instanceof Array)){
          _nextJob = [ _nextJob ]
        }
        serializedJob._nextJob = _nextJob.map((j)=>j.serializeJob ? j.serializeJob() : j)
      }
      return serializedJob
    },
    progress( complete, total = 100, data ){
      return this.originalProgress(complete, total, data)
    },
  })
}

module.exports = function(opts){
  const {
    MACHINE_ID = 0,
    maxFailedTime = 10 * 24 * 60 * 60 * 1000, // 10 days
    maxActiveTime = 2 * 60 * 60 * 1000, // 2 hours
    maxCompleteTime = 1 * 24 * 60 * 60 * 1000, // 1 day
    maxJobs = 2000,
    batchSize = 1000,
    ...options
  } = opts

  const queue = Queue.createQueue(options)

  overrideJob(queue.Job.Job)

  queue.initFlow({
    MACHINE_ID,
    maxFailedTime,
    maxActiveTime,
    maxCompleteTime,
    maxJobs,
    batchSize,
  })

  return queue
}
