local uv                   = require "lluv"
local ut                   = require "lluv.utils"
local EventEmitter         = require "EventEmitter"
local Utils                = require "lluv.qless.utils"
local BaseClass            = require "lluv.qless.base"
local QLessClient          = require "lluv.qless.client"
local QLessError           = require "lluv.qless.error"

local json = Utils.json

--! @todo handle signals
--  TERM, INT => terminate process
--  HUP  => custom handler from settings
--  QUIT => worker.shutdown
--  CONT => worker.unpause
--  USR2 => worker.pause

-------------------------------------------------------------------------------
local QLessWorkerSerial = ut.class(BaseClass) do

local DEFAULT = {
  interval = 5000;
  concurent = 1;
}

local LOCK_LOST_EVENTS = {
  lock_lost=true, canceled=true,
}

function QLessWorkerSerial:__init(options)
  self.__base.__init(self)

  assert(type(options) == 'table', 'Optrions have to be a table')
  assert(type(options.queues) == 'table', 'Optrions.queues have to be a array')

  options = options or {}

  local reserver = options.reserver or 'ordered'
  if type(reserver) == 'string' then
    reserver = require ("lluv.qless.reserver." .. reserver)
  end

  self._client        = options.client or QLessClient.new(options)
  self._events        = self._client:events()
  self._queues        = {}
  self._reserver      = reserver.new(self._queues)
  self._ee            = EventEmitter.new()
  self._fetch_timer   = uv.timer()
  self._max_jobs      = options.concurent or DEFAULT.concurent
  self._poll_interval = options.interval  or DEFAULT.interval
  self._klass_prefix  = options.klass_prefix
  self._active_jobs   = 0
  self._paused        = false

  local ev = 'w:' .. self._client.worker_name

  --! @todo handle error
  self._events:subscribe{ev}

  self._events:on(ev, function(_, event, data)
    data = json.decode(data)
    self._ee:emit(data.jid, data)
  end)

  for _, name in ipairs(options.queues) do
    if type(name) == 'string' then
      local q = self._client:queue(name)
      self._queues[#self._queues + 1] = q
    else
      local q = name
      --!@ todo supports queues from multiple servers
      assert(q.client == self.client, 'Worker supports queues only from one server')
      self._queues[#self._queues + 1] = q
    end
  end

  return self
end

function QLessWorkerSerial:__tostring()
  return self.__base.__tostring(self, "QLess::Worker::Serial")
end

function QLessWorkerSerial:run()

  local on_perform, on_reserve

  local lock_lost_jobs = {}

  local on_fail = function(job, err, res)
    if err then
      job.client.logger.warning("failed to fail %s: %s", job.jid, tostring(err))
    end
  end

  local on_complite = function(job, err, res)
    if err then
      job.client.logger.warning("failed to complete %s: %s", job.jid, tostring(err))
    end
  end

  on_perform = function(job, err, res)
    self._active_jobs = self._active_jobs - 1

    local lock_lost = lock_lost_jobs[job.jid]

    assert(self._active_jobs >= 0)
    self._ee:off(job.jid)
    lock_lost_jobs[job.jid] = nil

    if not (lock_lost or job.state_changed) then
      if err then
        --! @todo better formatting group/error message
        if (type(err) == 'table') and err.cat then
          res = tostring(err)
          err = job.klass .. ":" .. err:cat() .. ":" .. err:name()
        end

        job:fail(err, res, on_fail)

        -- we do this in case job raise Lua error but did not call
        -- `done` callback. So job:perform calls this callback, but
        -- there may be still pending IO operations
        job:emit('error', {
          group   = err;
          message = res;
        })
      else job:complete(on_complite) end
    end

    if self:continuing() then
      local n = self:count_owned_jobs()
      if n < self._max_jobs then
        self._fetch_timer:stop()
        self._reserver:reserve(on_reserve)
      end
      return
    end

    if self._shutdown then
      local n = self:count_owned_jobs()
      if n == 0 then self:close() end
      return
    end
  end

  on_reserve = function(reserver, err, job)
    assert(not self._fetch_timer:active())
    assert(self._active_jobs < self._max_jobs)

    if self._shutdown then
      if job then job:retry() end

      local n = self:count_owned_jobs()
      if n == 0 then self:close() end
      return
    end

    -- we try all active queues or get error
    if not job then
      if err then
        self._client.logger.error('%s: error reserving job: %s', tostring(self), tostring(err))
      end

      if self:continuing() and (reserver:progressed() == 0) then
        self._fetch_timer:again(self._poll_interval)
      end
      return
    end

    assert(job and not err)

    self._active_jobs = self._active_jobs + 1

    self._ee:on(job.jid, function(_, jid, data)
      if data.event and LOCK_LOST_EVENTS[data.event] then
        lock_lost_jobs[jid] = true
      end
      job:emit(data.event, data)
    end)

    if self._klass_prefix then
      job.klass_prefix = self._klass_prefix
    end

    job:perform(on_perform, self._ee)

    if self:continuing() then
      local n = self:count_owned_jobs()
      if n < self._max_jobs then
        return reserver:reserve(on_reserve)
      end
    end

    -- we do not need restart timer here
    -- because we either have all possible runing jobs
    -- or there already sheduled next `reserve` call
  end

  self._fetch_timer:start(0, function()
    self._fetch_timer:stop()
    self._reserver:restart(on_reserve)
  end)

end

function QLessWorkerSerial:pause()
  self._fetch_timer:stop()
  self._paused = true;
end

function QLessWorkerSerial:unpause()
  self._paused = false;
  if not self._shutdown then
    local n = self:count_owned_jobs()
    if n < self._max_jobs then
      self._fetch_timer:again(self._poll_interval)
    end
  end
end

function QLessWorkerSerial:shutdown()
  self._fetch_timer:stop()
  self._shutdown = true
  local n = self:count_owned_jobs()
  if n == 0 then self:close() end
end

function QLessWorkerSerial:close(cb)
  self._fetch_timer:close()
  self._events:close()
  self:deregister(function() self._client:close(cb) end)
end

function QLessWorkerSerial:continuing()
  return not (self._shutdown or self._paused)
end

function QLessWorkerSerial:deregister(cb)
  self._client:deregister_workers({self._client.worker_name}, function(_, ...)
    if cb then return cb(self, ...) end
  end)
end

function QLessWorkerSerial:count_owned_jobs()
  -- progressed - we already send request for ownership so assume it done
  return self._reserver:progressed() + self._active_jobs
end

end
-------------------------------------------------------------------------------

return QLessWorkerSerial
