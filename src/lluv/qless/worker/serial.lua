local uv                   = require "lluv"
local ut                   = require "lluv.utils"
local EventEmitter         = require "EventEmitter"
local Utils                = require "qless.utils"
local BaseClass            = require "qless.base"
local QLessClient          = require "qless.client"

local json = Utils.json

-------------------------------------------------------------------------------
local QLessWorkerSerial = ut.class(BaseClass) do

local DEFAULT = {
  interval = 5000;
  concurent = 1;
}

function QLessWorkerSerial:__init(options)
  self.__base.__init(self)

  local reserver = options.reserver or 'ordered'
  if type(reserver) == 'string' then
    reserver = require ("qless.reserver." .. reserver)
  end

  self._client        = options.client or QLessClient.new(options)
  self._events        = self._client:events()
  self._queues        = {}
  self._reserver      = reserver.new(self._queues)
  self._ee            = EventEmitter.new()
  self._fetch_timer   = uv.timer()
  self._max_jobs      = options.concurent or DEFAULT.concurent
  self._poll_interval = options.interval  or DEFAULT.interval
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

  on_perform = function(job, err, res)
    self._active_jobs = self._active_jobs - 1

    assert(self._active_jobs >= 0)
    self._ee:off(job.jid)

    if not job.state_changed then
      if err then job:fail(err, res)
      else job:complete() end
    end

    if self:continuing() then
      local n = self._reserver:progressed() + self._active_jobs
      if n < self._max_jobs then
        self._fetch_timer:stop()
        self._reserver:reserve(on_reserve)
      end
      return
    end

    if self._shutdown then
      local n = self._reserver:progressed() + self._active_jobs
      if n == 0 then self:_do_shutdown() end
      return
    end
  end

  on_reserve = function(reserver, err, job)
    assert(not self._fetch_timer:active())
    assert(self._active_jobs < self._max_jobs)

    if self._shutdown then
      if job then job:retry() end

      local n = self._reserver:progressed() + self._active_jobs
      if n == 0 then self:_do_shutdown() end
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
      job:emit(data.event, data)
    end)

    job:perform(on_perform, self._ee)

    if self:continuing() then
      local n = reserver:progressed() + self._active_jobs
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
    local n = self._reserver:progressed() + self._active_jobs
    if n < self._max_jobs then
      self._fetch_timer:again(self._poll_interval)
    end
  end
end

function QLessWorkerSerial:shutdown()
  self._fetch_timer:stop()
  self._shutdown = true
  local n = self._reserver:progressed() + self._active_jobs
  if n == 0 then self:_do_shutdown() end
end

function QLessWorkerSerial:_do_shutdown()
  self._events:close()
  self._client:close()
end

function QLessWorkerSerial:continuing()
  return not (self._shutdown or self._paused)
end

end

-------------------------------------------------------------------------------

return QLessWorkerSerial
