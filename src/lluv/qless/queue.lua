local ut           = require "lluv.utils"
local Utils        = require "qless.utils"
local BaseClass    = require "qless.base"
local QLessJob     = require "qless.job"

local unpack = unpack or table.unpack

local json, now, pass_self, pack_args, dummy, is_callable =
  Utils.json, Utils.now, Utils.pass_self, Utils.pack_args, Utils.dummy, Utils.is_callable

local DEFAULT_OFFSET = 0
local DEFAULT_COUNT  = 25

-------------------------------------------------------------------------------
-- Object for interacting with jobs in different states in the queue. Not meant to be
-- instantiated directly, it's accessed via queue.jobs.
local QLessQueueJobs = ut.class(BaseClass) do

function QLessQueueJobs:__init(queue)
  self.__base.__init(self)

  self.client = queue.client
  self.name   = queue.name

  return self
end

function QLessQueueJobs:__tostring()
  return self.__base.__tostring(self, "QLess::QueueJobs")
end

local function call(self, cmd, ...)
  local args, cb = pack_args(...)
  return self.client:_call(self, 
    "jobs", cmd, self.name,
    args[1] or DEFAULT_OFFSET,
    args[2] or DEFAULT_COUNT,
    cb
  )
end

function QLessQueueJobs:running(self, ...)
  return call(self, "running", ...)
end

function QLessQueueJobs:stalled(self, ...)
  return call(self, "stalled", ...)
end

function QLessQueueJobs:scheduled(self, ...)
  return call(self, "scheduled", ...)
end

function QLessQueueJobs:depends(self, ...)
  return call(self, "depends", ...)
end

function QLessQueueJobs:recurring(self, ...)
  return call(self, "recurring", ...)
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
local QLessQueue = ut.class(BaseClass) do

function QLessQueue:__init(name, client)
  self.__base.__init(self)

  self.name        = name
  self.client      = client
  self.worker_name = client.worker_name
  self.jobs        = QLessQueueJobs.new(self)

  return self
end

function QLessQueue:__tostring()
  return self.__base.__tostring(self, "QLess::Queue")
end

function QLessQueue:counts(cb)
  self.client:_call("queues", self.name, function(self, err, res)
    if res and not err then res = json.decode(res) end
    if cb then cb(self, err, res) end
  end)
end

function QLessQueue:set_heartbeat(v, cb)
  self.client.config:set(self.name .. '-heartbeat', v, pass_self(self, cb or dummy))
end

function QLessQueue:get_heartbeat(cb)
  self.client.config:get(self.name .. '-heartbeat', function(_, err, res)
    if err then return cb(self, err, nil) end
    res = tonumber(res)
    if res then return cb(self, err, tonumber(res)) end
    self.client.config:get('heartbeat', function(_, err, res)
      return cb(self, err, tonumber(res))
    end)
  end)
end

function QLessQueue:paused(cb)
  self:counts(function(self, err, cb)
    if res and not err then res = res.paused or false end
    if cb then cb(self, err, res) end
  end)
end

function QLessQueue:pause(self, ...)
  local options, cb = ...
  if is_callable(options) then options, cb = nil, options end
  if not options then options = {} end
  if not cb then cb = dummy end

  self.client:_call(self, "pause", self.name, function(self, err, res)
    if err or not options.stop_jobs then
      return cb(self, err, res)
    end

    self.jobs:running(0, -1, function(_, err, res)
      if err then return cb(self, err, res) end
      self.client:_call(self, "timeout", res, cb)
    end)
  end)
end

function QLessQueue:unpause(cb)
  return self.client:_call(self, "unpause", self.name, cb or dummy)
end

function QLessQueue:put(klass, data, ...)
  local options, cb = ...
  if is_callable(options) then options, cb = nil, options end
  if not options then options = {} end

  return self.client:_call(self,
    "put",
    self.worker_name,
    self.name,
    options.jid or self.client:generate_jid(),
    klass,
    json.encode(data or {}),
    options.delay or 0,
    "priority", options.priority or 0,
    "tags", json.encode(options.tags or {}),
    "retries", options.retries or 5,
    "depends", json.encode(options.depends or {}),
    cb or dummy
  )
end

function QLessQueue:recur(klass, data, interval, ...)
  local options, cb = ...
  if is_callable(options) then options, cb = nil, options end
  if not options then options = {} end

  return self.client:_call(self,
    "recur",
    self.name,
    options.jid or self.client:generate_jid(),
    klass,
    cjson_encode(data or {}),
    "interval", interval, options.offset or 0,
    "priority", options.priority or 0,
    "tags", cjson_encode(options.tags or {}),
    "retries", options.retries or 5,
    "backlog", options.backlog or 0,
    cb or dummy
  )
end

function QLessQueue:pop(...)
  local count, cb = ...
  if is_callable(count) then count, cb = nil, count end
  count, cb = count or 1, cb or dummy

  self.client:_call(self, "pop", self.name, self.worker_name, count,
    function(self, err, res)
      if err then return cb(self, err, res) end
      res = json.decode(res)
      local jobs = {}
      for i = 1, #res do
        jobs[i] = QLessJob.new(self.client, res[i])
      end

      if count == 1 then jobs = jobs[1] end
      cb(self, err, jobs)
    end
  )
end

function QLessQueue:peek(...)
  local count, cb = ...
  if is_callable(count) then count, cb = nil, count end
  count, cb = count or 1, cb or dummy

  self.client:_call(self, "peek", self.name, self.worker_name, count,
    function(self, err, res)
      if err then return cb(self, err, res) end
      res = json.decode(res)
      local jobs = {}
      for i = 1, #res do
        jobs[i] = QLessJob.new(self.client, res[i])
      end

      if count == 1 then jobs = jobs[1] end
      cb(self, err, jobs)
    end
  )
end

function QLessQueue:stats(...)
  local time, cb = ...
  if is_callable(time) then time, cb = nil, time end
  time, cb = time or now(), cb or dummy

  self.client:_call(self, "stats", self.name, time, function(self, err, res)
    if res and not err then res = json.decode(res) end
    cb(self, err, res)
  end)
end

function QLessQueue:length(cb)
  local redis = self.client._redis

  cb = cb or dummy

  local len = 0

  redis:multi(function(cli, err, data)
    if err and err:cat() == 'REDIS' then
      error("Please fix me:" .. tostring(err))
    end
  end)

  redis:zcard("ql:q:"..self.name.."-locks")
  redis:zcard("ql:q:"..self.name.."-work"  )
  redis:zcard("ql:q:"..self.name.."-scheduled")

  redis:exec(function(cli, err, res)
    if err then return cb(self, err, res) end
    for _, v in ipairs(res) do len = len + v end
    cb(self, err, len)
  end)
end

end
-------------------------------------------------------------------------------

return QLessQueue