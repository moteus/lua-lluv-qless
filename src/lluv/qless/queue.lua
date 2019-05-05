------------------------------------------------------------------
--
--  Author: Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Copyright (C) 2017-2019 Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of lua-lluv-qless library.
--
------------------------------------------------------------------

local ut           = require "lluv.utils"
local Utils        = require "lluv.qless.utils"
local BaseClass    = require "lluv.qless.base"
local QLessJob     = require "lluv.qless.job"

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

function QLessQueueJobs:running(...)
  return call(self, "running", ...)
end

function QLessQueueJobs:stalled(...)
  return call(self, "stalled", ...)
end

function QLessQueueJobs:scheduled(...)
  return call(self, "scheduled", ...)
end

function QLessQueueJobs:depends(...)
  return call(self, "depends", ...)
end

function QLessQueueJobs:recurring(...)
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
  self.client:_call_json(self, "queues", self.name, cb)
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
  self:counts(function(self, err, res)
    if res and not err then res = res.paused or false end
    if cb then cb(self, err, res) end
  end)
end

function QLessQueue:pause(...)
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

function QLessQueue:unfail(group, ...)
  local count, cb = ...
  if is_callable(count) then count, cb = nil, options end
  if not count then count = 25 end

  return self.client:_call(self, "unfail", self.name, group, count, cb or dummy)
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
    json.encode(data or {}),
    "interval", interval, options.offset or 0,
    "priority", options.priority or 0,
    "tags", json.encode(options.tags or {}),
    "retries", options.retries or 5,
    "backlog", options.backlog or 0,
    cb or dummy
  )
end

function QLessQueue:pop(...)
  local count, cb = ...
  if is_callable(count) then count, cb = nil, count end
  count, cb = count or 1, cb or dummy

  self.client:_call_json(self, "pop", self.name, self.worker_name, count,
    function(self, err, jobs)
      if err then return cb(self, err, jobs) end

      for i = 1, #jobs do
        jobs[i] = QLessJob.new(self.client, jobs[i])
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

  self.client:_call_json(self, "peek", self.name, count,
    function(self, err, jobs)
      if err then return cb(self, err, jobs) end

      for i = 1, #jobs do
        jobs[i] = QLessJob.new(self.client, jobs[i])
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

  self.client:_call_json(self, "stats", self.name, time, cb)
end

function QLessQueue:length(cb)
  local redis = self.client._redis

  --! @fixme access to private fields
  if redis:closed() then
    return uv.defer(cb, self, self.client._last_redis_error or ENOTCONN)
  end

  cb = cb or dummy

  local len = 0

  redis:multi(function(_, err)
    if err and err:cat() == 'REDIS' then
      error("Please fix me:" .. tostring(err))
    end
  end)

  redis:zcard("ql:q:"..self.name.."-locks")
  redis:zcard("ql:q:"..self.name.."-work"  )
  redis:zcard("ql:q:"..self.name.."-scheduled")

  redis:exec(function(_, err, res)
    if err then return cb(self, err, res) end
    for _, v in ipairs(res) do len = len + v end
    cb(self, err, len)
  end)
end

end
-------------------------------------------------------------------------------

return QLessQueue