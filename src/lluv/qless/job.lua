local uv           = require "lluv"
local ut           = require "lluv.utils"
local Utils        = require "lluv.qless.utils"
local BaseClass    = require "lluv.qless.base"
local QLessError   = require "lluv.qless.error"
local EventEmitter = require "EventEmitter"

local unpack = unpack or table.unpack

local json, now, pass_self, pack_args, dummy, is_callable =
  Utils.json, Utils.now, Utils.pass_self, Utils.pack_args,
  Utils.dummy, Utils.is_callable

-------------------------------------------------------------------------------
local QLessJob = ut.class(BaseClass) do

local async_get = { };

local async_set = { priority = true };

function QLessJob:__init(client, atts)
  self.__base.__init(self)

  self.client            = client
  self.jid               = atts.jid
  self.data              = json.decode(atts.data or "{}")
  self.tags              = atts.tags
  self.state             = atts.state
  self.tracked           = atts.tracked
  self.failure           = atts.failure
  self.dependencies      = atts.dependencies
  self.dependents        = atts.dependents
  self.spawned_from_jid  = atts.spawned_from_jid
  self.expires_at        = atts.expires
  self.worker_name       = atts.worker
  self.klass             = atts.klass
  self.queue_name        = atts.queue
  self.original_retries  = atts.retries
  self.retries_left      = atts.remaining
  self.raw_queue_history = atts.history
  self.state_changed     = false
  self.klass_prefix      = atts.klass_prefix or ''

  self._priority         = atts.priority

  self._ee               = nil

  return self
end

function QLessJob:__tostring()
  return self.__base.__tostring(self, "QLess::Job")
end

function QLessJob:__index(k)
  if async_get[k] then
    error('Parameter `' .. k .. '` can be retrive only in async way', 2)
  end

  if async_set[k] then
    return self['_' .. k]
  end

  return rawget(QLessJob, k) or QLessJob.__base[k]
end

function QLessJob:__newindex(k, v)
  if async_set[k] then
    error('Parameter `' .. k .. '` can be set only in async way', 2)
  end

  if async_get[k] then
    self['_' .. k] = v
    return
  end

  rawset(self, k, v)
end

-- For building a job from attribute data, without the roundtrip to redis.
function QLessJob.build(client, klass, atts)
  local defaults = {
    jid              = client:generate_jid(),
    spawned_from_jid = nil,
    data             = {},
    klass            = klass,
    priority         = 0,
    tags             = {},
    worker           = 'mock_worker',
    expires          = now() + (60 * 60), -- an hour from now
    state            = 'running',
    tracked          = false,
    queue            = 'mock_queue',
    retries          = 5,
    remaining        = 5,
    failure          = {},
    history          = {},
    dependencies     = {},
    dependents       = {},
  }
  setmetatable(atts, { __index = defaults })
  atts.data = json.encode(atts.data)

  return QLessJob.new(client, atts)
end

function QLessJob:queue()
  return self.client.queues:queue(self.queue_name)
end

function QLessJob:set_priority(v, cb)
  assert(tonumber(v))
  self.client:_call(self, 'priority', self.jid, v, function(self, err, res)
    if not err then self._priority = v end
    if cb then return cb(self, err, res) end
  end)
end

local function once(fn)
  local called = false
  return function(...)
    if called then return end
    called = true
    return fn(...)
  end
end

function QLessJob:perform(cb, ...)
  local ok, task = pcall(require, self.klass_prefix .. self.klass)
  if not ok then
    local err = QLessError.General.new(
      self.queue_name .. '-' .. self.klass,
      'Failed to load ' .. self.klass,
      task
    )
    return self:fail(err:name(), err, function() cb(self, err) end)
  end

  if (type(task) ~= 'table') or not is_callable(task.perform) then
    local err = QLessError.General.new(
      self.queue_name .. '-method-missing',
      "Module '" .. self.klass .. "' has no perform function",
      self.queue_name
    )
    return self:fail(err:name(), err, function() cb(self, err) end)
  end

  cb = once(cb)

  local ok, err = pcall(task.perform, self, function(...)
    -- `cb` can be called like `job:complete(cb)/job:fail(..., cb)`
    -- or just `cb()/cb('error')`
    -- Not sure I want requre `cb(job)/cb(job, 'error')`
    if ... == self then return cb(...) end
    return cb(self, ...)
  end, ...)

  if not ok then
    local err = QLessError.General.new(
      self.queue_name .. '-' .. self.klass,
      'Failed to execute ' .. self.klass .. '.perform',
      err
    )
    return self:fail(err:name(), err, function() cb(self, err) end)
  end
end

function QLessJob:description()
  return self.klass .. " (" .. self.jid .. " / " .. self.queue_name .. " / " .. self.state .. ")"
end

function QLessJob:ttl()
  return self.expires_at - now()
end

function QLessJob:spawned_from(cb)
  if not self.spawned_from_jid then
    return uv.defer(cb, self, nil, nil)
  end

  if self.spawned_from then
    return uv.defer(cb, self, nil, self.spawned_from)
  end

  self.client.jobs:get(self.spawned_from_jid, pass_self(self, cb or dummy))
end

function QLessJob:requeue(queue, ...)
  local options, cb = ...
  if is_callable(options) then options, cb = nil, options end
  if not options then options = {} end

  self:begin_state_change("requeue")
  self.client:_call(self, "requeue", 
    self.client.worker_name, queue, self.jid, self.klass,
    json.encode(options.data or self.data),
    options.delay or 0,
    "priority", options.priority or self.priority,
    "tags",     json.encode(options.tags or self.tags),
    "retries",  options.retries or self.original_retries,
    "depends",  json.encode(options.depends or self.dependencies),
    function(self, err, res)
      self:finish_state_change("requeue", err)
      if cb then cb(self, err, res) end
    end
  )
end

function QLessJob:fail(...)
  local args, cb, group, message = pack_args(...)
  group, message = args[1], args[2]

  self:begin_state_change("fail")
  self.client:_call(self, "fail",
    self.jid,
    self.client.worker_name,
    group or "[unknown group]",
    message or "[no message]",
    json.encode(self.data),
    function(self, err, res)
      self:finish_state_change("fail", err)
      cb(self, err, res)
    end
  )
end

function QLessJob:heartbeat(skip_data, cb)
  if type(skip_data) == 'function' then
    skip_data, cb = nil, skip_data
  end

  local on_hb = function(self, err, res)
    if res and not err then self.expires_at = res end

    if err and QLessError.is(QLessError.LuaScript, err) then
      err = QLessError.LockLost.new(self.jid, err:msg())
      self:emit('lock_lost', {
        event = 'lock_lost';
        jid = self.jid;
      })
    end

    if cb then cb(self, err, res) end
  end

  if skip_data then
    self.client:_call(self,
     "heartbeat",
      self.jid,
      self.worker_name,
      on_hb
    )
  else
    self.client:_call(self,
     "heartbeat",
      self.jid,
      self.worker_name,
      json.encode(self.data),
      on_hb
    )
  end
end

function QLessJob:complete(...)
  local args, cb, next_queue, options = pack_args(...)
  next_queue, options = args[1], args[2] or {}

  local function on_complete(self, err, res)
    self:finish_state_change("complete", err, res)
    if cb then cb(self, err, res) end
  end

  self:begin_state_change("complete")
  if next_queue then
    self.client:_call(self, "complete",
      self.jid,
      self.worker_name,
      self.queue_name,
      json.encode(self.data),
      "next", next_queue,
      "delay", options.delay or 0,
      "depends", json.encode(options.depends or {}),
      on_complete
    )
  else
    self.client:_call(self, "complete",
      self.jid,
      self.worker_name,
      self.queue_name,
      json.encode(self.data),
      on_complete
    )
  end
end

function QLessJob:retry(...)
  local args, cb, delay, group, message = pack_args(...)
  delay, group, message = args[1], args[2], args[3]

  self:begin_state_change("retry")
  self.client:_call(self, "retry",
    self.jid,
    self.queue_name,
    self.worker_name,
    delay or 0,
    group or "[unknown group]",
    message or "[no message]",
    function(self, err, res)
      self:finish_state_change("retry", err)
      cb(self, err, res)
    end
  )
end

function QLessJob:cancel(cb)
  self:begin_state_change("cancel")
  self.client:_call(self, "cancel", self.jid, function(self, err, res)
    self:finish_state_change("cancel", err)
    if cb then cb(self, err, res) end
  end)
end

function QLessJob:timeout(cb)
  return self.client:_call(self, "timeout", self.jid, cb or dummy)
end

function QLessJob:track(cb)
  return self.client:_call(self, "track", "track", self.jid, cb or dummy)
end

function QLessJob:untrack(cb)
  return self.client:_call(self, "track", "untrack", self.jid, cb or dummy)
end

function QLessJob:tag(...)
  local args, cb, n = pack_args(...)
  args[n+1] = function(self, err, res)
    if res and not err then
      for k, v in pairs(res) do self.tags[k] = v end
      for k in pairs(self.tags) do
        if res[k] == nil then self.tags[k] = nil end
      end
    end
    return cb(self, err, res)
  end
  return self.client:_call_json(self, "tag", "add", self.jid, unpack(args))
end

function QLessJob:untag(...)
  local args, cb, n = pack_args(...)
  args[n+1] = function(self, err, res)
    if res and not err then
      for k, v in pairs(res) do self.tags[k] = v end
      for k in pairs(self.tags) do
        if res[k] == nil then self.tags[k] = nil end
      end
    end
    return cb(self, err, res)
  end
  return self.client:_call_json(self, "tag", "remove", self.jid, unpack(args))
end

function QLessJob:depend(...)
  return self.client:_call(self, "depends", self.jid, "on", ...)
end

function QLessJob:undepend(...)
  return self.client:_call(self, "depends", self.jid, "off", ...)
end

function QLessJob:log(message, ...)
  local data, cb = ...
  if is_callable(data) then data, cb = nil, data end

  if data then
    data = json.encode(data)
    self.client:_call(self, "log", self.jid, message, data, cb)
  else
    self.client:_call(self, "log", self.jid, message, cb)
  end
end

function QLessJob:begin_state_change(event)
  self:emit('before.' .. event)
end

function QLessJob:finish_state_change(event, err)
  -- if we reach to server and it return error than we can assume than
  -- state is canged. E.g. we lost lock but notify lost.
  if (not err) or (QLessError.is(QLessError.LuaScript, err)) then
    self.state_changed = true
  end

  self:emit('after.' .. event)
end

function QLessJob:emit(...)
  if self._ee then self._ee:emit(...) end
end

function QLessJob:on(...)
  if not self._ee then
    self._ee = EventEmitter.new{self=self}
  end
  self._ee:on(...)
end

function QLessJob:onAny(...)
  if not self._ee then
    self._ee = EventEmitter.new{self=self}
  end
  self._ee:onAny(...)
end

function QLessJob:off(...)
  if self._ee then
    self._ee:off(...)
  end
end

function QLessJob:offAny(...)
  if self._ee then
    self._ee:offAny(...)
  end
end

end
-------------------------------------------------------------------------------

return QLessJob