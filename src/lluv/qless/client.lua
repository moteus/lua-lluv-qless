local uv             = require "lluv"
local ut             = require "lluv.utils"
local redis          = require "lluv.redis"

local Utils          = require "lluv.qless.utils"
local BaseClass      = require "lluv.qless.base"

local QLessLuaScript = require "lluv.qless.script"
local QLessConfig    = require "lluv.qless.config"
local QLessEvents    = require "lluv.qless.events"
local QLessQueue     = require "lluv.qless.queue"
local QLessJobs      = require "lluv.qless.jobs"

local unpack = unpack or table.unpack

local json, now, generate_jid, pass_self, pack_args, dummy, gethostname, getpid =
  Utils.json, Utils.now, Utils.generate_jid, Utils.pass_self, Utils.pack_args,
  Utils.dummy, Utils.gethostname, Utils.getpid

local reconnect_redis, dummy_logger = Utils.reconnect_redis, Utils.dummy_logger

local call_q = Utils.call_q

local ENOTCONN = uv.error('LIBUV', uv.ENOTCONN)
-------------------------------------------------------------------------------
-- Queues, to be accessed via qless.queues etc.
local QLessQueues = ut.class(BaseClass) do

function QLessQueues:__init(client)
  self.__base.__init(self)

  self._client = client
  self._queues = {}

  return self
end

function QLessQueues:__tostring()
  return self.__base.__tostring(self, "QLess::Queues")
end

function QLessQueues:queue(name)
  local queue = self._queues[name]
  if not queue then
    queue = QLessQueue.new(name, self._client)
    self._queues[name] = queue
  end

  return queue
end

function QLessQueues:counts(cb)
  return self._client:_call_json(self, 'queues', cb)
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Workers, to be accessed via qless.workers.
local QLessWorkers = ut.class(BaseClass) do

function QLessWorkers:__init(client)
  self.__base.__init(self)

  self._client = client;

  return self
end

function QLessWorkers:__tostring()
  return self.__base.__tostring(self, "QLess::Workers")
end

function QLessWorkers:_get(_self, name, cb)
  return self._client:_call_json(_self, 'workers', name, cb)
end

function QLessWorkers:worker(name, cb)
  return self:_get(self, name, cb)
end

function QLessWorkers:counts(...)
  return self._client:_call_json(self, 'workers', ...)
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
local QLessClient = ut.class(BaseClass) do

function QLessClient:__init(options)
  options = options or {}

  self.__base.__init(self)

  if options.redis then
    self._redis = options.redis
  else
    self._redis = redis.Connection.new(options)
  end

  if options.logger then
    -- create new logger with our formatter
    local logger = options.logger
    self.logger = require "log".new(
      logger.lvl(),
      logger.writer(),
      require 'log.formatter.pformat'.new(),
      logger.format()
    )
  else
    self.logger = dummy_logger
  end

  self._script      = QLessLuaScript.new(self)

  self.config       = QLessConfig.new(self)
  self.queues       = QLessQueues.new(self)
  self.jobs         = QLessJobs.new(self)
  self.workers      = QLessWorkers.new(self)
  self.worker_name  = options.worker_name or string.format("%s-%d", gethostname(), getpid())

  self._close_q     = ut.Queue.new()

  self._last_redis_error = nil

  --! @fixme use configuriable reconnect interval
  self._reconnect_redis  = reconnect_redis(self._redis, 5000, function()
    self.logger.info('%s: connected to redis server', tostring(self))
    self._last_redis_error = nil
  end, function(_, err)
    if not self._reconnect_redis:closed() then
      if err then
        self.logger.error('%s: disconnected from redis server: %s', tostring(self), tostring(err))
      else
        self.logger.info('%s: disconnected from redis server', tostring(self))
      end
    end

    self._last_redis_error = err or ENOTCONN
  end)

  return self
end

function QLessClient:__tostring()
  return self.__base.__tostring(self, "QLess::Client")
end

function QLessClient:close(cb)
  if cb then
    if not self._close_q then
      return uv.defer(cb, self, ENOTCONN)
    end
    self._close_q:push(cb)
  end

  if not (self._reconnect_redis:closed() or self._reconnect_redis:closing()) then
    self._reconnect_redis:close(function()
      self._redis:close(function(_, ...)
        call_q(self._close_q, self, ...)
        self._close_q = nil
      end)
    end)
  end
end

function QLessClient:new_redis_connection()
  if self._redis.clone then
    return self._redis:clone()
  end

  local opt = {
    host = self._redis._host;
    port = self._redis._port;
    pass = self._redis._pass;
    db   = self._redis._db;
  }
  return redis.Connection.new(opt):open()
end

function QLessClient:generate_jid()
  return generate_jid()
end

function QLessClient:queue(name)
  return self.queues:queue(name)
end

function QLessClient:worker(name, cb)
  return self.workers:_get(self, name, cb)
end

function QLessClient:job(jid, cb)
  self.jobs:get(jid, pass_self(self, cb or dummy))
end

function QLessClient:events()
  return QLessEvents.new(self)
end

function QLessClient:_call(_self, command, ...)
  if self._redis:closed() then
    local _, cb = pack_args(...)
    return uv.defer(cb, _self, self._last_redis_error or ENOTCONN)
  end
  return self._script:call(_self, command, now(), ...)
end

function QLessClient:_call_json(_self, command, ...)
  if self._redis:closed() then
    local _, cb = pack_args(...)
    return uv.defer(cb, _self, self._last_redis_error or ENOTCONN)
  end
  return self._script:call_json(_self, command, now(), ...)
end

function QLessClient:call(...)
  return self:_call(self, ...)
end

function QLessClient:call_json(...)
  return self:_call_json(self, ...)
end

function QLessClient:track(jid, cb)
  return self:call("track", "track", jid, cb or dummy)
end

function QLessClient:untrack(jid, cb)
  return self:call("track", "untrack", jid, cb or dummy)
end

function QLessClient:tags(...)
  local args, cb, offset, count = pack_args(...)
  offset, count = args[1] or 0, args[2] or 100

  return self:call_json("tag", "top", offset, count, cb)
end

function QLessClient:unfail(queue, group, ...)
  local args, cb, count = pack_args(...)
  count = args[1] or 25

  return self:call_json("unfail", queue, group, count, cb)
end

function QLessClient:deregister_workers(worker_names, cb)
  local n = #worker_names + 1
  worker_names[n] = cb

  self:call("worker.deregister", unpack(worker_names))

  worker_names[n] = nil
end

function QLessClient:bulk_cancel(jids, cb)
  local n = #jids + 1
  jids[n] = cb
  self:call("cancel", unpack(jids))
  jids[n] = nil
end

end
-------------------------------------------------------------------------------

return QLessClient