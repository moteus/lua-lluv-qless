local uv            = require "lluv"
local ut            = require "lluv.utils"
local Utils         = require "lluv.qless.utils"
local BaseClass     = require "lluv.qless.base"
local EventEmitter  = require "EventEmitter"

local dummy, is_callable, pass_self = Utils.dummy, Utils.is_callable, Utils.pass_self

local reconnect_redis, call_q = Utils.reconnect_redis, Utils.call_q

local ENOTCONN = uv.error('LIBUV', uv.ENOTCONN)

-------------------------------------------------------------------------------
-- Events, to be accessed via qless.events etc.
local QLessEvents = ut.class(BaseClass) do

local ql_ns = "ql:"

function QLessEvents:__init(client)
  self.__base.__init(self)

  self._client = client
  self._redis  = client:new_redis_connection()
  self._ee     = EventEmitter.new{self=self}
  self._events = {}

  self._close_q = ut.Queue.new()

  self._redis:on_message(function(_, messsage, event, ...)
    if messsage ~= 'message' then return end

    event = string.sub(event, #ql_ns + 1)
    self._ee:emit(event, ...)
  end)

  self._last_redis_error = ENOTCONN

  self._reconnect_redis = reconnect_redis(self._redis, 5000, function()
    self._client.logger.info('%s: connected to redis server', tostring(self))

    self._last_redis_error = nil

    for event in pairs(self._events) do
      self._redis:subscribe(ql_ns .. event, function(_, err, res)
        if err then
          self._client.logger.error('%s: subscribe %s - fail: %s', tostring(self), event, tostring(err))
        else
          self._client.logger.info('%s: subscribe %s - pass', tostring(self), event, tostring(err))
        end
      end)
    end
  end, function(_, err)
    if not self._reconnect_redis:closed() then
      if err then
        self._client.logger.error('%s: disconnected from redis server: %s', tostring(self), tostring(err))
      else
        self._client.logger.info('%s: disconnected from redis server', tostring(self))
      end
    end

    self._last_redis_error = err or ENOTCONN
  end)

  return self
end

function QLessEvents:__tostring()
  return self.__base.__tostring(self, "QLess::Events")
end

function QLessEvents:subscribe(events, cb)
  cb = cb or dummy

  local n, closed = 0, self._redis:closed()
  for _, event in ipairs(events) do
    --! @todo subscribe to multiple channels at once
    if not self._events[event] then
      self._events[event] = true
      if not closed then
        n = n + 1
        self._redis:subscribe(ql_ns .. event, function(_, err, res)
          n = n - 1
          if err then
            self._client.logger.error('%s: subscribe %s - fail: %s', tostring(self), event, tostring(err))
          else
            self._client.logger.info('%s: subscribe %s - pass', tostring(self), event, tostring(err))
          end
          if n == 0 then cb(self, err, not err) end
        end)
      end
    end
  end

  if closed then
    return uv.defer(cb, self, self._last_redis_error or ENOTCONN)
  end

  if n == 0 then
    return uv.defer(cb, self, nil, 0)
  end
end

function QLessEvents:unsubscribe(events, cb)
  if self._redis:closed() then
    return uv.defer(cb, self, self._last_redis_error or err)
  end

  if is_callable(events) then
    return self._redis:unsubscribe(pass_self(self, events))
  end

  if not events then return self._redis:unsubscribe() end

  local ev = {}
  for k, v in ipairs(events) do ev[k] = ql_ns .. v end
  if cb then ev[#ev + 1] = pass_self(self, cb) end

  return self._redis:unsubscribe(unpack(ev))
end

function QLessEvents:on(event, cb)
  if is_callable(event) then
    self._ee:onAny(event)
  else
    self._ee:on(event, cb)
  end
end

function QLessEvents:off(event, cb)
  if (event == nil) or is_callable(event) then
    return self._ee:offAny(event)
  end

  return self._ee:off(ql_ns .. event, cb)
end

function QLessEvents:close(cb)
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

end
-------------------------------------------------------------------------------

return QLessEvents