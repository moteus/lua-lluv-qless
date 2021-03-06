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

  local on_message = function(event, ...)
    if string.find(event, "^ql:") then
      event = string.sub(event, #ql_ns + 1)
    end

    self._ee:emit(event, ...)
  end

  self._redis:on('message', function(_, _, ...) on_message(...) end)

  self._redis:on('pmessage', function(_, _, _, ...) on_message(...) end)

  self._reconnect_redis = reconnect_redis(self._redis, 5000, function()
    self._client.logger.info('%s: connected to redis server', tostring(self))

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
  end)

  return self
end

function QLessEvents:__tostring()
  return self.__base.__tostring(self, "QLess::Events")
end

function QLessEvents:subscribe(events, cb)
  cb = cb or dummy

  local n = 0
  for _, event in ipairs(events) do
    if not self._events[event] then
      self._events[event] = true
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

  if n == 0 then
    return uv.defer(cb, self, nil, 0)
  end
end

function QLessEvents:unsubscribe(events, cb)
  if is_callable(events) then
    return self._redis:unsubscribe(pass_self(self, events))
  end

  if not events then return self._redis:unsubscribe() end

  local ev = {}
  for k, v in ipairs(events) do
    self._events[v] = nil
    ev[k] = ql_ns .. v
  end

  local n = 0
  for _, event in ipairs(ev) do
    self._redis:unsubscribe(event, function(_, ...)
      n = n + 1
      if n == #ev then return cb(self, ...) end
    end)
  end

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