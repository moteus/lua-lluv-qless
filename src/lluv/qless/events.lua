local ut            = require "lluv.utils"
local Utils         = require "qless.utils"
local BaseClass     = require "qless.base"
local EventEmitter  = require "EventEmitter"

local unpack = unpack or table.unpack

local dummy, is_callable = Utils.dummy, Utils.is_callable

local reconnect_redis = Utils.reconnect_redis

-------------------------------------------------------------------------------
-- Events, to be accessed via qless.events etc.
local QLessEvents = ut.class(BaseClass) do

local ql_ns = "ql:"

function QLessEvents:__init(client)
  self.__base.__init(self)

  self._redis  = client
  self._ee     = EventEmitter.new{self=self}
  self._events = {}

  self._redis:on_message(function(_, channel, ...)
    channel = string.sub(channel, #ql_ns + 1)
    self._ee:emit(channel, ...)
  end)

  self._reconnect_redis = reconnect_redis(self._redis, 5000, function()
    for event in pairs(self._events) do
      self._redis:subscribe(ql_ns .. event)
    end
  end, function(_, err) end)

  return self
end

function QLessEvents:__tostring()
  return self.__base.__tostring(self, "QLess::Events")
end

function QLessEvents:subscribe(events, cb)
  cb = cb or dummy

  for _, event in ipairs(events) do
    if not self._events[event] then
      self._events[event] = true
      --! @fixme do not use private field
      if self._redis._cnn then
        self._redis:subscribe(ql_ns .. event)
      end
    end
  end
end

function QLessEvents:unsubscribe(cb)
    return self.redis:unsubscribe(cb)
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

end
-------------------------------------------------------------------------------

return QLessEvents