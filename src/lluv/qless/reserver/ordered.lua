local uv           = require "lluv"
local ut           = require "lluv.utils"
local Utils        = require "qless.utils"
local BaseClass    = require "qless.base"

-------------------------------------------------------------------------------
local QLessReserverOrdered = ut.class(BaseClass) do

function QLessReserverOrdered:__init(queues)
  self.__base.__init(self)

  -- active queue index
  self._i       = 1

  -- number of active concurent job requests
  self._requests = 0;

  -- array of polling queues (do not copy)
  self._queues  = queues

  return self
end

function QLessReserverOrdered:__tostring()
  return self.__base.__tostring(self, "QLess::Reserver::Ordered")
end

local function on_reserver_finish(self, cb)
  self._requests = self._requests - 1
  assert(self._requests >= 0)
  cb(self, nil, nil)
end

function QLessReserverOrdered:reserve(cb)
  self._requests = self._requests + 1

  local q = self._queues[self._i]

  if not q then
    return uv.defer(on_reserver_finish, self, cb)
  end

  q:pop(function(_, err, job)
    self._requests = self._requests - 1

    assert(self._requests >= 0)

    if err then return cb(self, err, nil) end

    if not job then
      self._i = self._i + 1
      return self:reserve(cb)
    end

    cb(self, nil, job)
  end)
end

function QLessReserverOrdered:restart(cb)
  self._i = 1
  return self:reserve(cb)
end

function QLessReserverOrdered:progressed()
  return self._requests
end

end
-------------------------------------------------------------------------------

return QLessReserverOrdered