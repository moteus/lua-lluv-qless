local ut           = require "lluv.utils"
local Utils        = require "qless.utils"
local BaseClass    = require "qless.base"

local json, dummy = Utils.json, Utils.dummy

-------------------------------------------------------------------------------
local QLessConfig = ut.class(BaseClass) do

function QLessConfig:__init(client)
  self.__base.__init(self)

  self._client = client

  return self
end

function QLessConfig:__tostring()
  return self.__base.__tostring(self, "QLess::Config")
end

function QLessConfig:set(k, v, cb)
  return self._client:call("config.set", k, v, cb or dummy)
end

function QLessConfig:get(k, cb)
  return self._client:call("config.get", k, cb or dummy)
end

function QLessConfig:all(cb)
  local res, err = self._client:call("config.get", function(self, err, res)
    if res and not err then res = json.decode(res) end
    if cb then cb(self._client, err, res) end
  end)
end

function QLessConfig:clear(k, cb)
  return self._client:call("config.unset", k, cb or dummy)
end

end
-------------------------------------------------------------------------------

return QLessConfig