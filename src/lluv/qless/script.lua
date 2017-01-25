local ut           = require "lluv.utils"
local Utils        = require "qless.utils"
local BaseClass    = require "qless.base"
local debug        = require "debug"
local QLessError   = require "qless.error"

local unpack = unpack or table.unpack
local pack_args, read_sha1_file = Utils.pack_args, Utils.read_sha1_file

local QLESS_LUA_PATH do
  local sep = package.config:sub(1, 1)
  local off = #"script.lua" + 1
  local cwd = string.sub(debug.getinfo(1).source, 2, -off)
  QLESS_LUA_PATH = cwd .. 'lib' .. sep .. 'qless.lua'
end

-------------------------------------------------------------------------------
local QLessLuaScript = ut.class(BaseClass) do

local script_cache = {}

function QLessLuaScript:__init(client, path)
  self.__base.__init(self)

  path = path or QLESS_LUA_PATH

  local cached = script_cache[path]
  if not cached then
    cached = {assert(read_sha1_file(path))}
    script_cache[path] = cached
  end

  self._script, self._sha = cached[1], cached[2]
  self._client = client
  self._redis  = client._redis

  return self
end

function QLessLuaScript:__tostring()
  return self.__base.__tostring(self, "QLess::LuaScript")
end

function QLessLuaScript:reload(cb)
  self._redis:script_load(self._script, function(_, err, hash)
    if err then return cb(self, err) end
    self._sha = hash
    return cb(self, err, hash)
  end)
end

function QLessLuaScript:_call_again(_self, cb, args)
  args[#args + 1] = function(_, err, result)
    return cb(_self, err, result)
  end

  self._redis:evalsha(self._sha, "0", unpack(args))
end

local function check_error(cb)
  return function(self, err, ...)
    if err then
      if err:cat() == 'REDIS' and err:name() == 'ERR' then
        err = QLessError.LuaScript.match(err:msg()) or err
      end
    end
    return cb(self, err, ...)
  end
end

function QLessLuaScript:call(_self, ...)
  local args, cb, n = pack_args(...)
  cb = check_error(cb)

  for i = 1, n do
    assert(args[i] ~= nil)
    args[i] = tostring(args[i])
  end

  args[#args + 1] = function(_, err, result)
    if err and err:cat() == 'REDIS' and err:name() == 'NOSCRIPT' then
      return self:reload(function(self, err)
        if err then return cb(_self, err) end

        -- remove current callback
        args[#args] = nil
        self:_call_again(_self, cb, args)
      end)
    end

    return cb(_self, err, result)
  end

  self._redis:evalsha(self._sha, "0", unpack(args))
end

end
-------------------------------------------------------------------------------

return QLessLuaScript