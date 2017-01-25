local ut = require "lluv.utils"

local function super(self, m, ...)
  return self.__base[m](self, ...)
end

-------------------------------------------------------------------------------
local QLessError = ut.class() do

function QLessError:__init(msg, ext, name, no)
  self._name = name or 'Custom'
  self._no   = no or -1
  self._msg  = msg
  self._ext  = ext

  return self
end

function QLessError:cat()
  return 'QLESS'
end

function QLessError:name()
  return self._name
end

function QLessError:no()
  return self._no
end

function QLessError:msg()
  return self._msg
end

function QLessError:ext()
  return self._ext
end

function QLessError:__tostring()
  local err = string.format("[%s][%s] %s (%d)",
    self:cat(), self:name(), self:msg(), self:no()
  )
  if self:ext() then
    err = string.format("%s - %s", err, self:ext())
  end
  return err
end

function QLessError:__eq(lhs)
  return getmetatable(lhs) == getmetatable(self)
    and self:name() == lhs:name()
    and self:msg()  == lhs:msg()
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
local QLessLuaScriptError = ut.class(QLessError) do

-- @classmethod
local pat = 'user_script:(%d+):%s*(.-)%s*$'
function QLessLuaScriptError.match(s)
  local l, e  = string.match(s, pat)
  if e then
    local _, e2 = string.match(e, pat)
    if e2 then e = e2 end
    return QLessLuaScriptError.new(e, 'Line: ' .. l)
  end
end

function QLessLuaScriptError:__init(msg, ext)
  return super(self, '__init', msg, ext, 'LuaScript', -1)
end

function QLessLuaScriptError:__tostring()
  return super(self, '__tostring')
end

function QLessLuaScriptError:__eq(lhs)
  return super(self, '__eq', lhs)
end

end
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
local QLessLockLostError = ut.class(QLessError) do

local single

function QLessLockLostError:__init(jid)
  return super(self, '__init', 'Lost lock for job', jid 'LockLost', -1)
end

function QLessLockLostError:__tostring()
  return super(self, '__tostring')
end

function QLessLockLostError:__eq(lhs)
  return super(self, '__eq', lhs)
end

end
-------------------------------------------------------------------------------

local function is(klass, err)
  return err and (getmetatable(err) == klass)
end

return {
  is        = is;
  LuaScript = QLessLuaScriptError;
  LockLost  = QLessLockLostError;
}
