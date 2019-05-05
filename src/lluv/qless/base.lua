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

local ut    = require "lluv.utils"

-------------------------------------------------------------------------------
local BaseClass = ut.class() do

function BaseClass:__init()
  self._object_id = {}
  self._object_id_hash = string.gsub(tostring(self._object_id), 'table: ', '')

  return self
end

function BaseClass:__tostring(name)
  return string.format( "%s (%s)", name, self._object_id_hash)
end

end
-------------------------------------------------------------------------------

return BaseClass
