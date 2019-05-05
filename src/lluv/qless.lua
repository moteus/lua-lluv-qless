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

local QLessClient = require "lluv.qless.client"

local QLess = {
  _VERSION   = "0.1.0-dev",
  _NAME      = "lluv-qless",
  _LICENSE   = "MIT",
  _COPYRIGHT = "Copyright (C) 2017-2019 Alexey Melnichuk",

  new = QLessClient.new;
  Client = QLessClient;
}

return QLess