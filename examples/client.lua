local uv    = require 'lluv'
local QLess = require 'lluv.qless'

local qless = QLess.new()

local queue = qless:queue('test-queue')

queue:put("counter", {counter = 10; interval = 1000}, function(self, err, jid)
  print(err, jid)
  qless:close()
end)

uv.run()
