# lua-lluv-qless
Lua Bindings for qless

### Usage

#### Test job
```Lua
-- this is separate Lua module which export `perform` function
-- This function accept 2 args
-- 1 - QLessJob object
-- 2 - `done` callback. Function have to call this function 
--      when job complete or fail.
local uv = require "lluv"

local function perform(job, done)

  -- get work data
  local i = job.data.counter
  local t = job.data.interval

  -- start async job
  local timer = uv.timer():start(0, t, function(self)
    print(job, '#', i) i = i - 1

    if i == 0 then
      self:close()
      -- can just pass `done` as callback
      return job:complete(done)
    end
  end)

  -- to handle messages form server we can use EventEmiter API
  job:onAny(function(self, event, data)
    if event == 'lock_lost' then
      timer:close()
      job:complete(done)
    end
  end)

end

return {
  perform = perform
}
```

#### Worker
```Lua
local uv                = require 'lluv'
local QLessWorkerSerial = require 'qless.worker.serial'

local worker = QLessWorkerSerial.new{
  queues    = {'test-queue'};
  reserver  = 'ordered';
  concurent = 2;
}

worker:run()

uv.run()
```

#### Client
```Lua
local QLess = require 'qless'

local qless = QLess.new()

local queue = qless:queue('test-queue')

queue:put("myjobs.test", { counter = 10; interval = 1000 }, function(self, err, jid)
  qless:close()
end)

uv.run()
```
