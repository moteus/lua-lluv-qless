# lua-lluv-qless
[![Licence](http://img.shields.io/badge/Licence-MIT-brightgreen.svg)](LICENSE)
[![Build Status](https://travis-ci.org/moteus/lua-lluv-qless.svg?branch=master)](https://travis-ci.org/moteus/lua-lluv-qless)
[![Coverage Status](https://coveralls.io/repos/github/moteus/lua-lluv-qless/badge.svg?branch=master)](https://coveralls.io/github/moteus/lua-lluv-qless?branch=master)

## Lua Bindings for [qless](https://github.com/seomoz/qless-core)

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

  -- to handle messages form server we can use EventEmitter API
  job:on('lock_lost', function(self, event, data)
    timer:close()
    -- we do not need call `job:complete`/`job:fail` so just call `done`
    -- we have not pass any error because worker class already know
    -- that this job lost its lock so it just ignore any error.
    done()
  end)

end

return {
  perform = perform
}
```

#### Worker
```Lua
local uv                = require 'lluv'
local QLessWorkerSerial = require 'lluv.qless.worker.serial'

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
local uv    = require 'lluv'
local QLess = require 'lluv.qless'

local qless = QLess.new()

local queue = qless:queue('test-queue')

queue:put("myjobs.test", { counter = 10; interval = 1000 }, function(self, err, jid)
  qless:close()
end)

uv.run()
```
