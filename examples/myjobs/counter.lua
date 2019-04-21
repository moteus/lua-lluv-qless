-- this is separate Lua module which export `perform` function
-- This function accept 2 args
-- 1 - QLessJob object
-- 2 - `done` callback. Function have to call this function 
--      when job complete or fail.
local uv = require "lluv"

local function perform(job, done)
  -- get work data
  local i = tonumber(job.data.counter)
  local t = tonumber(job.data.interval)

  if not (i and t) then
    job.client.logger.error('%s invalid input data', job.jid)
    return job:fail('invalid-job', 'missing time interval or counter value', done)
  end

  -- start async job
  local timer = uv.timer():start(0, t, function(self)
    job.client.logger.debug("%s - #%.2d", job.jid, i)
    i = i - 1

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
