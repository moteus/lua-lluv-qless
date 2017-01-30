--! @todo move to config
local TEST_SERVER = 'redis://127.0.0.1/11'

local prequire = function(m)
  local ok, m = pcall(require, m)
  if ok then return m end
end

if not prequire"spec.spec_helper" then require "spec_helper" end

local QLess = require "lluv.qless"
local uv    = require "lluv"
local ut    = require "lluv.utils"

QLess.Reserver = {
  Ordered = require "lluv.qless.reserver.ordered"
}

QLess.Worker = {
  Serial = require "lluv.qless.worker.serial"
}

local stp do local ok
ok, stp = pcall(require, "StackTracePlus")
if not ok then stp = nil end
end

local logger if pcall(require, "log") then
  logger = require "log".new('warning', require 'log.writer.stdout'.new())
end

local loop  = require 'lluv.busted.loop'

setloop(loop)

loop.set_timeout(5)

loop.set_traceback(stp and stp.stacktrace or debug.traceback)

local before_each = function(ctx, done)
  ctx.client = assert.qless_class('Client', QLess.new{
    server = TEST_SERVER,
    logger = logger,
  })
  ctx.redis = ctx.client._redis
  ctx.redis:flushdb(function(self, err)
    assert.is_nil(err)
    self:script_flush(function(self, err)
      assert.is_nil(err)
      done(ctx)
    end)
  end)
end

local after_each = function(ctx, done)
  if ctx.client then
    ctx.client:close(function()
      loop.verify_after()
      done(ctx)
    end)
    ctx.client, ctx.redis = nil
  else
    done(ctx)
  end
end

local setup = function(ctx, done)
  local ctx = {}

  local function V(version)
    local maj, min = ut.usplit(version, '.', true)
    return tonumber(maj) * 1000 + tonumber(min)
  end

  local verify_redis_version = function(res)
    local version = assert.string(res.server.redis_version)
    assert.truthy(V(version) >= V'2.2' and V(version) < V'4.0', 'Unsupported Redis version:' .. version)
    done(ctx)
  end

  local client = assert.qless_class('Client', QLess.new{
    server = TEST_SERVER,
    logger = logger,
  })

  local redis = client._redis

  redis:info('server', function(self, err, res)
    client:close(function()
      uv.defer(verify_redis_version, res)
    end)
  end)
end

return {
  before_each = before_each;
  after_each  = after_each;
  setup       = setup;
}