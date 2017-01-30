do -- retgister `qless_class` assertion
local say   = require "say"

local getmetatable = getmetatable

local Classes = {
  LuaScript             = require "lluv.qless.script";
  Job                   = require "lluv.qless.job";
  RecurJob              = require "lluv.qless.rjob";
  Jobs                  = require "lluv.qless.jobs";
  Queue                 = require "lluv.qless.queue";
  Events                = require "lluv.qless.events";
  Client                = require "lluv.qless.client";
  Config                = require "lluv.qless.config";
  ["Reserver::Ordered"] = require "lluv.qless.reserver.ordered";
  ["Worker::Serial"]    = require "lluv.qless.worker.serial";
  ["Error::General"]    = require "lluv.qless.error".General;
  ["Error::LuaScript"]  = require "lluv.qless.error".LuaScript;
  ["Error::LockLost"]   = require "lluv.qless.error".LockLost;
}

local function is_qless_class(state, arguments)
  local class  = arguments[1]
  local object = arguments[2]

  if type(class) ~= 'string' then
    error('First argument have to be a QLess class name.')
    return false
  end

  if not Classes[class] then
    error('Unknown QLess class name: ' .. class)
    return false
  end

  class = Classes[class]

  arguments[1] = class

  return class == getmetatable(object), {object}
end

assert:add_formatter(function(t)
  if type(t) ~= 'table' then return end
  for name, cls in pairs(Classes) do
    if cls == t then return "QLess::" .. name end
  end
end)

assert:add_formatter(function(t)
  if type(t) ~= 'table' then return end
  for name, cls in pairs(Classes) do
    if cls == getmetatable(t) then return tostring(t) end
  end
end)

say:set("assertion.qless_class.positive", "Expected %s type, but got: %s")
say:set("assertion.qless_class.negative", "Expected not %s type, but got it")
assert:register("assertion", "qless_class", is_qless_class, "assertion.qless_class.positive", "assertion.qless_class.negative")

end

KlassUtils = {} do

function KlassUtils.preload(name, klass)
  package.preload[name] = function() return klass end
  return klass
end

function KlassUtils.load(name, klass)
  package.loaded[name] = klass
  return klass
end

function KlassUtils.unload(name)
  package.preload[name], package.loaded[name] = nil
end

end

-- some aliases
assert_equal = assert.equal
assert_same  = assert.same
assert_nil   = assert.is_nil

REDIS_ARRAY = function(a)
  if not a.n then a.n = #a end
  return a
end
