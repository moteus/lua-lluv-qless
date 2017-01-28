local ut           = require "lluv.utils"
local Utils        = require "lluv.qless.utils"
local BaseClass    = require "lluv.qless.base"

local json, dummy, pack_args = Utils.json, Utils.dummy, Utils.pack_args

-------------------------------------------------------------------------------
local QLessRecurJob = ut.class(BaseClass) do

local async_get = { };

local async_set = { 
  priority = true,
  retries  = true,
  interval = true,
  data     = true,
  klass    = true,
  backlog  = true,
}

function QLessRecurJob:__init(client, atts)
  self.__base.__init(self, atts)

  self.client     = client
  self.jid        = atts.jid
  self.tags       = atts.tags
  self.count      = atts.count
  self.klass_name = atts.klass
  self.queue_name = atts.queue

  self._priority   = atts.priority
  self._retries    = atts.retries
  self._interval   = atts.interval
  self._data       = json.decode(atts.data or "{}")
  self._klass      = atts.klass
  self._backlog    = atts.backlog

  return self
end

function QLessRecurJob:__tostring()
  return self.__base.__tostring(self, "QLess::RecurJob")
end

function QLessRecurJob:__index(k)
  if async_get[k] then
    error('Parameter `' .. k .. '` can be retrive only in async way', 2)
  end

  if async_set[k] then
    return self['_' .. k]
  end

  return rawget(QLessRecurJob, k) or QLessRecurJob.__base[k]
end

function QLessRecurJob:__newindex(k, v)
  if async_set[k] then
    error('Parameter `' .. k .. '` can be set only in async way', 2)
  end

  if async_get[k] then
    self['_' .. k] = v
    return
  end

  rawset(self, k, v)
end

for name in pairs(async_set) do

QLessRecurJob["set_" .. name] = function(self, ...)
  return self:update(name, ...)
end

end

function QLessRecurJob:update(property, value, cb)
  if property == "data" and value then value = json.encode(value) end

  self.client:_call(self, "recur.update", self.jid, property, value, function(self, err, res)
    if not err then self["_" .. property] = value end
    res = (res == 1) and value or res
    if cb then return cb(self, err, res) end
  end)
end

function QLessRecurJob:requeue(queue, cb)
  self.client:_call(self, "recur.update", self.jid, "queue", queue, function(self, err, res)
    if not err then self.queue_name = queue end
    res = (res == 1) and queue or res
    if cb then cb(self, err, res) end
  end)
end

function QLessRecurJob:cancel(cb)
  local jid = self.jid

  self.client:_call(self, "unrecur", self.jid, function(self, err, res)
    if res == 1 then res = {n=1, jid} end
    if cb then cb(self, err, res) end
  end)
end

function QLessRecurJob:tag(...)
  local args, cb, n = pack_args(...)
  args[n+1] = function(self, err, res)
    if res and not err then
      for k, v in pairs(res) do self.tags[k] = v end
      for k in pairs(self.tags) do
        if res[k] == nil then self.tags[k] = nil end
      end
    end
    return cb(self, err, res)
  end
  return self.client:_call_json(self, "recur.tag", self.jid, unpack(args))
end

function QLessRecurJob:untag(...)
  local args, cb, n = pack_args(...)
  args[n+1] = function(self, err, res)
    if res and not err then
      for k, v in pairs(res) do self.tags[k] = v end
      for k in pairs(self.tags) do
        if res[k] == nil then self.tags[k] = nil end
      end
    end
    return cb(self, err, res)
  end
  return self.client:_call_json(self, "recur.untag", self.jid, unpack(args))
end

function QLessRecurJob:get_next(cb)
  local key = 'ql:q:' .. self.queue_name .. '-recur'
  self.client._redis:zscore(key , self.jid, function(_, err, res)
    cb(self, err, tonumber(res) or res)
  end)
end

end
-------------------------------------------------------------------------------

return QLessRecurJob