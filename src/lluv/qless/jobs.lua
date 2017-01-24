local ut           = require "lluv.utils"
local Utils        = require "qless.utils"
local BaseClass    = require "qless.base"

local unpack = unpack or table.unpack
local json, pack_args, dummy = Utils.json, Utils.pack_args, Utils.dummy

-------------------------------------------------------------------------------
local QLessJobs = ut.class(BaseClass) do

function QLessJobs:__init(client)
  self.__base.__init(self)

  self._client = client

  return self
end

function QLessJobs:__tostring()
  return self.__base.__tostring(self, "QLess::Jobs")
end

function QLessJobs:complete(...)
  local args, cb = pack_args(...)
  return self._client:_call(self, "jobs", "complete",
    args[1] or DEFAULT_OFFSET,
    args[2] or DEFAULT_COUNT,
    cb
  )
end

function QLessJobs:tracked(cb)
    local res = self._client:_call(self, "track", function(self, err, res)
      if err then return cb(self, err, res) end
      res = json.decode(res)

      -- local tracked_jobs = {}
      -- for k,v in pairs(res.jobs) do
      --     tracked_jobs[k] = qless_job.new(self.client, v)
      -- end
      -- res.jobs = tracked_jobs
      -- return res

      cb(self, nil, res)
    end)
end

function QLessJobs:tagged(...)
  local args, cb = pack_args(...)
  return self._client:_call(self, "tag", "get",
    assert(args[1], 'no tag argument'),
    args[2] or DEFAULT_OFFSET,
    args[3] or DEFAULT_COUNT,
    function(self, err, res)
      if res and not err then res = json.decode(res) end
      return cb(self, err, res)
    end
  )
end

function QLessJobs:failed(...)
  local args, cb = pack_args(...)
  tag, offset, count = unpack(args)

  if not tag then
    return self._client:_call(self, "failed", cb)
  end

  self._client:_call(self, "failed", tag,
    offset or DEFAULT_OFFSET,
    count  or DEFAULT_COUNT,
    function(self, err, res)
      if err then return cb(self, err, res) end
      res = cjson_decode(res)

      if res.jobs and #res.jobs > 0 then
        res.jobs[#res.jobs + 1] = function(self, err, jobs)
          if err then return cb(self, err, jobs) end
          res.jobs = jobs
          cb(self, err, res)
        end
        return self:multiget(unpack(#res.jobs))
      end

      return cb(self, err, res)
    end
  )
end

function QLessJobs:get(jid, cb)
  cb = cb or dummy

  self._client:_call(self, "get", jid, function(self, err, res)
    if err then return cb(self, err, res) end
    if res then
      res = json.decode(res)
      res = QLessJob.new(self._client, res)
      return cb(self, err, res)
    end

    self._client:_call(self, "recur.get", jid, function(self, err, res)
      if res and not err then
        res = json.decode(res)
        res = QLessRecurJob.new(self._client, res)
      end
      return cb(self, err, res)
    end)
  end)
end

function QLessJobs:multiget(...)
  local args, cb = pack_args(...)

  args[#args + 1] = function(self, err, res)
    if res and not err then res = cjson_decode(res) end

    local jobs = {}
    for _, data in ipairs(res) do
      jobs[#jobs + 1] = QLessJob.new(self._client, data)
    end

    cb(self, err, jobs)
  end

  local res = self.client:_call(self, "multiget", unpack(args))
end

end
-------------------------------------------------------------------------------

return QLessJobs