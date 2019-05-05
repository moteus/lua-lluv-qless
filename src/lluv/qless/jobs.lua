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

local ut            = require "lluv.utils"
local Utils         = require "lluv.qless.utils"
local BaseClass     = require "lluv.qless.base"
local QLessJob      = require "lluv.qless.job"
local QLessRecurJob = require "lluv.qless.rjob"

local unpack = unpack or table.unpack

local json, pack_args, dummy = Utils.json, Utils.pack_args, Utils.dummy

local DEFAULT_OFFSET = 0
local DEFAULT_COUNT  = 25

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

local function fetch_jobs(self, err, res, cb)
  if not cb then return end

  if res and res.jobs and #res.jobs > 0 then
    local on_multiget = function(self, err, jobs)
      if err then return cb(self, err, jobs) end
      res.jobs = jobs
      cb(self, err, res)
    end
    res.jobs[#res.jobs + 1] = on_multiget
    return self:multiget(unpack(res.jobs))
  end

  return cb(self, err, res)
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
  local res = self._client:_call_json(self, "track", function(self, err, res)
    if not cb then return end

    local tracked_jobs = {}
    for k,v in pairs(res.jobs) do
      tracked_jobs[k] = QLessJob.new(self._client, v)
    end
    res.jobs = tracked_jobs

    return cb(self, err, res)
  end)
end

function QLessJobs:tagged(...)
  local args, cb = pack_args(...)
  return self._client:_call_json(self, "tag", "get",
    assert(args[1], 'no tag argument'),
    args[2] or DEFAULT_OFFSET,
    args[3] or DEFAULT_COUNT,
    function(self, err, res)
      return fetch_jobs(self, err, res, cb)
    end
  )
end

function QLessJobs:failed(...)
  local args, cb = pack_args(...)
  local group, offset, count = unpack(args)

  if not group then
    return self._client:_call_json(self, "failed", function(self, err, res)
      return cb(self, err, res)
    end)
  end

  return self._client:_call_json(self, "failed", group,
    offset or DEFAULT_OFFSET,
    count  or DEFAULT_COUNT,
    function(self, err, res)
      return fetch_jobs(self, err, res, cb)
    end
  )
end

function QLessJobs:get(jid, cb)
  cb = cb or dummy

  self._client:_call_json(self, "get", jid, function(self, err, res)
    if err then return cb(self, err, res) end

    if res then
      res = QLessJob.new(self._client, res)
      return cb(self, err, res)
    end

    self._client:_call_json(self, "recur.get", jid, function(self, err, res)
      if res and not err then
        res = QLessRecurJob.new(self._client, res)
      end
      return cb(self, err, res)
    end)
  end)
end

function QLessJobs:multiget(...)
  local args, cb = pack_args(...)

  args[#args + 1] = function(self, err, res)
    if res and not err then res = json.decode(res) end

    local jobs = {}
    for _, data in ipairs(res) do
      jobs[#jobs + 1] = QLessJob.new(self._client, data)
    end

    cb(self, err, jobs)
  end

  local res = self._client:_call(self, "multiget", unpack(args))
end

end
-------------------------------------------------------------------------------

return QLessJobs