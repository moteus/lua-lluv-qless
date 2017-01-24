local uuid         = require "uuid"
local sha1         = require "bgcrypto.sha1"
local psapi        = require "pdh.psapi"
local socket       = require "socket"
local json         = require "cjson"
local uv           = require "lluv"

local Utils do

local _pid
local function getpid()
  if not _pid then
    local proc = psapi.process()
    _pid = proc:pid()
    proc:destroy()
  end
  return _pid
end

local _hostname
local function gethostname()
  if not _hostname then
    _hostname = socket.dns.gethostname()
  end
  return _hostname
end

local function now()
  return os.time()
end

local function generate_jid()
  return string.gsub(uuid.new(), '%-', '')
end

local function dummy() end

local function pass_self(self, cb)
  return function(_, ...)
    return cb(self, ...)
  end
end

local function is_callable(f) return (type(f) == 'function') and f end

local pack_args = function(...)
  local n    = select("#", ...)
  local args = {...}
  local cb   = args[n]
  if is_callable(cb) then
    args[n] = nil
    n = n - 1
  else
    cb = dummy
  end

  return args, cb, n
end

local function read_sha1_file(fname)
  local f, e = io.open(fname, "rb")
  if not f then return nil, e end
  local data = f:read("*all")
  f:close()
  data = data:gsub("\r\n", "\n"):gsub("%f[^\n]%-%-[^\n]-\n", ""):gsub("^%-%-[^\n]-\n", "")
  return data, sha1.digest(data, true)
end

-- create monitoring timer to be able to reconnect redis connection
local function reconnect_redis(cnn, interval, on_connect, on_disconnect)
  local error_handler, timer, connected

  timer = uv.timer():start(0, interval, function(self)
    self:stop()
    cnn:open(error_handler)
  end):stop()

  error_handler = function(self, err)
    if err and connected then
      on_disconnect(cnn, err)
    end

    connected = not err

    if connected then
      return on_connect(cnn)
    end

    if not timer:closed() then
      timer:again()
    end
  end

  cnn:open(error_handler)

  cnn:on_error(error_handler)

  return timer
end

Utils = {
  now             = now;
  getpid          = getpid;
  gethostname     = gethostname;
  generate_jid    = generate_jid;
  pass_self       = pass_self;
  pack_args       = pack_args;
  dummy           = dummy;
  read_sha1_file  = read_sha1_file;
  is_callable     = is_callable;
  json            = json;
  reconnect_redis = reconnect_redis;
}

end

return Utils
