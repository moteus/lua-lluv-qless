local function prequire(...)
  local ok, mod = pcall(require, ...)
  if not ok then return nil, mod end
  return mod, ...
end

local uuid         = require "uuid"
local sha1         = require "bgcrypto.sha1"
local socket       = require "socket"
local json         = require "cjson"
local uv           = require "lluv"

local Utils do

local getpid do

local _pid

local psapi = prequire("pdh.psapi")
if psapi then
  getpid = function ()
    if not _pid then
      local proc = psapi.process()
      _pid = proc:pid()
      proc:destroy()
    end
    return _pid
  end
end

if not getpid then
  local posix = prequire ("posix")
  if not (posix and posix.getpid) then
    posix = prequire ("posix.unistd")
  end

  if posix and posix.getpid then
    getpid = function ()
      if not _pid then
        _pid = posix.getpid()
        if type(_pid) == 'table' then
          _pid = _pid.pid
        end
      end
      return _pid
    end
  end
end

assert(getpid, 'can not find getpid implementation. Try install lua-posix library for *nix or lua-pdh for Windwos')
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
-- close this timer before close connection object
local function reconnect_redis(cnn, interval, on_connect, on_disconnect)

  local timer = uv.timer():start(0, interval, function(self)
    self:stop()
    cnn:open()
  end):stop()

  local connected = not cnn:closed()

  cnn:on('close', function(self, event, ...)
    local flag = connected

    connected = false

    if flag then on_disconnect(self, ...) end

    if timer:closed() or timer:closing() then
      return
    end

    timer:again()
  end)

  cnn:on('ready', function(self, event, ...)
    connected = true
    on_connect(self, ...)
  end)

  if not connected then
    cnn:open()
  end

  return timer
end

local DummyLogger = {} do
  local lvl = {'emerg','alert','fatal','error','warning','notice','info','debug','trace'}
  for _, l in ipairs(lvl) do
    DummyLogger[l] = dummy;
    DummyLogger[l..'_dump'] = dummy;
  end

  local api = {'writer', 'formatter', 'format', 'lvl', 'set_lvl', 'set_writer', 'set_formatter', 'set_format'}
  for _, meth in ipairs(api) do
    DummyLogger[meth] = dummy
  end
end

local function super(self, m, ...)
  return self.__base[m](self, ...)
end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

Utils = {
  super           = super;
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
  dummy_logger    = DummyLogger;
  call_q          = call_q;
}

end

return Utils
