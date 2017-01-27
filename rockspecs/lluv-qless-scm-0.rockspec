package = "lluv-qless"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-lluv-qless/archive/master.zip",
  dir = "lua-lluv-qless-master",
}

description = {
  summary    = "Lua binding for qless - queue / pipeline management system",
  homepage   = "https://github.com/moteus/lua-lluv-qless",
  license    = "MIT/X11",
  maintainer = "Alexey Melnichuk",
  detailed   = [[Qless is a powerful Redis-based job queueing system inspired by resque, 
  but built on a collection of Lua scripts.]],
}

dependencies = {
  "lua >= 5.1, < 5.4",
  "bgcrypto-sha",
  "lluv-redis",
  "luasocket",
  "luuid",
  "lua-cjson",

  -- "pdh" for Windows or "luaposix" for other systems
}

build = {
  copy_directories = {'spec'},

  type = "builtin",

  modules = {
    ["qless"                  ] = "src/lluv/qless.lua",
    ["qless.base"             ] = "src/lluv/qless/base.lua",
    ["qless.client"           ] = "src/lluv/qless/client.lua",
    ["qless.config"           ] = "src/lluv/qless/config.lua",
    ["qless.error"            ] = "src/lluv/qless/error.lua",
    ["qless.events"           ] = "src/lluv/qless/events.lua",
    ["qless.job"              ] = "src/lluv/qless/job.lua",
    ["qless.jobs"             ] = "src/lluv/qless/jobs.lua",
    ["qless.queue"            ] = "src/lluv/qless/queue.lua",
    ["qless.reserver.ordered" ] = "src/lluv/qless/reserver/ordered.lua",
    ["qless.rjob"             ] = "src/lluv/qless/rjob.lua",
    ["qless.script"           ] = "src/lluv/qless/script.lua",
    ["qless.utils"            ] = "src/lluv/qless/utils.lua",
    ["qless.worker.serial"    ] = "src/lluv/qless/worker/serial.lua",

    -- qless core library
    ["qless.lib.qless"        ] = "src/lluv/qless/lib/qless.lua",
    ["qless.lib.qless-lib "   ] = "src/lluv/qless/lib/qless-lib.lua",
  }
}






















