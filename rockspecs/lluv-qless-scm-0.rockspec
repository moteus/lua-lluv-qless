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
  "eventemitter",
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
    ["lluv.qless"                  ] = "src/lluv/qless.lua",
    ["lluv.qless.base"             ] = "src/lluv/qless/base.lua",
    ["lluv.qless.client"           ] = "src/lluv/qless/client.lua",
    ["lluv.qless.config"           ] = "src/lluv/qless/config.lua",
    ["lluv.qless.error"            ] = "src/lluv/qless/error.lua",
    ["lluv.qless.events"           ] = "src/lluv/qless/events.lua",
    ["lluv.qless.job"              ] = "src/lluv/qless/job.lua",
    ["lluv.qless.jobs"             ] = "src/lluv/qless/jobs.lua",
    ["lluv.qless.queue"            ] = "src/lluv/qless/queue.lua",
    ["lluv.qless.reserver.ordered" ] = "src/lluv/qless/reserver/ordered.lua",
    ["lluv.qless.rjob"             ] = "src/lluv/qless/rjob.lua",
    ["lluv.qless.script"           ] = "src/lluv/qless/script.lua",
    ["lluv.qless.utils"            ] = "src/lluv/qless/utils.lua",
    ["lluv.qless.worker.serial"    ] = "src/lluv/qless/worker/serial.lua",

    -- qless core library
    ["lluv.qless.lib.qless"        ] = "src/lluv/qless/lib/qless.lua",
    ["lluv.qless.lib.qless-lib "   ] = "src/lluv/qless/lib/qless-lib.lua",
  }
}






















