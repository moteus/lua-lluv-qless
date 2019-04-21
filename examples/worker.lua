local uv          = require 'lluv'
local QLessWorker = require 'lluv.qless.worker.serial'
local STP         = require 'StackTracePlus'
local log         = require 'log'.new('debug', 
  require 'log.writer.stdout'.new(),
  require 'log.formatter.pformat'.new(true, true)
)

local worker = QLessWorker.new{
  host         = '127.0.0.1';
  queues       = {'test-queue'};
  reserver     = 'ordered';
  concurent    = 1;
  klass_prefix = 'myjobs.',
  logger       = log;
}

uv.signal():start(uv.SIGINT, function()
  log.info('SIGINT shutdown')
  worker:shutdown()
end):unref()

uv.signal():start(uv.SIGBREAK, function()
  log.info('SIGBREAK shutdown')
  worker:shutdown()
end):unref()

worker:run()

uv.run(STP.stacktrace)
