local QLess = require "qless"
local uv    = require "lluv"
local say   = require "say"
local loop  = require 'lluv.busted.loop'

local A = function(a)
  if not a.n then a.n = #a end
  return a
end

setloop(loop) loop.set_timeout(5)

describe('QLess test', function()
  local client, redis

  local assert_equal = assert.equal
  local assert_same  = assert.same
  local assert_nil   = assert.is_nil

  describe('Basic tests about the client', function()

    describe('Test the client', function()
      it('track', function(done) async()
        local queue = assert.qless_class('Queue', client:queue('foo'))
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert.is_nil(err) assert.equal('jid', jid)
          client:track('jid', function(_, err)
            assert.is_nil(err)
            client.jobs:tracked(function(_, err, res)
              assert.is_nil(err) assert.table(res)
              assert.are.same({}, res.expired)
              local job = assert.qless_class('Job', res.jobs[1])
              assert.equal('jid', job.jid)
              client:untrack('jid', function(_, err)
                assert.is_nil(err)
                client.jobs:tracked(function(_, err, res)
                  assert.is_nil(err)
                  assert.are.same({jobs = {}, expired = {}}, res)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

      pending('todo', function()
        it('should fail access to invalid attributes', function() async()
          assert.error(function() client.foo = 1 end)
          assert.error(function() local s = client.boo end)
          done()
        end)
      end)

      it('provides access to top tags', function(done) async()
        local queue = assert.qless_class('Queue', client:queue('foo'))
        client:tags(function(self, err, res)
          assert.equal(client, self) assert.is_nil(err) assert.are.same({}, res)
          local n = 10
          for i = 1, n do
            queue:put('Foo', {}, {tags = {'foo'}}, function(self, err, jid)
              assert.equal(queue, self) assert.is_nil(err)
              n = n - 1
              if n == 0 then
                client:tags(function(self, err, res)
                  assert.equal(client, self) assert.is_nil(err) assert.are.same({'foo'}, res)
                  done()
                end)
              end
            end)
          end
        end)
      end)

      it('provides access to unfail', function(done) async()
        local queue = assert.qless_class('Queue', client:queue('foo'))
        local n, jids = 10, {}

        local do_unfail, test_state

        test_state = function(state, next)
          n = #jids
          for i = 1, n do
            local jid = jids[i]
            client:job(jid, function(self, err, job)
              assert.equal(client, self)
              assert.is_nil(err)
              assert.qless_class('Job', job)
              assert.equal(jid, job.jid)
              assert.equal(state, job.state)
              n = n - 1
              if n == 0 then uv.defer(next) end
            end)
          end
        end

        do_unfail = function()
          client:unfail('foo', 'foo', function(self, err, res)
            assert.equal(client, self)
            assert.is_nil(err)
            uv.defer(test_state, 'waiting', function() done() end)
          end)
        end

        for jid = 1, n do
          jid = tostring(jid)
          jids[#jids + 1] = jid

          queue:put('Foo', {}, {jid = jid}, function(self, err, created_jid)
            assert.equal(queue, self)
            assert.is_nil(err)
            assert.equal(jid, created_jid)
            queue:pop(function(self, err, job)
              assert.equal(queue, self)
              assert.is_nil(err)
              assert.qless_class('Job', job)
              job:fail('foo', 'bar', function(self, err, res)
                n = n - 1
                assert.equal(job, self)
                assert.is_nil(err)
                if n == 0 then uv.defer(test_state, 'failed', do_unfail) end
              end)
            end)
          end)
        end
      end)
    end)

    describe('Test the Jobs class', function()
      it('Can give us access to jobs', function(done) async()
        client.jobs:get('jid', function(self, err, job)
          assert.same(client.jobs, self) assert.is_nil(err) assert.is_nil(job)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            client.jobs:get('jid', function(self, err, job)
              assert.same(client.jobs, self) assert.is_nil(err) assert.qless_class('Job', job)
              done()
            end)
          end)
        end)
      end)

      it('Can give us access to recurring jobs', function(done) async()
        client.jobs:get('jid', function(self, err, job)
          assert.same(client.jobs, self) assert.is_nil(err) assert.is_nil(job)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:recur('Foo', {}, 60, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            client.jobs:get('jid', function(self, err, job)
              assert.same(client.jobs, self) assert.is_nil(err) assert.qless_class('RecurJob', job)
              done()
            end)
          end)
        end)
      end)

      it('Can give us access to complete jobs', function(done) async()
        client.jobs:complete(function(self, err, jobs)
          assert.same(client.jobs, self) assert.is_nil(err) assert.same(A{}, jobs)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            queue:pop(function(self, err, job)
              assert.same(queue, self) assert.is_nil(err) assert.qless_class('Job', job)
              job:complete(function(self, err, res)
                assert.same(job, self) assert.is_nil(err) assert.equal('complete', res)
                client.jobs:complete(function(self, err, jobs)
                  assert.same(client.jobs, self) assert.is_nil(err) assert.same(A{'jid'}, jobs)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

      it('Gives us access to tracked jobs', function(done) async()
        client.jobs:tracked(function(self, err, res)
          assert.same(client.jobs, self) assert.is_nil(err) assert.same({jobs={},expired={}}, res)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            client:track('jid', function(self, err, res)
              assert.same(client, self) assert.is_nil(err) assert.equal('1', res)
              client.jobs:tracked(function(self, err, res)
                assert.same(client.jobs, self) assert.is_nil(err)
                assert.table(res)
                assert.same({}, res.expired)
                assert.table(res.jobs)
                assert.qless_class('Job', res.jobs[1])
                assert.equal('jid', res.jobs[1].jid)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Gives us access to tagged jobs', function(done) async()
        client.jobs:tagged('foo', function(self, err, res)
          assert.same(client.jobs, self) assert.is_nil(err) assert.same({jobs={},total=0}, res)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid', tags = {'foo'}}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            client.jobs:tagged('foo', function(self, err, res)
              assert.same(client.jobs, self) assert.is_nil(err)
              assert.table(res)
              assert.equal(1, res.total)
              assert.table(res.jobs)
              assert.qless_class('Job', res.jobs[1])
              assert.equal('jid', res.jobs[1].jid)
              done()
            end)
          end)
        end)
      end)

      it('Gives us access to failed jobs', function(done) async()
        client.jobs:failed('foo', function(self, err, res)
          assert.same(client.jobs, self) assert.is_nil(err) assert.same({jobs={},total=0}, res)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            queue:pop(function(self, err, job)
              assert.same(queue, self) assert.is_nil(err) assert.qless_class('Job', job)
              job:fail('foo', 'bar', function(self, err, jid)
                assert.same(job, self) assert.is_nil(err) assert.equal('jid', jid)
                client.jobs:failed('foo', function(self, err, res)
                  assert.same(client.jobs, self) assert.is_nil(err)
                  assert.table(res)
                  assert.equal(1, res.total)
                  assert.table(res.jobs)
                  assert.qless_class('Job', res.jobs[1])
                  assert.equal('jid', res.jobs[1].jid)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

      it('Gives us access to failure types', function(done) async()
        client.jobs:failed(function(self, err, res)
          assert.same(client.jobs, self) assert.is_nil(err) assert.same({}, res)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            queue:pop(function(self, err, job)
              assert.same(queue, self) assert.is_nil(err) assert.qless_class('Job', job)
              job:fail('foo', 'bar', function(self, err, jid)
                assert.same(job, self) assert.is_nil(err) assert.equal('jid', jid)
                client.jobs:failed(function(self, err, res)
                  assert.same(client.jobs, self) assert.is_nil(err)
                  assert.same({foo=1}, res)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)
    end) -- Jobs test

    describe('Test the Queues class', function()
      it('Geves sync access to queue', function(done) async()
        assert.qless_class('Queue', client:queue('foo'))
        done()
      end)

      it('Gives us access to counts', function(done) async()
        client.queues:counts(function(self, err, res)
          assert_equal(client.queues, self) assert_nil(err)  assert_same({}, res)
          client:queue('foo'):put('Foo', {}, function(self, err, res)
            assert.qless_class('Queue', self) assert_nil(err)
            client.queues:counts(function(self, err, res)
              assert_equal(client.queues, self) assert_nil(err)
              assert_same({{
                scheduled = 0,
                name      = 'foo',
                paused    = false,
                waiting   = 1,
                depends   = 0,
                running   = 0,
                stalled   = 0,
                recurring = 0
              }}, res)
              done()
            end)
          end)
        end)
      end)

      pending('todo', function()
        it('should fail access to invalid attributes', function() async()
          assert.error(function() client.queues.foo = 1 end)
          assert.error(function() local s = client.queues.boo end)
          done()
        end)
      end)
    end) -- queues test

    describe('Test the Workers class', function()
      local worker

      it('Gives us access to individual workers', function(done)async()
        client:queue('foo'):put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert.qless_class('Queue', self) assert_nil(err) assert_equal('jid', jid)
          client:worker('worker', function(self, err, res)
            assert_equal(client, self) assert_nil(err)
            assert_same({jobs={},stalled={}}, res)
            worker:queue('foo'):pop(function(self, err, job)
              assert.qless_class('Queue', self) assert_nil(err) assert.qless_class('Job', job)
              client:worker('worker', function(self, err, res)
                assert_equal(client, self) assert_nil(err)
                assert_same({jobs={'jid'},stalled={}}, res)
                done()
              end)
            end)
          end)
        end)
      end)

      it('', function(done) async()
        client:queue('foo'):put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert.qless_class('Queue', self) assert_nil(err) assert_equal('jid', jid)
          client.workers:counts(function(self, err, res)
            assert_equal(client.workers, self) assert_nil(err)
            assert_same({}, res)
            worker:queue('foo'):pop(function(self, err, job)
              assert.qless_class('Queue', self) assert_nil(err) assert.qless_class('Job', job)
              client.workers:counts(function(self, err, res)
                assert_equal(client.workers, self) assert_nil(err)
                assert_same({{name = 'worker', jobs=1, stalled=0}}, res)
                done()
              end)
            end)
          end)
        end)
      end)

      pending('todo', function()
        it('should fail access to invalid attributes', function() async()
          assert.error(function() client.workers.foo = 1 end)
          assert.error(function() local s = client.workers.boo end)
          done()
        end)
      end)

      before_each(function(done) async()
        worker = assert.qless_class('Client', QLess.new{worker_name = 'worker'})
        done()
      end)

      after_each(function(done) async()
        if worker then
          worker:close(function() done() end)
        else
          done()
        end
      end)
    end) -- Workers test

  end)

  describe('Tests about the config class', function()
    describe('Test the config class', function()
      it('Basic set/get/unset', function(done) async()
        client.config:get('foo', function(self, err, res)
          assert_equal(client, self) assert_nil(err) assert_nil(res)
          client.config:set('foo', 5, function(self, err, res)
            assert_equal(client, self) assert_nil(err) assert_nil(res)
            client.config:set('boo', 6, function(self, err, res)
              assert_equal(client, self) assert_nil(err) assert_nil(res)
              client.config:get('foo', function(self, err, res)
                assert_equal(client, self) assert_nil(err) assert_equal('5', res)
                client.config:unset('foo', function(self, err, res)
                  assert_equal(client, self) assert_nil(err) assert_nil(res)
                  client.config:get('foo', function(self, err, res)
                    assert_equal(client, self) assert_nil(err) assert_nil(res)
                    client.config:get('boo', function(self, err, res)
                      assert_equal(client, self) assert_nil(err) assert_equal('6', res)
                      done()
                    end)
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

      it('Ensure we can get all the configuration', function(done) async()
        client.config:all(function(self, err, res)
          assert_equal(client, self) assert_nil(err)
          assert_same({
            [ 'application'        ] = 'qless',
            [ 'grace-period'       ] = 10,
            [ 'heartbeat'          ] = 60,
            [ 'histogram-history'  ] = 7,
            [ 'jobs-history'       ] = 604800,
            [ 'jobs-history-count' ] = 50000,
            [ 'stats-history'      ] = 30
          }, res)
          done()
        end)
      end)

      it('We can get default config values', function(done) async()
        client.config:get('heartbeat', function(self, err, res)
          assert_equal(client, self) assert_nil(err)
          assert_equal(60, res)
          done()
        end)
      end)

      it('Can unset all keys', function(done) async()
        client.config:all(function(self, err, original)
          assert_equal(client, self) assert_nil(err) assert.table(original)
          local n = 0
          for key in pairs(original) do
            n = n + 1
            client.config:set(key, 1, function(self, err)
              assert_equal(client, self) assert_nil(err)
              n = n - 1
              if n == 0 then
                self.config:clear(function(self, err)
                  assert_equal(client, self) assert_nil(err)
                  client.config:all(function(self, err, config)
                    assert_equal(client, self) assert_nil(err)
                    assert_same(original, config)
                    done()
                  end)
                end)
              end
            end)
          end
        end)
      end)
    end)
  end)

  before_each(function(done) async()
    client = assert.qless_class('Client', QLess.new())
    redis = client._redis
    redis:flushdb(function(self, err)
      assert.is_nil(err)
      self:script_flush(function(self, err)
        assert.is_nil(err)
        done()
      end)
    end)
  end)

  after_each(function(done) async()
    if client then
      client:close(function()
        loop.verify_after()
        done()
      end)
      redis, client = nil
    else
      done()
    end
  end)

end)

do -- retgister `qless_class` assertion
local getmetatable = getmetatable

local Classes = {
  LuaScript             = require "qless.script";
  Job                   = require "qless.job";
  RecurJob              = require "qless.rjob";
  Jobs                  = require "qless.jobs";
  Queue                 = require "qless.queue";
  Events                = require "qless.events";
  Client                = require "qless.client";
  Config                = require "qless.config";
  ["Reserver::Ordered"] = require "qless.reserver.ordered";
  ["Worker::Serial"]    = require "qless.worker.serial";
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
