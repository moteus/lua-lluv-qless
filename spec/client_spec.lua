io.stdout:setvbuf'no';io.stderr:setvbuf'no';

-- Lua 5.2/5.3
package.path = './?.lua;' .. package.path

local prequire = function(m)
  local ok, m = pcall(require, m)
  if ok then return m end
end

local srequire = function(m)
  return require('spec.'..m) or require(m)
end

local TestSetup = srequire"setup"
local QLess     = require "lluv.qless"
local uv        = require "lluv"
local loop      = require "lluv.busted.loop"

print("------------------------------------")
print("Module    name: " .. QLess._NAME);
print("Module version: " .. QLess._VERSION);
print("Lua    version: " .. (_G.jit and _G.jit.version or _G._VERSION))
print("------------------------------------")
print("")

describe('QLess test', function()
  local client, redis

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

      it('allows call close twice', function(done) async()
        local c1, c2

        uv.timer():start(1000, function()
          assert.truthy(c1)
          assert.truthy(c2)
          done()
        end)

        client:close(function() c1 = true end)
        client:close(function() c2 = true end)
      end)

      it('allows call closed client', function(done) async()
        local c1, c2

        uv.timer():start(1000, function()
          assert.truthy(c1)
          done()
        end)

        client:close(function()
          uv.defer(function()
            client:close(function(_, err)
              c1 = true
              assert.not_nil(err)
              assert_equal('ENOTCONN', err:name())
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

      it('can bulk cancel several jobs', function(done) async()
        local queue = assert.qless_class('Queue', client:queue('foo'))
        local j1, j2, j3
        queue:put('Foo', {}, function(_, err, jid) assert_nil(err) j1 = jid end)
        queue:put('Foo', {}, function(_, err, jid) assert_nil(err) j2 = jid end)
        queue:put('Foo', {}, function(_, err, jid) assert_nil(err) j3 = jid
          assert.string(j1)
          assert.string(j2)
          assert.string(j3)
          client:bulk_cancel({j1, j2, j3}, function(self, err, res)
            assert_equal(client, self) assert_nil(err)
            assert_same(REDIS_ARRAY{j1, j2, j3}, res)
            done()
          end)
        end)
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
          assert.same(client.jobs, self) assert.is_nil(err) assert.same(REDIS_ARRAY{}, jobs)
          local queue = assert.qless_class('Queue', client:queue('foo'))
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert.same(queue, self) assert.is_nil(err) assert.equal('jid', jid)
            queue:pop(function(self, err, job)
              assert.same(queue, self) assert.is_nil(err) assert.qless_class('Job', job)
              job:complete(function(self, err, res)
                assert.same(job, self) assert.is_nil(err) assert.equal('complete', res)
                client.jobs:complete(function(self, err, jobs)
                  assert.same(client.jobs, self) assert.is_nil(err) assert.same(REDIS_ARRAY{'jid'}, jobs)
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

      it('Returns same object for same queue', function(done) async()
        local q1 = assert.qless_class('Queue', client:queue('foo'))
        local q2 = assert.qless_class('Queue', client:queue('foo'))
        assert.equal(q1, q2)
        done()
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

      it('Gives us access to worker counts', function(done) async()
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
        worker = assert.qless_class('Client', QLess.new{
          worker_name = 'worker',
          redis       = client:new_redis_connection(),
          logger      = client.logger,
        })
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

  describe('Tests about events', function()
    local events

    describe('test Event class', function()
      it('allows call close twice', function(done) async()
        local c1, c2

        uv.timer():start(1000, function()
          assert.truthy(c1)
          assert.truthy(c2)
          done()
        end)

        events:close(function() c1 = true end)
        events:close(function() c2 = true end)
      end)

      it('allows call closed client', function(done) async()
        local c1, c2

        uv.timer():start(1000, function()
          assert.truthy(c1)
          done()
        end)

        events:close(function()
          uv.defer(function()
            events:close(function(_, err)
              c1 = true
              assert.not_nil(err)
              assert_equal('ENOTCONN', err:name())
            end)
          end)
        end)
      end)
    end)

    describe('Ensure we can get a basic event', function()

      it('Ensure we can subscribe from multiple events', function(done) async()
        events:unsubscribe({'popped', 'timeout'}, function(self, err, res)
          assert_nil(err)
          done()
        end)
      end)

      it('Ensure we can unsubscribe from multiple events', function(done) async()
        events:unsubscribe({'popped', 'timeout'}, function(self, err, res)
          assert_nil(err)
          done()
        end)
      end)

      it('Basic set/get/unset', function(done) async()
        local popped = 0

        events:on('popped', function(self, event, data)
          assert_equal(events, self) assert_equal('popped', event)
          popped = popped + 1
        end)

        events:subscribe({'popped', 'timeout'}, function(self, err)
          assert_equal(events, self) assert_nil(err) 
          client:queue('foo'):pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            uv.timer():start(100, function()
              assert_equal(1, popped)
              done()
            end)
          end)
        end)
      end)

      it('unsubscribe from all events', function(done) async()
        local popped = 0

        events:on('popped', function(self, event, data)
          assert_equal(events, self) assert_equal('popped', event)
          popped = popped + 1
        end)

        events:subscribe({'popped'}, function(self, err)
          assert_equal(events, self) assert_nil(err)
          events:unsubscribe(function(self, err)
            assert_equal(events, self) assert_nil(err)
            client:queue('foo'):pop(function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              uv.timer():start(100, function()
                assert_equal(0, popped)
                done()
              end)
            end)
          end)
        end)
      end)

      it('unsubscribe from specific events', function(done) async()
        local popped, canceled = 0, 0

        events:on('popped', function(self, event, data)
          assert_equal(events, self) assert_equal('popped', event)
          popped = popped + 1
        end)

        events:on('canceled', function(self, event, data)
          assert_equal(events, self) assert_equal('canceled', event)
          canceled = canceled + 1
        end)

        events:subscribe({'popped', 'canceled'}, function(self, err)
          assert_equal(events, self) assert_nil(err)
          events:unsubscribe({'popped'}, function(self, err)
            assert_equal(events, self) assert_nil(err)
            client:queue('foo'):pop(function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              job:cancel(function(self, err, jid)
                assert_equal(job, self) assert_nil(err) assert_equal(job.jid, jid)
                uv.timer():start(100, function()
                  assert_equal(0, popped)
                  assert_equal(1, canceled)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

    end)

    before_each(function(done) async()
      events = assert.qless_class('Events', client:events())
      client:queue('foo'):put('Foo', {}, {jid='jid'}, function(_, err, jid)
        assert_nil(err) assert_equal('jid', jid)
        client:job(jid, function(_, err, job)
          assert_nil(err) assert.qless_class('Job', job)
          job:track(function(_, err)
            assert_nil(err) done()
          end)
        end)
      end)
    end)

    after_each(function(done) async()
      if events then
        events:close(function() done() end)
      else
        done()
      end
    end)
  end)

  describe('Basic tests about the Job classes', function()
    local queue

    describe('Test the Job class', function()

      it('Has all the basic attributes we would expect', function(done) async()
        local atts = {'data', 'jid', 'priority', 'klass', 'queue_name', 'tags',
            'expires_at', 'original_retries', 'retries_left', 'worker_name',
            'dependents', 'dependencies'}
        queue:put('Foo', {whiz = 'bang'}, {jid='jid', tags={'foo'}, retries=3},
        function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            local values = {}
            for _, name in ipairs(atts) do values[name] = job[name] end
            assert_same({
              data = {whiz = 'bang'},
              dependencies = {},
              dependents = {},
              expires_at = 0,
              jid = 'jid',
              klass = 'Foo',
              original_retries = 3,
              priority = 0,
              queue_name = 'foo',
              retries_left= 3,
              tags =  {'foo'},
              worker_name = ''
            }, values)
            done()
          end)
        end)
      end)

      it('We can set a job`s priority', function(done) async()
        queue:put('Foo', {}, {jid='jid', priority=0}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            assert_equal(0, job.priority)
            assert.error(function() job.priority = 10 end)
            job:set_priority(10, function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert_equal(10, res)
              assert_equal(10, job.priority)
              client:job(jid, function(self, err, job)
                assert.qless_class('Job', job) assert_nil(err)
                assert_equal(10, job.priority)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes a queue object', function(done) async()
        queue:put('Foo', {}, {jid='jid', priority=0}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            local q = assert.qless_class('Queue', job:queue())
            assert.equal('foo', q.name)
            assert.equal(q, job:queue())
            done()
          end)
        end)
      end)

      it('Exposes the ttl for a job', function(done) async()
        client.config:set('heartbeat', 10, function(self, err)
          assert_nil(err)
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert_nil(err) assert_equal('jid', jid)
            queue:pop(function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              assert.truthy( job:ttl() <= 10 )
              assert.truthy( job:ttl() >= 9  )
              done()
            end)
          end)
        end)
      end)

      pending('todo', function()
        it('should fail access to invalid attributes', function() async()
          queue:put('Foo', {}, {jid='jid', priority=0}, function(self, err, jid)
            assert_nil(err) assert_equal('jid', jid)
            client:job(jid, function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              assert.error(function() job.foo = 1 end)
              assert.error(function() local s = job.boo end)
              done()
            end)
          end)
        end)
      end)

      it('Exposes the cancel method', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:cancel(function(self, err, res)
              assert_nil(err) assert.equal(jid, res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert_nil(job)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes a way to tag and untag a job', function(done) async()
        --! @note Python and Ruby API seems do not sync job.tags.
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:tag('foo', function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert.same({'foo'}, res)
              assert.same({'foo'}, self.tags)
              job:tag('boo', function(self, err, res)
                assert_equal(job, self) assert_nil(err) assert.same({'foo', 'boo'}, res)
                assert.same({'foo', 'boo'}, self.tags)
                job:untag('foo', function(self, err, res)
                  assert_equal(job, self) assert_nil(err) assert.same({'boo'}, res)
                  assert.same({'boo'}, self.tags)
                  job:untag('foo', function(self, err, res)
                    assert_equal(job, self) assert_nil(err) assert.same({'boo'}, res)
                    assert.same({'boo'}, self.tags)
                    job:untag('boo', function(self, err, res)
                      assert_equal(job, self) assert_nil(err) assert.same({}, res)
                      assert.same({}, self.tags)
                      job:tag('foo', 'boo', function(self, err, res)
                        assert_equal(job, self) assert_nil(err) assert.same({'foo', 'boo'}, res)
                        assert.same({'foo', 'boo'}, self.tags)
                        job:untag('foo', 'boo', function(self, err, res)
                          assert_equal(job, self) assert_nil(err) assert.same({}, res)
                          assert.same({}, self.tags)
                          done()
                        end)
                      end)
                    end)
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

      it('Able to move jobs through the requeue method', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:requeue('bar', function(self, err, res)
              assert_nil(err) assert.equal('bar', res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert_equal('bar', job.queue_name)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Able to complete a job', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:complete(function(self, err, res)
              assert_nil(err) assert.equal('complete', res)
              assert.truthy(job.state_changed)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert.equal('complete', job.state)
                done()
              end)
            end)
            assert.falsy(job.state_changed)
          end)
        end)
      end)

      it('State changing if LuaError rised on complete', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)

          client:job('jid', function(self, err, job)
            queue:pop(function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              job:cancel(function(self, err, res)
                assert_nil(err) assert.same(job.jid, res)
              end)
              assert_nil(err) assert.qless_class('Job', job)
              job:complete(function(self, err, res)
                assert.qless_class('Error::LuaScript', err)
                assert.truthy(job.state_changed)
                done()
              end)
              assert.falsy(job.state_changed)
            end)
          end)
        end)
      end)

      it('Able to advance a job to another queue', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:complete('bar', function(self, err, res)
              assert_nil(err) assert.same('waiting', res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert_equal('bar', job.queue_name)
                assert_equal('waiting', job.state)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Provides access to heartbeat', function(done) async()
        client.config:set('heartbeat', 10, function(self, err)
          assert_nil(err)
          queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
            assert_nil(err) assert_equal('jid', jid)
            queue:pop(function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              local before = job:ttl()
              client.config:set('heartbeat', 20, function(self, err)
                assert_nil(err)
                job:heartbeat(function(self, err, res)
                  assert_equal(job, self) assert_nil(err) assert.number(res)
                  assert.truthy(job:ttl() > before)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

      it('Failed heartbeats raise an error', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)

            local called = 0
            job:on('lock_lost', function() called = called + 1 end)

            job:heartbeat(function(self, err, res)
              assert_equal(job, self)
              assert.qless_class('Error::LockLost', err)
              uv.defer(function()
                assert.equal(1, called)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes a track, untrack method', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:track(function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert.equal('1', res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert.is_true(job.tracked)
                job:untrack(function(self, err, res)
                  assert_equal(job, self) assert_nil(err) assert.equal('1', res)
                  client:job(jid, function(self, err, job)
                    assert_nil(err) assert.qless_class('Job', job)
                    assert.is_false(job.tracked)
                    done()
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

      it('Exposes a depend, undepend methods', function(done) async()
        queue:put('Foo', {}, {jid='a'}, function(self, err, jid)
          assert_nil(err) assert_equal('a', jid)
        end)
        queue:put('Foo', {}, {jid='b'}, function(self, err, jid)
          assert_nil(err) assert_equal('b', jid)
        end)
        queue:put('Foo', {}, {jid='c', depends = {'a'}}, function(self, err, jid)
          assert_nil(err) assert_equal('c', jid)
        end)
        client:job('c', function(self, err, job)
          assert_nil(err) assert.qless_class('Job', job)
          assert.same({'a'}, job.dependencies)
          job:depend('b', function(self, err, res)
            assert_equal(job, self) assert_nil(err) assert_equal(1, res)
            client:job('c', function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              assert.same({'a', 'b'}, job.dependencies)
              job:undepend('a', function(self, err, res)
                assert_equal(job, self) assert_nil(err) assert_equal(1, res)
                client:job('c', function(self, err, job)
                  assert_nil(err) assert.qless_class('Job', job)
                  assert.same({'b'}, job.dependencies)
                  job:undepend('all', function(self, err, res)
                    assert_equal(job, self) assert_nil(err) assert_equal(1, res)
                    client:job('c', function(self, err, job)
                      assert_nil(err) assert.qless_class('Job', job)
                      assert.same({}, job.dependencies)
                      done()
                    end)
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

      it('Retry raises an error if retry fails', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:retry(function(self, err)
              assert_equal(job, self)
              assert.qless_class('Error::LuaScript', err)
              assert.truthy(job.state_changed)
              done()
            end)
            assert.falsy(job.state_changed)
          end)
        end)
      end)

      it('Can supply a group and message when retrying', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:retry(0, 'group', 'message', function(self, err)
              assert_equal(job, self) assert_nil(err)
              assert.truthy(job.state_changed)
              client:job('jid', function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert.equal('group', job.failure.group)
                assert.equal('message', job.failure.message)
                done()
              end)
            end)
            assert.falsy(job.state_changed)
          end)
        end)
      end)

      it('Raises an error if the class doesn`t have the method', function(done) async()
        queue:put('Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:perform(function(self, err, res)
              assert.qless_class('Error::General', err)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert_equal('failed', job.state)
                assert_equal('foo-method-missing', job.failure.group)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Raises an error if it can`t import the module', function(done) async()
        queue:put('foo.Foo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:perform(function(self, err, res)
              assert.qless_class('Error::General', err)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert_equal('failed', job.state)
                assert_equal('foo-NameError', job.failure.group)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Raises an error if it can`t import the module', function(done) async()
        local called, timer = 0

        KlassUtils.load('Boo', {
          perform = function(job, job_done)
            timer = uv.timer():start(1000, function()
              uv.defer(function()
                assert.equal(2, called)
                done()
              end)
              -- we ignore callback calls
              job_done()
            end)
            error('some error')
          end
        })

        queue:put('Boo', {}, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          queue:pop(function(self, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:perform(function(self, err, res)
              called = called + 1
              assert.qless_class('Error::General', err)
              assert.match('some error', err)
              -- in fact we can not stop execution this action
              assert.truthy(timer:active())
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                assert_equal('failed', job.state)
                assert_equal('foo-Boo', job.failure.group)
                assert.match('some error', job.failure.message)
                called = called + 1
                -- wait until timer fire
              end)
            end)
          end)
        end)
      end)

      it('Allows subscribe to multiple events', function(done) async()
        queue:put('Foo', {}, function(_, err, jid) assert_nil(err)
          client:job(jid, function(_, err, job) assert_nil(err)
            local events = {}
            job:on({'lock_lost', 'canceled'}, function(self, event)
              events[#events + 1] = event
            end)
            job:emit('lock_lost')
            job:emit('canceled')
            job:emit('foo')
            assert.same({'lock_lost', 'canceled'}, events)
            done()
          end)
        end)
      end)

      before_each(function()
        KlassUtils.preload('Foo', {})
      end)

      after_each(function()
        KlassUtils.unload('Foo')
        KlassUtils.unload('Boo')
      end)
    end)

    describe('Test the RecurJob class', function()
      it('We can access all the recurring attributes', function(done) async()
        local atts = {'data', 'jid', 'priority', 'klass', 'queue_name', 'tags',
            'retries', 'interval', 'count'}
        queue:recur('Foo', {whiz = 'bang'}, 60, {jid='jid', tags={'foo'}, retries=3},
        function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            local values = {}
            for _, name in ipairs(atts) do values[name] = job[name] end
            assert_same({
              count = 0,
              data = {whiz = 'bang'},
              interval = 60,
              jid = 'jid',
              klass = 'Foo',
              priority = 0,
              queue_name = 'foo',
              retries = 3,
              tags = {'foo'}
            }, values)
            done()
          end)
        end)
      end)

      it('We can set a job`s priority', function(done) async()
        queue:recur('Foo', {}, 60, {jid='jid', priority=0}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            assert_equal(0, job.priority)
            assert.error(function() job.priority = 10 end)
            job:set_priority(10, function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert_equal(10, res)
              assert_equal(10, job.priority)
              client:job(jid, function(self, err, job)
                assert.qless_class('RecurJob', job) assert_nil(err)
                assert_equal(10, job.priority)
                done()
              end)
            end)
          end)
        end)
      end)

      it('We can set retries', function(done) async()
        queue:recur('Foo', {}, 60, {jid='jid', retries=3}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            assert_equal(3, job.retries)
            assert.error(function() job.retries = 2 end)
            job:set_retries(2, function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert_equal(2, res)
              assert_equal(2, job.retries)
              client:job(jid, function(self, err, job)
                assert.qless_class('RecurJob', job) assert_nil(err)
                assert_equal(2, job.retries)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes the next time a job will run', function(done) async()
        queue:recur('Foo', {}, 60, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            job:get_next(function(self, err, nxt)
              assert_equal(job, self) assert_nil(err) assert.number(nxt)
              queue:pop(function(self, err, job)
                assert_nil(err) assert.qless_class('Job', job)
                client:job(jid, function(self, err, job)
                  assert_nil(err) assert.qless_class('RecurJob', job)
                  job:get_next(function(self, err, nxt2)
                    assert_equal(job, self) assert_nil(err) assert.number(nxt2)
                    assert.truthy(math.abs(nxt2 - nxt - 60) <= 1)
                    done()
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

      it('Able to move jobs through the requeue method', function(done) async()
        queue:recur('Foo', {}, 60, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            job:requeue('bar', function(self, err, res)
              assert_nil(err) assert.same('bar', res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert.qless_class('RecurJob', job)
                assert_equal('bar', job.queue_name)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes the cancel method', function(done) async()
        queue:recur('Foo', {}, 60, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            job:cancel(function(self, err, res)
              assert_nil(err) assert.same(REDIS_ARRAY{jid}, res)
              client:job(jid, function(self, err, job)
                assert_nil(err) assert_nil(job)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes a way to tag and untag a job', function(done) async()
        --! @note Python and Ruby API seems do not sync job.tags.
        queue:recur('Foo', {}, 60, {jid='jid'}, function(self, err, jid)
          assert_nil(err) assert_equal('jid', jid)
          client:job(jid, function(self, err, job)
            assert_nil(err) assert.qless_class('RecurJob', job)
            job:tag('foo', function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert.same({'foo'}, res)
              assert.same({'foo'}, self.tags)
              job:tag('boo', function(self, err, res)
                assert_equal(job, self) assert_nil(err) assert.same({'foo', 'boo'}, res)
                assert.same({'foo', 'boo'}, self.tags)
                job:untag('foo', function(self, err, res)
                  assert_equal(job, self) assert_nil(err) assert.same({'boo'}, res)
                  assert.same({'boo'}, self.tags)
                  job:untag('foo', function(self, err, res)
                    assert_equal(job, self) assert_nil(err) assert.same({'boo'}, res)
                    assert.same({'boo'}, self.tags)
                    job:untag('boo', function(self, err, res)
                      assert_equal(job, self) assert_nil(err) assert.same({}, res)
                      assert.same({}, self.tags)
                      job:tag('foo', 'boo', function(self, err, res)
                        assert_equal(job, self) assert_nil(err) assert.same({'foo', 'boo'}, res)
                        assert.same({'foo', 'boo'}, self.tags)
                        job:untag('foo', 'boo', function(self, err, res)
                          assert_equal(job, self) assert_nil(err) assert.same({}, res)
                          assert.same({}, self.tags)
                          done()
                        end)
                      end)
                    end)
                  end)
                end)
              end)
            end)
          end)
        end)
      end)

    end)

    before_each(function(done) async()
      queue = assert.qless_class('Queue', client:queue('foo'))
      done()
    end)

    after_each(function(done) async()
      queue = nil
      done()
    end)

  end)

  describe('Basic tests about the Queue class', function()
    local queue

    describe('Test the QueueJobs class', function()
      it('The queue.Jobs class provides access to job counts', function(done) async()
        local jobs = queue.jobs
        jobs:depends(function(self, err, res)
          assert_equal(jobs, self) assert_nil(err) assert_same(REDIS_ARRAY{}, res)
          jobs:running(function(self, err, res)
            assert_equal(jobs, self) assert_nil(err) assert_same(REDIS_ARRAY{}, res)
            jobs:stalled(function(self, err, res)
              assert_equal(jobs, self) assert_nil(err) assert_same(REDIS_ARRAY{}, res)
              jobs:scheduled(function(self, err, res)
                assert_equal(jobs, self) assert_nil(err) assert_same(REDIS_ARRAY{}, res)
                jobs:recurring(function(self, err, res)
                  assert_equal(jobs, self) assert_nil(err) assert_same(REDIS_ARRAY{}, res)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)
    end)
  
    describe('Test the Queue class', function()
      it('Provides access to job counts', function(done) async()
        queue:put('Foo', {}, function(_, err, jid)
          assert_nil(err)
          queue:counts(function(self, err, res)
            assert_equal(queue, self) assert_nil(err)
            assert_same({
              depends   = 0,
              name      = 'foo',
              paused    = false,
              recurring = 0,
              running   = 0,
              scheduled = 0,
              stalled   = 0,
              waiting   = 1
            }, res)
            done()
          end)
        end)
      end)

      it('Queue can be paused/unpaused', function(done) async()
        queue:pause(function(self, err, res)
          assert_equal(queue, self) assert_nil(err) assert_nil(res)
          queue:counts(function(self, err, res)
            assert_equal(queue, self) assert_nil(err) assert.table(res)
            assert.truthy(res.paused)
            queue:unpause(function(self, err, res)
              assert_equal(queue, self) assert_nil(err) assert_nil(res)
              queue:counts(function(self, err, res)
                assert_equal(queue, self) assert_nil(err) assert.table(res)
                assert.falsy(res.paused)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Provided access to heartbeat configuration', function(done) async()
        queue:get_heartbeat(function(self, err, original)
          assert_equal(queue, self) assert_nil(err) assert.number(original)
          queue:set_heartbeat(10, function(self, err, res)
            assert_equal(queue, self) assert_nil(err) assert_nil(res)
            queue:get_heartbeat(function(self, err, new)
              assert_equal(queue, self) assert_nil(err) assert.number(new)
              assert.not_equal(original, new)
              -- we change value only for this particular queue
              queue = assert.qless_class('Queue', client:queue('boo'))
              queue:get_heartbeat(function(self, err, res)
                assert_equal(queue, self) assert_nil(err) assert.equal(original, res)
                done()
              end)
            end)
          end)
        end)
      end)

      it('Exposes multi-pop', function(done) async()
        queue:put('Foo', {}, function(self, err, jid) assert_nil(err)
          queue:put('Foo', {}, function(self, err, jid) assert_nil(err)
            queue:pop(10, function(self, err, jobs)
              assert_equal(queue, self) assert_nil(err) assert.table(jobs)
              assert.qless_class('Job', jobs[1])
              assert.qless_class('Job', jobs[2])
              assert.not_equal(jobs[1].jid, jobs[2].jid)
              assert_nil(jobs[3])
              done()
            end)
          end)
        end)
      end)

      it('Exposes queue peeking', function(done) async()
        queue:put('Foo', {}, function(self, err, jid) assert_nil(err)
          queue:peek(function(self, err, job)
            assert_equal(queue, self) assert_nil(err) assert.qless_class('Job', job)
            assert_equal(jid, job.jid)
            assert_equal('waiting', job.state)
            client:job(jid, function(self, err, job)
              assert_nil(err) assert.qless_class('Job', job)
              assert_equal(jid, job.jid)
              assert_equal('waiting', job.state)
              done()
            end)
          end)
        end)
      end)

      it('Exposes queue multi-peeking', function(done) async()
        queue:put('Foo', {}, function(self, err, jid) assert_nil(err)
          queue:put('Foo', {}, function(self, err, jid) assert_nil(err)
            queue:peek(10, function(self, err, jobs)
              assert_equal(queue, self) assert_nil(err) assert.table(jobs)
              assert.qless_class('Job', jobs[1])
              assert.qless_class('Job', jobs[2])
              assert.not_equal(jobs[1].jid, jobs[2].jid)
              assert_equal('waiting', jobs[1].state)
              assert_equal('waiting', jobs[2].state)
              done()
            end)
          end)
        end)
      end)

      it('Exposes stats', function(done) async()
        queue:stats(function(self, err, res)
          assert_equal(queue, self) assert_nil(err) assert.table(res)
          done()
        end)
      end)

      it('Exposes the length of a queue', function(done) async()
        queue:put('Foo', {}, function(_, err) assert_nil(err)
          queue:recur('Foo', {}, 60, function(_, err) assert_nil(err)
            queue:put('Foo', {}, function(_, err) assert_nil(err)
              queue:pop(function(_, err) assert_nil(err)
                queue:length(function(self, err, len)
                  assert_equal(queue, self) assert_nil(err)
                  assert_equal(3, len)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

    end)

    before_each(function(done) async()
      queue = assert.qless_class('Queue', client:queue('foo'))
      done()
    end)

    after_each(function(done) async()
      queue = nil
      done()
    end)

  end)

  describe('Basic tests about the Reserver class', function()
    local q1, q2, q3

    describe('Test the ordered reserver', function()
      it('should raise error if no queues provided',function(done) async()
        assert.error(function() QLess.Reserver.Ordered.new() end)
        done()
      end)

      it('should respect change queues array',function(done) async()
        local queues = {}
        local reserver = QLess.Reserver.Ordered.new(queues)
        queues[#queues + 1] = q1

        q1:put('Foo', {}, function(_, err) assert_nil(err)
          reserver:restart(function(self, err, job)
            assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
            done()
          end)
        end)
      end)

      it('should move to next queue only if current is empty',function(done) async()
        local queues = {}
        local reserver = QLess.Reserver.Ordered.new(queues)
        queues[#queues + 1] = q1
        queues[#queues + 1] = q2

        q1:put('Foo', {}, {jid = 'q1-jid-1'}, function(_, err) assert_nil(err) end)
        q1:put('Foo', {}, {jid = 'q1-jid-2'}, function(_, err) assert_nil(err) end)
        q2:put('Foo', {}, {jid = 'q2-jid-1'}, function(_, err) assert_nil(err)
          reserver:restart(function(self, err, job)
            assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
            assert_equal('q1-jid-2', job.jid)
            reserver:reserve(function(self, err, job)
              assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
              assert_equal('q1-jid-1', job.jid)
              reserver:reserve(function(self, err, job)
                assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
                assert_equal('q2-jid-1', job.jid)
                reserver:reserve(function(self, err, job)
                  assert_equal(reserver, self) assert_nil(err) assert_nil(job)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

      it('should not auto restart from first queue',function(done) async()
        local queues = {}
        local reserver = QLess.Reserver.Ordered.new(queues)
        queues[#queues + 1] = q1
        queues[#queues + 1] = q2

        q2:put('Foo', {}, {jid = 'q2-jid-1'}, function(_, err) assert_nil(err) end)
        q2:put('Foo', {}, {jid = 'q2-jid-2'}, function(_, err) assert_nil(err)
          reserver:restart(function(self, err, job)
            assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
            assert_equal('q2-jid-2', job.jid)
            q1:put('Foo', {}, {jid = 'q1-jid-1'}, function(_, err) assert_nil(err) end)
            reserver:reserve(function(self, err, job)
              assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
              assert_equal('q2-jid-1', job.jid)
              reserver:reserve(function(self, err, job)
                assert_equal(reserver, self) assert_nil(err) assert_nil(job)
                reserver:restart(function(self, err, job)
                  assert_equal(reserver, self) assert_nil(err) assert.qless_class('Job', job)
                  assert_equal('q1-jid-1', job.jid)
                  done()
                end)
              end)
            end)
          end)
        end)
      end)

    end)

    before_each(function(done) async()
      q1 = assert.qless_class('Queue', client:queue('q1'))
      q2 = assert.qless_class('Queue', client:queue('q2'))
      q3 = assert.qless_class('Queue', client:queue('q3'))
      done()
    end)

    after_each(function(done) async()
      q1, q2, q3 = nil
      done()
    end)

  end)

  describe('Basic tests about the Workers classes', function()
    describe('Test the Serial worker', function()
      local worker, queue, timeout

      it('worker should limits the number of jobs it runs', function(done) async()
        worker = assert.qless_class('Worker::Serial', QLess.Worker.Serial.new{
          redis     = client:new_redis_connection();
          logger    = client.logger;
          queues    = {'foo'};
          concurent = 3;
        })
        local n = 5
        for i = 1, n do queue:put('Foo', {}, function(_, err) assert_nil(err) end) end

        local concurent, max_concurent, total = 0, 0, 0

        local function done_test()
          assert_equal(3, max_concurent)
          assert_equal(0, concurent)
          assert_equal(n, total)
          worker:shutdown()
          done()
        end

        KlassUtils.preload('Foo', {perform=function(job, done)
          concurent = concurent + 1
          if max_concurent < concurent then max_concurent = concurent end
          uv.timer():start(500, function()
            concurent = concurent - 1
            total = total + 1
            done()
            if total == n then uv.defer(done_test) end
          end):unref()
        end})

        worker:run()
      end)

      it('worker should notify jobs about lock lost', function(done) async()
        worker = assert.qless_class('Worker::Serial', QLess.Worker.Serial.new{
          redis     = client:new_redis_connection();
          logger    = client.logger;
          queues    = {'foo'};
        })

        queue:put('Foo', {}, function(_, err) assert_nil(err) end)

        local timer, timeout_called, lock_called

        local function done_test()
          worker:shutdown()
          timer:close()
          uv.defer(function() done() end)
          assert.truthy(timeout_called)
          assert.truthy(lock_called)
        end

        timer = uv.timer():start(500, function()
          uv.defer(done_test)
        end):unref()

        KlassUtils.preload('Foo', {perform=function(job, done)
          -- mark job as timeout
          client:job(job.jid, function(_, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            job:timeout(function(self, err, res)
              assert_equal(job, self) assert_nil(err) assert_nil(res)
              timeout_called = true
            end)
          end)

          job:on('lock_lost', function()
            job:off('lock_lost')
            lock_called = true
            uv.defer(done_test)
          end)
        end})

        worker:run()
      end)

      it('worker should apply class prefix', function(done) async()
        worker = assert.qless_class('Worker::Serial', QLess.Worker.Serial.new{
          redis        = client:new_redis_connection();
          logger       = client.logger;
          klass_prefix = 'Boo.';
          queues       = {'foo'};
        })

        queue:put('Foo', {}, function(_, err) assert_nil(err) end)

        local timer, called

        local function done_test()
          worker:shutdown()
          timer:close()
          uv.defer(function() done() end)
          assert.truthy(called)
        end

        timer = uv.timer():start(5000, function()
            uv.defer(done_test)
        end):unref()

        KlassUtils.preload('Boo.Foo', {perform=function(job, done)
          called = true
          uv.defer(done_test)
        end})

        worker:run()
      end)

      before_each(function()
        timeout = loop.set_timeout(20)
        queue = assert.qless_class('Queue', client:queue('foo'))
      end)

      after_each(function(done) async()
        KlassUtils.unload('Foo')
        KlassUtils.unload('Foo.Boo')
        loop.set_timeout(timeout)
        if worker then
          worker:close(function() done() end)
          worker = nil
        else 
          done()
        end
      end)
    end)
  end)

  describe('Test reconnect to Redis', function()
    local timeout
    local ENOTCONN = uv.error('LIBUV', uv.ENOTCONN)
    local ECONNRESET = uv.error('LIBUV', uv.ECONNRESET)

    local function reset_connection(object, err)
      err = err or ECONNRESET
      --! @fixme do not use private `stream` object
      object._redis._stream:halt(err)
    end

    describe('Client reconnect', function()
      it('Client should reconnect', function(done) async()
        local c1, c2
      
        client:job('jid', function(_, err)
          assert.same(ECONNRESET, err)
          c1 = true
        end)

        reset_connection(client)

        client:job('jid', function(_, err)
          assert.same(ECONNRESET, err)
          c2 = true
        end)

        uv.timer():start(8000, function()
          assert.truthy(c1)
          assert.truthy(c2)
          client:job('jid', function(_, err)
            assert_nil(err)
            done()
          end)
        end)
      end)
    end)

    describe('Events reconnect', function()
      it('Events should reconnect and resubscribe', function(done) async()
        local popped, c1, c2 = 0

        local queue = client:queue('foo')
        queue:put('Foo', {}, function(_, err, jid) assert_nil(err)
          client:job(jid, function(_, err, job) assert_nil(err)
            job:track(function(_, err) assert_nil(err)
              c1 = true
            end)
          end)
        end)

        events = client:events()

        events:on('popped', function(self, event, data)
          assert_equal(events, self) assert_equal('popped', event)
          popped = popped + 1
        end)

        events:subscribe({'canceled', 'timeout', 'popped'}, function(_, err)
          assert.same(ECONNRESET, err)
          c2 = true
        end)

        --! @fixme do not use private `stream` object
        reset_connection(events)

        uv.timer():start(8000, function()
          assert.truthy(c1)
          assert.truthy(c2)
          queue:pop(function(_, err, job)
            assert_nil(err) assert.qless_class('Job', job)
            uv.timer():start(500, function()
              assert_equal(1, popped)
              done()
            end)
          end)
        end)
      end)

      after_each(function(done) async()
        if events then
          events:close(function() done() end)
          events = nil
        else done() end
      end)

    end)

    before_each(function()
      timeout = loop.set_timeout(15)
    end)

    after_each(function()
      loop.set_timeout(timeout)
    end)

  end)

  before_each(function(done) async()
    TestSetup.before_each({}, function(ctx)
      client, redis = ctx.client, ctx.redis
      done()
    end)
  end)

  after_each(function(done) async()
    local ctx = {client = client, redis = redis}
    redis, client = nil
    TestSetup.after_each(ctx, function(ctx) done() end)
  end)

  setup(function(done) async()
    TestSetup.setup({}, function(ctx) done() end)
  end)

end)
