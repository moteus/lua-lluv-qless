local QLess = require "lluv.qless"
local uv    = require "lluv"
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

  describe('Tests about events', function()
    local events

    describe('Ensure we can get a basic event', function()
      it('Basic set/get/unset', function(done) async()
        local popped = 0

        events:on('popped', function(self, event, data)
          assert_equal(events, self) assert_equal('popped', event)
          popped = popped + 1
        end)

        events:subscribe({'popped'}, function(self, err)
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
              done()
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
              assert_nil(err) assert.same(A{jid}, res)
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
            job:requeue('bar', function(self, err, jid)
              assert_nil(err) assert.same(job.jid, jid)
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
                assert_nil(err) assert.same(A{'jid'}, res)
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
local say   = require "say"

local getmetatable = getmetatable

local Classes = {
  LuaScript             = require "lluv.qless.script";
  Job                   = require "lluv.qless.job";
  RecurJob              = require "lluv.qless.rjob";
  Jobs                  = require "lluv.qless.jobs";
  Queue                 = require "lluv.qless.queue";
  Events                = require "lluv.qless.events";
  Client                = require "lluv.qless.client";
  Config                = require "lluv.qless.config";
  ["Reserver::Ordered"] = require "lluv.qless.reserver.ordered";
  ["Worker::Serial"]    = require "lluv.qless.worker.serial";
  ["Error::General"]    = require "lluv.qless.error".General;
  ["Error::LuaScript"]  = require "lluv.qless.error".LuaScript;
  ["Error::LockLost"]   = require "lluv.qless.error".LockLost;
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
