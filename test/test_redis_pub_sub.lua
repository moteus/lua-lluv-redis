pcall(require, "luacov")

local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()
cli:open(function()
  cli:on('message', function(cli, event, chan, msg)
    assert(chan == 'hello')
    assert(msg  == 'world')
    print("Done!")
    uv.stop()
  end)

  cli:on('subscribe', function(cli, event, chan, count)
    assert(chan  == 'hello')
    assert(count == 1)
  end)

  cli:subscribe("hello")
end)

local cli = redis.Connection.new()
cli:open(function()
  uv.timer():start(1000, 5000, function()
    cli:publish("hello", "world")
  end)
end)

uv.timer():start(3000, function()
  os.exit(-1)
end)

uv.run()
