local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()
cli:open(function()
  cli:on('message', function(...)
    print("NEW MESSAGE:", ...)
  end)
  cli:subscribe("hello")
end)

local cli = redis.Connection.new()
cli:open(function()
  uv.timer():start(1000, 5000, function()
    cli:publish("hello", "world")
  end)
end)

uv.run()
