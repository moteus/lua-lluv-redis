pcall(require, "luacov")

local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()
cli:open(function()
  cli:on_message(function(cli, typ, chan, msg)
    if typ == 'message' then
      assert(typ  == 'message')
      assert(chan == 'hello')
      assert(msg  == 'world')
      print("Done!")
      uv.stop()
    else
      assert(typ  == 'subscribe')
      assert(chan == 'hello')
      assert(msg  == 1)
    end
  end)
  :subscribe("hello")
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
