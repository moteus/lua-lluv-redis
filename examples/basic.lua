local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()
cli:open(function()
  cli:ping(print)
  cli:quit(print)
end)

uv.run()
