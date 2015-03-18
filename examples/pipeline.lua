local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()

cli:open(function()
  local pip = cli:pipeline()
  pip
    :echo("Hello", print)
    :echo("World", print)

  pip:execute(true) -- do not clear pipeline
  pip:execute()     -- so we can execute again

  cli:quit(print)
end)

uv.run(debug.traceback)
