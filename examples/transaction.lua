local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()
cli:open(function()
  cli:multi(function(cli, err, data) -- begin transaction
    print("MULTI:", data)

    -- we can proceed each command in separate callback
    cli:set("a", "10", function(cli, err, data)
      print("SET:", data)
    end)

    cli:ping() --we can ignore callback

    cli:exec(function(cli, err, res)
      -- proceed all results in transaction
      for k, v in ipairs(res) do print("CMD #" .. k, v) end

      cli:quit()
    end)
  end)
end)

uv.run()
