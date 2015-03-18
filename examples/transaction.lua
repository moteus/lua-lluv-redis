local uv    = require "lluv"
local redis = require "lluv.redis"

local cli = redis.Connection.new()

cli:open(function()
  cli:multi(function(cli, err, data) -- begin transaction
    if err and err:cat() == 'REDIS' then
      -- we try begin transaction in wrong state
      -- application error and should be fixed in client code
      error("Please fix me")
    end

    print("MULTI:", err or data)
  end)

  -- we can proceed each command in separate callback
  cli:set("a", "10", function(cli, err, data)
    print("SET:", data)
  end)

  cli:ping() --or we can ignore callback

  cli:exec(function(cli, err, res)   -- end transaction
    -- proceed all results in transaction
    for k, v in ipairs(res) do print("CMD #" .. k, v) end
  end)

  cli:quit()
end)

uv.run()
