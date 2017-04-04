# lua-lluv-redis
[![Licence](http://img.shields.io/badge/Licence-MIT-brightgreen.svg)](LICENSE)
[![Build Status](https://travis-ci.org/moteus/lua-lluv-redis.svg?branch=master)](https://travis-ci.org/moteus/lua-lluv-redis)
[![Coverage Status](https://coveralls.io/repos/moteus/lua-lluv-redis/badge.svg)](https://coveralls.io/r/moteus/lua-lluv-redis)

## Usage

### lluv client
```Lua
local cli = redis.Connection.new()
cli:open(function(cli)
  cli:ping(print)
  cli:quit(print)
end)
```

### basic transaction
```Lua
cli:open(function()
  cli:multi(function(cli, err, data) -- begin transaction
    -- We can cat IO/redis error
    -- if we get redis error that means application error
    -- try nested transaction or transaction in SUBSCRIBE mode
    -- so application shoul fix it.
    assert(not err or err:cat() ~= 'REDIS')
  end)

  -- we can proceed each command in separate callback
  cli:set("a", "10", function(cli, err, data)
    print("SET:", data)
  end)

  cli:ping() --or we can ignore callback

  cli:exec(function(cli, err, res)   -- end transaction
    -- and proceed all results in transaction
    for k, v in ipairs(res) do print("CMD #" .. k, v) end
  end)

  cli:quit()
end)
```

### pipeline
```lua
-- new pipeline
p = cli:pipeline()
  :set(a, 10)
  :set(b, 20)

-- execute and preserve pipeline
p:execute(true)

-- append command
p:set(c, 30)

-- and execute it again
p:execute() -- execute and clear

p -- set new commands and execute
  :set(a, 20)
  :set(b, 30)
  :execute()
```
