# lua-lluv-redis
[![Licence](http://img.shields.io/badge/Licence-MIT-brightgreen.svg)](LICENSE)
[![Build Status](https://travis-ci.org/moteus/lua-lluv-redis.svg?branch=master)](https://travis-ci.org/moteus/lua-lluv-redis)
[![Coverage Status](https://coveralls.io/repos/moteus/lua-lluv-redis/badge.svg)](https://coveralls.io/r/moteus/lua-lluv-redis)

```Lua
-- Using stream decoder with lluv
local uv          = require "lluv"
local RedisStream = require "lluv.redis.stream"

uv.tcp():connect("127.0.0.1", 6379, function(cli, err)
  local stream = RedisStream.new()
  :on_command(function(self, msg, cb)
    return cli:write(msg)
  end)

  cli:start_read(function(cli, err, data)
    stream
      :append(data)
      :execute()
  end)

  stream:command("PING", function(...)
    print("PING:", ...)
  end)

  local msg = '"Hello, world!!!"'
  stream:command({"ECHO", msg}, function(...)
    print("ECHO:", ...)
  end)

  stream:command("PING2", function(...)
    print("ERROR:", ...)
    uv.stop()
  end)

end)

uv.run(debug.traceback)
```
