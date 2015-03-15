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
