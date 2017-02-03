-- Using stream decoder with lluv
local uv          = require "lluv"
local RedisStream = require "lluv.redis.stream"

uv.tcp():connect("127.0.0.1", 6379, function(cli, err)
  local stream stream = RedisStream.new()
  :on_command(function(self, msg, cb)
    return cli:write(msg, function(_, err)
      if err then return stream:halt(err) end
    end)
  end)
  :on_halt(function(self, err)
    cli:close()
  end)

  cli:start_read(function(cli, err, data)
    if err then return stream:halt(err) end
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
  end)

  stream:command("*3\r\n$3\r\nSET\r\n$1\r\na\r\n+1", function(...)
    print("SET:", ...)
  end)

  stream:command("QUIT", function(...)
    print("QUIT:", ...)
  end)

end)

uv.run(debug.traceback)
