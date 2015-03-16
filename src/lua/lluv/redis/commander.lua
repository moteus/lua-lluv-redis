local ut     = require "lluv.utils"
local Stream = require "lluv.redis.stream"

local function bind_converter(fn, self, decoder)
  return function(self, err, data)
    if err then return fn(self, err, data) end
    return fn(self, nil, decoder(data))
  end
end

local function make_single_command(cmd)
  return function(self, cb)
    return self._stream:command(cmd, cb)
  end
end

local function make_single_args_command(cmd)
  return function(self, arg, cb)
    return self._stream:command({n = 2, cmd, arg}, cb)
  end
end

local function pack_args(...)
  local n    = select("#", ...)
  local args = {n = n - 1, ...}
  local cb   = args[n]
  args[n] = nil
  return args, cb
end

local function make_multi_args_command(cmd)
  return function(self, ...)
    return self._stream:command(pack_args(cmd, ...))
  end
end

local function make_decoder_command(cmd, decoder)
  return function(self, cb)
    return self._stream:command(cmd, bind_converter(cb, self, decoder))
  end
end

local RedisCommander = ut.class() do

function RedisCommander:__init(stream)
  self._stream = stream or Stream.new()

  return self
end

RedisCommander.ping = make_single_command("PING")

RedisCommander.echo = make_single_args_command("ECHO")

end

return {
  new = RedisCommander.new
}
