------------------------------------------------------------------
--
--  Author: Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Copyright (C) 2015 Alexey Melnichuk <alexeymelnichuck@gmail.com>
--
--  Licensed according to the included 'LICENSE' document
--
--  This file is part of lua-lluv-redis library.
--
------------------------------------------------------------------

local uv             = require "lluv"
local ut             = require "lluv.utils"
local RedisStream    = require "lluv.redis.stream"
local RedisCommander = require "lluv.redis.commander"

local function ocall(fn, ...) if fn then return fn(...) end end

local EOF   = uv.error("LIBUV", uv.EOF)

local function split_host(server, def_host, def_port)
  if not server then return def_host, def_port end
  local host, port = string.match(server, "^(.-):(%d*)$")
  if not host then return server, def_port end
  if #port == 0 then port = def_port end
  return host, port
end

-------------------------------------------------------------------
local Connection = ut.class() do

function Connection:__init(server)
  self._host, self._port = split_host(server, "127.0.0.1", "6379")
  self._stream           = RedisStream.new(self)
  self._commander        = RedisCommander.new(self._stream)

  local function on_write_error(cli, err)
    if err then self._stream:halt(err) end
  end

  self._stream
  :on_command(function(s, data, cb)
    return self._cnn:write(data, on_write_error)
  end)
  :on_halt(function(s, err)
    self:close(err)
    if err ~= EOF then
      ocall(self._on_error, self, err)
    end
  end)

  return self
end

function Connection:connected()
  return not not self._cnn
end

function Connection:open(cb)
  if self:connected() then return ocall(cb, self) end

  local ok, err =uv.tcp():connect(self._host, self._port, function(cli, err)
    if err then
      cli:close()
      return ocall(cb, self, err)
    end

    self._cnn = cli

    cli:start_read(function(cli, err, data)
      if err then return self._stream:halt(err) end
      self._stream:append(data):execute()
    end)

    return ocall(cb, self)
  end)
  if not ok then return nil, err end
  return self
end

function Connection:close(err)
  if not self:connected() then return end
  self._cnn:close()
  self._stream:reset(err or EOF)
  self._stream, self._cnn = nil
end

function Connection:on_error(handler)
  self._on_error = handler
  return self
end

function Connection:__tostring()
  return string.format("Lua UV Redis (%s)", tostring(self._cnn))
end

RedisCommander.commands(function(name)
  name = name:lower()
  Connection[name] = function(self, ...)
    return self._commander[name](self._commander, ...)
  end
end)

end
-------------------------------------------------------------------

return {
  Connection = Connection;
}
