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
  self._open_q           = ut.Queue.new()
  self._close_q          = ut.Queue.new()
  self._delay_q          = ut.Queue.new()
  self._ready            = false

  self._on_message       = nil
  self._on_error         = nil

  local function on_write_error(cli, err)
    if err then self._stream:halt(err) end
  end

  self._on_write_handler = on_write_error

  self._stream
  :on_command(function(s, data, cb)
    if self._ready then
      return self._cnn:write(data, on_write_error)
    end
    if self._cnn then
      self._delay_q:push(data)
      return true
    end
    error('Can not execute command on closed client', 3)
  end)
  :on_halt(function(s, err)
    self:close(err)
    if err ~= EOF then
      ocall(self._on_error, self, err)
    end
  end)
  :on_message(function(...)
    ocall(self._on_message, ...)
  end)

  return self
end

function Connection:connected()
  return not not self._cnn
end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

function Connection:open(cb)
  if self._ready then
    uv.defer(cb, self)
    return self
  end

  if not self._cnn then
    local ok, err = uv.tcp():connect(self._host, self._port, function(cli, err)
      if err then return self:close(err) end

      cli:start_read(function(cli, err, data)
        if err then return self._stream:halt(err) end
        self._stream:append(data):execute()
      end)

      while true do
        local data = self._delay_q:pop()
        if not data then break end
        cli:write(data, self._on_write_handler)
      end
      call_q(self._open_q, self)
      self._ready = true
    end)

    if not ok then return nil, err end
    self._cnn = ok
  end

  if cb then self._open_q:push(cb) end

  return self
end

function Connection:close(err, cb)
  if type(err) == 'function' then
    cb, err = err
  end

  if not self._cnn then
    if cb then uv.defer(cb, self) end
    return
  end

  if cb then self._close_q:push(cb) end

  if not (self._cnn:closed() or self._cnn:closing()) then
    local err = err
    self._cnn:close(function()
      self._cnn = nil
      call_q(self._close_q, self, err)
    end)
  end

  err = err or EOF
  call_q(self._open_q, self, err)
  self._stream:reset(err or EOF)
  self._delay_q:reset()
  self._ready = false
end

function Connection:pipeline()
  return self._commander:pipeline()
end

function Connection:on_error(handler)
  self._on_error = handler
  return self
end

function Connection:on_message(handler)
  self._on_message = handler
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
