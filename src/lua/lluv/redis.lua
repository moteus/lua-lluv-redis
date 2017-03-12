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
local EventEmitter   = require "EventEmitter"

local function ocall(fn, ...) if fn then return fn(...) end end

local EOF      = uv.error("LIBUV", uv.EOF)
local ENOTCONN = uv.error('LIBUV', uv.ENOTCONN)

local function nil_if_empty(t)
  if t and #t == 0 then return nil end
  return t
end

-- Redis url shold be `[<redis>://][<password>@]host[:<port>][/<db>]`
function decode_url(url)
  local scheme, pass, host, port, db

  scheme, url = ut.split_first(url, '://', true)
  if not url then url = scheme
  elseif scheme ~= 'redis' then
    error('unsupported scheme: ' .. scheme)
  end

  pass, url = ut.split_first(url, '@', true)
  if not url then url, pass = pass end

  host, url = ut.split_first(url, ':', true)
  if not url then host, db = ut.split_first(host, '/', true)
  else port, db = ut.split_first(url, '/', true) end

  return nil_if_empty(host), nil_if_empty(port), nil_if_empty(pass), nil_if_empty(db)
end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

local function is_callable(f)
  return (type(f) == 'function') and f
end

-------------------------------------------------------------------
-- create monitoring timer to be able to reconnect redis connection
-- close this timer before close connection object
local function AutoReconnect(cnn, interval, on_connect, on_disconnect)

  local timer = uv.timer():start(0, interval, function(self)
    self:stop()
    cnn:open()
  end):stop()

  local connected = true

  cnn:on('close', function(self, event, ...)
    local flag = connected

    connected = false

    if flag then on_disconnect(self, ...) end

    if timer:closed() or timer:closing() then
      return
    end

    timer:again()
  end)

  cnn:on('ready', function(self, event, ...)
    connected = true
    on_connect(self, ...)
  end)

  return timer
end
-------------------------------------------------------------------

-------------------------------------------------------------------
local Connection = ut.class() do

local function on_write_handler(cli, err, self)
  if err then
    if err ~= EOF then
      self._ee:emit('error', err)
    end
    self:_close(err)
  end
end

function Connection:__init(opt)
  if type(opt) == 'string' then
    opt = {server = opt}
  else opt = opt or {} end

  if opt.server then
    self._host, self._port, self._pass, self._db = decode_url(opt.server)
    self._host = self._host or '127.0.0.1'
    self._port = self._port or '6379'
    self._pass = self._pass or opt.pass
    self._db   = self._db   or opt.db
  else
    self._host = opt.host or '127.0.0.1'
    self._port = opt.port or '6379'
    self._db   = opt.db
    self._pass = opt.pass
  end

  self._stream           = RedisStream.new(self)
  self._commander        = RedisCommander.new(self._stream)
  self._open_q           = nil
  self._close_q          = nil
  self._delay_q          = ut.Queue.new()
  self._ready            = false
  self._ee               = EventEmitter.new{self=self}

  if opt.reconnect then
    local interval = 30
    if type(opt.reconnect) == 'number' then
      interval = opt.reconnect * 1000
    end
    self._reconnect_interval = interval
  end

  self._stream
  :on_command(function(s, data, cb)
    if self._ready then
      return self._cnn:write(data, on_write_handler, self)
    end
    if self._cnn then
      self._delay_q:push(data)
      return true
    end
    error('Can not execute command on closed client', 3)
  end)
  :on_halt(function(s, err)
    self:_close(err)
    if err ~= EOF then
      self._ee:emit('error', err)
    end
  end)
  :on_message(function(_, ...)
    self._ee:emit(...)
  end)

  return self
end

function Connection:clone()
  return Connection.new{
    host = self._host;
    port = self._port;
    pass = self._pass;
    db   = self._db;
  }
end

local function on_ready(self, ...)
  self._ready = true

  self._ee:emit('ready')

  while true do
    local data = self._delay_q:pop()
    if not data then break end
    self._cnn:write(data, on_write_handler, self)
  end

  while self._ready do
    local cb = self._open_q:pop()
    if not cb then break end
    cb(self, ...)
  end
end

local on_reconnect  = function(self, ...) self._ee:emit('reconnect',  ...) end

local on_disconnect = function(self, ...) self._ee:emit('disconnect', ...) end

function Connection:open(cb)
  if self._ready then
    uv.defer(cb, self)
    return self
  end

  -- Only first call 
  if self._cnn then
    if cb then
      self._open_q:push(cb)
    end
    return self
  end

  local cmd -- Init command

  local ok, err = uv.tcp():connect(self._host, self._port, function(cli, err)
    if err then
      self._ee:emit('error', err)
      return self:_close(err)
    end

    self._ee:emit('open')

    cli:start_read(function(cli, err, data)
      if err then return self._stream:halt(err) end
      self._stream:append(data):execute()
    end)

    if not cmd then return on_ready(self) end

    -- send out init command
    for _, data in ipairs(cmd) do
      cli:write(data, on_write_handler, self)
    end
  end)

  if not ok then return nil, err end

  self._cnn     = ok
  self._open_q  = ut.Queue.new()
  self._close_q = ut.Queue.new()

  if self._reconnect_interval and not self._reconnect then
    self._reconnect = AutoReconnect(self,
      self._reconnect_interval,
      on_reconnect,
      on_disconnect
    )
  end

  if cb then
    self._open_q:push(cb)
  end

  if self._db or self._pass then
    local called = false

    -- call until first error
    local wrap = function(last)
      return function(_, err, ...)
        if called then return end

        if err then
          called = true
          self._ee:emit('error', err)
          return self:_close(err)
        end

        if last then on_ready(self, err, ...) end
      end
    end

    local last = not not self._db
    if self._pass then self:auth  (tostring(self._pass), wrap(last)) end
    if self._db   then self:select(tostring(self._db  ), wrap(true)) end

    cmd = {}
    while true do
      local data = self._delay_q:pop()
      if not data then break end
      cmd[#cmd + 1] = data
    end
  end

  return self
end

function Connection:close(...)
  if self._reconnect then
    self._reconnect:close()
    self._reconnect = nil
  end
  return self:_close(...)
end

function Connection:_close(err, cb)
  if type(err) == 'function' then
    cb, err = err
  end

  if not self._cnn then
    if cb then uv.defer(cb, self) end
    return
  end

  if cb then self._close_q:push(cb) end

  if not (self._cnn:closed() or self._cnn:closing()) then
    self._cnn:close(function()
      -- we can not use same queues because callback can
      -- call `open` again and put new callback to `next` connection
      local open_q, close_q  = self._open_q, self._close_q

      self._delay_q:reset()
      self._cnn, self._open_q, self._close_q = nil

      local command_q = self._stream:defer_reset()

      call_q(open_q, self, err or EOF)
      for _, cb in ipairs(command_q) do
        cb(self, err or EOF)
      end
      call_q(close_q, self, err)

      self._ee:emit('close', err)
    end)
  end

  self._ready = false
end

function Connection:closed()
  return not self._cnn
end

function Connection:pipeline()
  return self._commander:pipeline()
end

function Connection:__tostring()
  return string.format("Lua UV Redis (%s)", tostring(self._cnn))
end

function Connection:on(...)
  return self._ee:on(...)
end

function Connection:off(...)
  return self._ee:off(...)
end

function Connection:onAny(...)
  return self._ee:onAny(...)
end

function Connection:offAny(...)
  return self._ee:offAny(...)
end

function Connection:removeAllListeners(...)
  return self._ee:removeAllListeners(...)
end

RedisCommander.commands(function(name)
  name = name:lower()

  assert(nil == Connection[name], 'ALERT! name already taken by connection API: ' .. name)

  Connection[name] = function(self, ...)
    if not self._cnn then
      local cb = select('#', ...) > 0 and is_callable(select(-1, ...))
      if cb then uv.defer(cb, self, ENOTCONN) end
      return
    end

    return self._commander[name](self._commander, ...)
  end
end)

end
-------------------------------------------------------------------

return {
  Connection = Connection;
}
