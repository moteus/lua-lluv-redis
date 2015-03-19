-- Syncronus client based on LuaSocket
-- This is just POF
--
-- Known issues
--  * require yieldable pcall
--  * does not correctly work with multi/exec
--  * does not correctly work with pub/sub

local socket         = require "socket"
local va             = require "vararg"
local ut             = require "lluv.utils"
local RedisStream    = require "lluv.redis.stream"
local RedisCommander = require "lluv.redis.commander"

local SyncClient = ut.class() do

function SyncClient:__init(host, port)
  self._cli = socket.connect("127.0.0.1", 6379)
  self._stm = RedisStream.new(self._cli)
  self._cmd = RedisCommander.new(self._stm)

  self._co = coroutine.create(function()
    local interupt = false

    self._stm:on_command(function(cli, cmd)
      local ok, err = cli:send(cmd)
      if not ok then return self._stm:halt(err) end
      return true
    end)

    self._stm:on_halt(function(err) interupt = true end)

    local step, exec

    local function check_exec(ok, ...)
      if not ok then return exec(step, coroutine.yield(...)) end
      return ...
    end

    exec = function (cb, cmd, ...)
      local fn = self._cmd[cmd]
      return check_exec(pcall(fn, self._cmd, va.append(cb, ...)))
    end

    step = function(cli, ...)
      return exec(step, coroutine.yield(...))
    end

    step()

    while not interupt do
      local line, err = self._cli:receive("*l")
      if not line then self._stm:halt(err)
      else self._stm:append(line .. "\r\n"):execute() end
    end

  end)
  assert(coroutine.resume(self._co))

  return self
end

function SyncClient:_check_execute(ok, err, ...)
  if not ok then -- coroutine error
    self:close()
    return nil, ...
  end

  if err then -- redis error
    return nil, err
  end

  return ...
end

function SyncClient:_execute(...)
  return self:_check_execute(coroutine.resume(self._co, ...))
end

function SyncClient:close()
  self._cli:close()
end

RedisCommander.commands(function(name)
  name = name:lower()
  SyncClient[name] = function(self, ...)
    return self:_execute(name, ...)
  end
end)

end

local function Connect(host, port)
  return SyncClient.new(host, port)
end

local cli = Connect("127.0.0.1", 6379)

print(cli:ping())
print(cli:echo("hello world"))
print(cli:quit())
