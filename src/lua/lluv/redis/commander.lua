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

local ut     = require "lluv.utils"
local Stream = require "lluv.redis.stream"

--- Notes
-- Request is array of strings.
-- You can not pass number (:123), NULL(*-1), basic string (+HELLO) or any other type.

local function dummy()end

local function is_callable(f) return (type(f) == 'function') and f end

--- Arguments variants
-- argument            -- ECHO "hello"
-- hash                -- MSET {key1=value,key2=value}
-- array               -- MGET {key1, key2}
-- multiple arguments  -- SET  key value timeout

local REQUEST REQUEST = {
  arg = function(cmd, arg1, cb)
    assert(arg1 ~= nil and not is_callable(arg1))
    assert(cb == nil or is_callable(cb))
    return {cmd, tostring(arg1)}, cb or dummy
  end;

  arg_or_array = function(cmd, args, cb)
    assert(args ~= nil and not is_callable(args))
    assert(cb == nil or is_callable(cb))

    local res
    if type(args) == "table" then
      res = {cmd}
      for i = 1, #args do res[i+1] = tostring(args[i]) end
    else res = {cmd, tostring(args)} end
    return res, cb or dummy
  end;

  arg_or_hash = function(cmd, args, cb)
    assert(args ~= nil and not is_callable(args))
    assert(cb == nil or is_callable(cb))

    local res
    if type(args) == "table" then
      res = {cmd}
      for k, v in pairs(args) do
        res[#res + 1] = tostring(k)
        res[#res + 1] = tostring(v)
      end
    else res = {cmd, args} end
    return res, cb or dummy
  end;

  multi_args  = function(...)
    local n    = select("#", ...)
    local args = {...}
    local cb   = args[n]
    if is_callable(cb) then
      args[n] = nil
    else
      cb = dummy
    end

    for i = 2, #args do args[i] = tostring(args[i]) end

    return args, cb
  end;

  array_or_multi = function(...)
    local a = select(2, ...)
    if type(a) == "table" then
      return REQUEST.arg_or_array(...)
    end
    return REQUEST.multi_args(...)
  end;

  none = function(cmd, cb) return cmd, cb or dummy end;
}

--- Result variants
-- Boolean
--     - `OK` string MULTI
--     - `1`  number EXISTS 1
-- Number INCR a
-- Array  MGET key1 key2
-- Hash   HGETALL hash
-- NULL   GET nonexists
-- Custom INFO

local RESPONSE RESPONSE = {
  string_bool = function(err, resp)
    return err, resp == 'OK'
  end;

  number_bool = function(err, resp)
    return err, resp == 1
  end;

  hash        = function(err, resp)
    local res = {}
    for i = 1, #resp, 2 do
      res[ resp[i] ] = resp[i + 1]
    end
    return err, res
  end;

  pass        = function(err, resp)
    return err, resp
  end;
}

local RedisCommander = ut.class() do

RedisCommander._commands = {}

function RedisCommander:__init(stream)
  self._stream   = stream or Stream.new()
  self._commands = {}

  return self
end

function RedisCommander:add_command(name, request, response)
  response = response or RESPONSE.pass

  name = name:upper()

  local decoder = function(err, data)
    if err then return err, data end --! @todo Build error object
    return response(err, data)
  end

  self[name:lower()] = function(self, ...)
    local cmd, cb = request(name, ...)
    self._stream:command(cmd, cb, decoder)
  end

  self._commands[name] = true

  return self
end

function RedisCommander:each_command(fn)
  for cmd in pairs(RedisCommander._commands) do fn(cmd, true) end
  if self ~= RedisCommander then
    for cmd in pairs(self._commands) do fn(cmd, false) end
  end
  return self
end

RedisCommander
  :add_command("QUIT",                 REQUEST.none,                   RESPONSE.string_bool        )
  :add_command("PING",                 REQUEST.none,                   RESPONSE.pass               )
  :add_command("ECHO",                 REQUEST.arg,                    RESPONSE.pass               )
  :add_command("EXISTS",               REQUEST.arg,                    RESPONSE.number_bool        )
  :add_command("SET",                  REQUEST.multi_args,             RESPONSE.string_bool        )
  :add_command("GET",                  REQUEST.arg,                    RESPONSE.pass               )
  :add_command("DEL",                  REQUEST.array_or_multi,         RESPONSE.pass               )
  :add_command("MULTI",                REQUEST.none,                   RESPONSE.string_bool        )
  :add_command("EXEC",                 REQUEST.none,                   RESPONSE.pass               )
  :add_command("DISCARD",              REQUEST.none,                   RESPONSE.string_bool        )

  :add_command("PUBLISH",              REQUEST.multi_args,             RESPONSE.pass               )
  :add_command("SUBSCRIBE",            REQUEST.array_or_multi,         RESPONSE.pass               )
  :add_command("PSUBSCRIBE",           REQUEST.array_or_multi,         RESPONSE.pass               )
  :add_command("UNSUBSCRIBE",          REQUEST.array_or_multi,         RESPONSE.pass               )
  :add_command("PUNSUBSCRIBE",         REQUEST.array_or_multi,         RESPONSE.pass               )
end

return {
  new      = RedisCommander.new;
  commands = function(...) RedisCommander:each_command(...) end;
}
