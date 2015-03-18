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
-- key array           -- HMGET key {key1, key2}
-- key multiple        -- HMGET key key1, key2

local pack_args = function(...)
  local n    = select("#", ...)
  local args = {...}
  local cb   = args[n]
  if is_callable(cb) then args[n] = nil
  else cb = dummy end
  return args, cb
end

local function n_args(min, max)
  if min == max then
    if min == 0 then
      return function(cmd, cb)
        assert(cb == nil or is_callable(cb))
        return cmd, cb
      end
    end
  end

  max = max and max + 1 or math.huge

  return function(...)
    local args, cb = pack_args(...)
    assert((#args > min) and (#args <= max), "invalid number of arguments")
    for i = 2, #args do args[i] = tostring(args[i]) end
    return args, cb
  end
end

local function n_args_then_array(n)
  n = n + 1 -- command itself
  n = n + 1 -- arg
  return function(...)
    local args, cb = pack_args(...)
    assert(#args >= n, "not enouth arguments")
    if #args == n then -- array or single key
      if type(args[n]) == 'table' then
        local t = args[n]
        for i = 1, #t do args[n + i - 1] = t[i] end
      end
    end
    for i = 2, #args do args[i] = tostring(args[i]) end
    return args, cb
  end
end

local function n_args_then_hash(n)
  n = n + 1 -- command itself
  n = n + 1 -- arg
  return function(...)
    local args, cb = pack_args(...)
    assert(#args >= n, "not enouth arguments")

    if #args == n then -- hash
      assert(type(args[n]) == 'table')
      local t, i = args[n], n
      for k, v in pairs(t) do
        args[i]   = k
        args[i+1] = v
        i = i + 2
      end
    else
      assert(math.mod(#args - n, 2) ~= 0, "invalid number of arguments")
    end
    for i = 2, #args do args[i] = tostring(args[i]) end
    return args, cb
  end
end

local function args_test()

local function n_args_then_array_test()
  local function CMD(a, b) return table.concat(a, ' '), b end
  local s = "BITOP AND destkey srckey1 srckey2 srckey3"

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", {"srckey1", "srckey2", "srckey3"}))
  assert(e == s, e) assert(f == dummy)

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", "srckey1", "srckey2", "srckey3" ))
  assert(e == s) assert(f == dummy)

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", {"srckey1", "srckey2", "srckey3"}, print))
  assert(e == s, e) assert(f == print)

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", "srckey1", "srckey2", "srckey3", print ))
  assert(e == s) assert(f == print)

  local s = "BITOP AND destkey srckey1"

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", "srckey1"))
  assert(e == s) assert(f == dummy)

  local e, f = CMD(n_args_then_array(2)( "BITOP", "AND", "destkey", "srckey1", print ))
  assert(e == s) assert(f == print)

  assert(not pcall(function() n_args_then_array(2)( "BITOP", "AND", "destkey") end))

  assert(not pcall(function() n_args_then_array(2)( "BITOP", "AND", "destkey", print) end))

  local s = "DEL key1 key2 key3"

  local e, f = CMD(n_args_then_array(0)( "DEL", {"key1", "key2", "key3"}))
  assert(e == s, e) assert(f == dummy)

  local e, f = CMD(n_args_then_array(0)( "DEL", "key1", "key2", "key3"))
  assert(e == s, e) assert(f == dummy)

  assert(not pcall(function() n_args_then_array(0)( "DEL" ) end))

  assert(not pcall(function() n_args_then_array(0)( "DEL", print) end))
end

local function n_args_then_hash_test()
  local function CMD(a, b) return table.concat(a, ' '), b end
  local s1 = "HMSET myhash field2 World field1 Hello"
  local s2 = "HMSET myhash field1 Hello field2 World"

  local e, f = CMD(n_args_then_hash(1)("HMSET", "myhash", "field1", "Hello", "field2", "World"))
  assert(e == s1 or e == s2, e) assert(f == dummy)

  local e, f = CMD(n_args_then_hash(1)("HMSET", "myhash", {field1 = "Hello", field2 = "World"}))
  assert(e == s1 or e == s2, e) assert(f == dummy)

  local e, f = CMD(n_args_then_hash(1)("HMSET", "myhash", "field1", "Hello", "field2", "World", print))
  assert(e == s1 or e == s2, e) assert(f == print)

  local e, f = CMD(n_args_then_hash(1)("HMSET", "myhash", {field1 = "Hello", field2 = "World"}, print))
  assert(e == s1 or e == s2, e) assert(f == print)

  assert(not pcall(function() n_args_then_hash(1)("HMSET", "myhash", "field1", "Hello", "field2", print) end))
  assert(not pcall(function() n_args_then_hash(1)("HMSET", "myhash", print) end))

  ----------------------------------------------------

  local s1 = "MSET field2 World field1 Hello"
  local s2 = "MSET field1 Hello field2 World"

  local e, f = CMD(n_args_then_hash(0)("MSET", "field1", "Hello", "field2", "World"))
  assert(e == s1 or e == s2, e) assert(f == dummy)

  local e, f = CMD(n_args_then_hash(0)("MSET", {field1 = "Hello", field2 = "World"}))
  assert(e == s1 or e == s2, e) assert(f == dummy)

  local e, f = CMD(n_args_then_hash(0)("MSET", "field1", "Hello", "field2", "World", print))
  assert(e == s1 or e == s2, e) assert(f == print)

  local e, f = CMD(n_args_then_hash(0)("MSET", {field1 = "Hello", field2 = "World"}, print))
  assert(e == s1 or e == s2, e) assert(f == print)

  assert(not pcall(function() n_args_then_hash(0)("MSET", "field1", "Hello", "field2", print) end))
  assert(not pcall(function() n_args_then_hash(0)("MSET", print) end))
end

local function n_args_test()
  local function CMD(a, b) return table.concat(a, ' '), b end
  local s = "A B C"

  local e, f = CMD(n_args(0, 2)("A", "B", "C"))
  assert(e == s, e) assert(f == dummy)

  local e, f = CMD(n_args(2, 2)("A", "B", "C"))
  assert(e == s, e) assert(f == dummy)

  local e, f = CMD(n_args(2)("A", "B", "C"))
  assert(e == s, e) assert(f == dummy)

  assert(not pcall(function() n_args(0, 1)("A", "B", "C") end))
  assert(not pcall(function() n_args(1, 1)("A", "B", "C") end))
  assert(not pcall(function() n_args(3)   ("A", "B", "C") end))
end

n_args_then_array_test()
n_args_then_hash_test()
n_args_test()

end

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

local RedisPipeline

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
    return self._stream:command(cmd, cb, decoder)
  end

  self["_pipeline_" .. name:lower()] = function(self, ...)
    local cmd, cb = request(name, ...)
    return self._stream:pipeline_command(cmd, cb, decoder)
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

function RedisCommander:_pipeline(...)
  return self._stream:pipeline(...)
end

function RedisCommander:pipeline()
  return RedisPipeline.new(self)
end

RedisCommander
  :add_command("QUIT",                 n_args(0, 0),               RESPONSE.string_bool        )
  :add_command("PING",                 n_args(0, 0),               RESPONSE.pass               )
  :add_command("ECHO",                 n_args(1, 1),               RESPONSE.pass               )
  :add_command("EXISTS",               n_args(1, 1),               RESPONSE.number_bool        )
  :add_command("SET",                  n_args(2, 7),               RESPONSE.string_bool        )
  :add_command("GET",                  n_args(1, 1),               RESPONSE.pass               )
  :add_command("DEL",                  n_args_then_array(0),       RESPONSE.pass               )
  :add_command("MULTI",                n_args(0, 0),               RESPONSE.string_bool        )
  :add_command("EXEC",                 n_args(0, 0),               RESPONSE.pass               )
  :add_command("DISCARD",              n_args(0, 0),               RESPONSE.string_bool        )

  :add_command("PUBLISH",              n_args(2, 2),               RESPONSE.pass               )
  :add_command("SUBSCRIBE",            n_args_then_array(0),       RESPONSE.pass               )
  :add_command("PSUBSCRIBE",           n_args_then_array(0),       RESPONSE.pass               )
  :add_command("UNSUBSCRIBE",          n_args_then_array(0),       RESPONSE.pass               )
  :add_command("PUNSUBSCRIBE",         n_args_then_array(0),       RESPONSE.pass               )
end

RedisPipeline = ut.class() do

function RedisPipeline:__init(commander)
  self._commander = assert(commander)
  self._cmd, self._arg = {},{}

  return self
end

function RedisPipeline:add_command(name)
  local n = '_pipeline_' .. name:lower()
  self[name:lower()] = function(self, ...)
    local cmd, args = self._commander[n](self._commander, ...)
    if cmd then
      self._cmd[#self._cmd + 1] = cmd
      self._arg[#self._arg + 1] = args
    end
    return self
  end
end

RedisCommander:each_command(function(name)
  RedisPipeline:add_command(name)
end)

function RedisPipeline:execute(preserve)
  self._commander:_pipeline(self._cmd, self._arg, preserve)

  if not preserve then
    self._cmd, self._arg = {}, {}
  end

  return self
end

end

return {
  new      = RedisCommander.new;
  commands = function(...) RedisCommander:each_command(...) end;

  self_test = function()
    args_test()
  end;
}
