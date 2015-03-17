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

local REQUEST = {
  arg = function(cmd, arg, cb)
    return {cmd, arg}, cb or dummy
  end;
  
  arg_or_array = function(cmd, args, cb)
    local res
    if type(args) == "table" then
      res = {cmd}
      for i = 1, #args do res[i+1] = args[i] end
      return res
    else res = {cmd, args} end
    return res, cb or dummy
  end;

  arg_or_hash = function(cmd, args, cb)
    local res
    if type(args) == "table" then
      res = {cmd}
      for k, v in pairs(args) do
        res[#res + 1] = k
        res[#res + 1] = v
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

    return args, cb
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

local RESPONSE = {
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

function RedisCommander:__init(stream)
  self._stream = stream or Stream.new()

  return self
end

function RedisCommander:add_command(name, request, response)
  response = response or RESPONSE.pass

  local decoder = function(err, data)
    if err then return err, data end --! @todo Build error object
    return response(err, data)
  end

  self[name:lower()] = function(self, ...)
    local cmd, cb = request(name, ...)
    self._stream:command(cmd, cb, decoder)
  end

  return self
end

RedisCommander
  :add_command("PING",   REQUEST.none,        RESPONSE.pass        )
  :add_command("ECHO",   REQUEST.arg,         RESPONSE.pass        )
  :add_command("EXISTS", REQUEST.arg,         RESPONSE.number_bool )
  :add_command("SET",    REQUEST.multi_args,  RESPONSE.string_bool )
  :add_command("GET",    REQUEST.arg,         RESPONSE.pass        )

end

return {
  new = RedisCommander.new
}
