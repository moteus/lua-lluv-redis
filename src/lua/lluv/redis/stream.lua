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

local ut = require "lluv.utils"

local NULL = {}

local unpack = unpack or table.unpack

local RedisError = ut.class() do

function RedisError:__init(no, name, msg, ext)
  self._no   = no
  self._name = name
  self._msg  = msg or ''
  self._ext  = ext or ''
  return self
end

function RedisError:cat() return 'REDIS'     end

function RedisError:no()  return self._no    end

function RedisError:name() return self._name end

function RedisError:msg() return self._msg   end

function RedisError:ext() return self._ext   end

function RedisError:__tostring()
  local err = string.format("[%s][%s] %s (%d)",
    self:cat(), self:name(), self:msg(), self:no()
  )
  if self:ext() then
    err = string.format("%s - %s", err, self:ext())
  end
  return err
end

function RedisError:__eq(lhs)
  return getmetatable(lhs) == RedisError
    and self:no()   == lhs:no()
    and self:name() == lhs:name()
end

end

local meta = getmetatable
local function IsRedisError(obj)
  return meta(obj) == RedisError
end

local EPROTO = -1

local function RedisError_EPROTO(desc)
  return RedisError.new(EPROTO, "EPROTO", "Protocol error", desc)
end

local function RedisError_SERVER(name, msg, cmd)
  return RedisError.new(0, name, msg or '', cmd)
end

local RedisCmdStream = ut.class() do

local EOL  = "\r\n"
local OK   = '+'
local ERR  = '-'
local INT  = ':'
local BULK = '$'
local ARR  = '*'

local CB, STATE, DATA, CMD, DECODER = 1, 2, 3, 4, 5
local I, N = 3, 1

local SUBSCRIBE_COMMANDS = {
  subscribe    = true; SUBSCRIBE    = true;
  unsubscribe  = true; UNSUBSCRIBE  = true;
  psubscribe   = true; PSUBSCRIBE   = true;
  punsubscribe = true; PUNSUBSCRIBE = true;
}

local SUBSCRIBE_MESSAGES = {
  subscribe    = true; SUBSCRIBE    = true; -- [channel, total_channels]
  unsubscribe  = true; UNSUBSCRIBE  = true; -- [channel, total_channels]
  psubscribe   = true; PSUBSCRIBE   = true; -- [pattern, total_channels]
  punsubscribe = true; PUNSUBSCRIBE = true; -- [pattern, total_channels]
  message      = true; MESSAGE      = true; -- [channel, data]
  pmessage     = true; PMESSAGE     = true; -- [pattern, channel, data]
}

local function pass(err, data) return err, data end

local function iclone(t, n)
  local r = {}
  for i = 1, n or #t do r[i] = t[i] end
  return r
end

local function decode_line(line)
  local p, d = line:sub(1, 1), line:sub(2)
  if p == OK   then return p, d end
  if p == ERR  then return p, ut.split_first(d, " ", true) end
  if p == INT  then return p, tonumber(d) end
  if p == BULK then return p, tonumber(d) end
  if p == ARR  then return p, tonumber(d) end
  return p, d
end

local function array_context(n)
  return {
    [0] = {n, 'line', 1}, n = n
  }
end

local function append_str(r, s)
  -- s = tostring(s)
  r[#r + 1] = BULK .. #s .. EOL
  r[#r + 1] = s
  r[#r + 1] = EOL
  r[1] = r[1] + 1
end

local function encode(res, t)
  if type(t) == "table" then
    if t[1] then -- array
      for i = 1, #t do
        encode(res, t[i])
      end
    else -- hash
      for k, v in pairs(t) do
        append_str(res, k)
        append_str(res, v)
      end
    end
  else
    append_str(res, t)
  end
  return res
end

local function encode_cmd(t)
  local res = {0}
  encode(res, t)
  res[1] = "*" .. res[1] .. EOL
  return table.concat(res)
end

local function is_callable(f) return (type(f) == 'function') and f end

local function flat(t)
  if type(t) == 'string' then return t end

  local r = {}
  for i = 1, #t do
    local v = t[i]
    if type(v) == 'string' then
      r[#r + 1] = v
    else
      for j = 1, #v do
        r[#r + 1] = v[j]
      end
    end
  end
  return r
end

function RedisCmdStream:__init(cb_self)
  self._buffer = ut.Buffer.new(EOL)
  self._queue  = ut.Queue.new()
  self._txn    = ut.Queue.new()
  self._self   = cb_self or self
  self._sub    = nil -- fake task to proceed `message` replays

  self._on_halt    = nil
  self._on_message = nil
  self._on_command = nil
  return self
end

function RedisCmdStream:_decode_array(task, t)
  local ctx = t[0]

  while ctx[I] <= ctx[N] do
    local i = ctx[I]
    if ctx[STATE] == 'line' then
      local line = self._buffer:read_line()
      if not line then return end
      local typ, n, ext = decode_line(line)
  
      if not typ then return end

      if typ == ARR then
        if n == -1 then
          t[i], ctx[I] = nil, i + 1
        else
          ctx[STATE], t[i] = 'array', array_context(n, t)
        end
      elseif typ == OK or typ == INT then
        t[i], ctx[I] = n, i + 1
      elseif typ == ERR then
        local err = RedisError_SERVER(n, ext, task[CMD])
        t[i], ctx[I] = err, i + 1
      elseif typ == BULK then
        if n == -1 then
          t[i], ctx[I] = nil, i + 1
        else
          t[i], ctx[STATE] = n, 'array_string'
        end
      else
        --! @todo raise EPROTO error
        error("Unsupported Type:" .. typ)
      end
    end

    if ctx[STATE] == 'array_string' then
      local data = self._buffer:read_n(t[i])
      if not data then return end
      t[i], ctx[STATE] = data, 'array_string_eol'
    end

    if ctx[STATE] == 'array_string_eol' then
      local data = self._buffer:read_n(#EOL)
      if not data then return end
      assert(data == EOL)
      ctx[STATE], ctx[I] = 'line', i + 1
    end

    if ctx[STATE] == 'array' then
      if not self:_decode_array(task, t[i]) then
        return
      end
      ctx[STATE], ctx[I] = 'line', i + 1
    end
  end

  if ctx[I] > ctx[N] then
    t[0] = nil
    return true
  end
end

function RedisCmdStream:_next_data_task()
  local queue = self._queue

  while true do
    local task = queue:peek() or self._sub

    if not task then
      if not self._buffer:empty() then
        return nil, RedisError_EPROTO(self._buffer:read_all())
      end
      return
    end

    if task[STATE] then return task end

    local line = self._buffer:read_line()
    if not line then return end

    local cb, decoder = task[CB], task[DECODER]
    local typ, data, add = decode_line(line)
    if typ == OK then
      queue:pop()
      if data == 'QUEUED' then
        self._txn:push(task)
      else
        if task[CMD] == 'DISCARD' then
          while true do
            local task = self._txn:pop()
            if not task then break end
            task[CB](self._self, task[DECODER]('DISCARD'))
          end
        end
        cb(self._self, decoder(nil, data))
      end
    elseif typ == ERR then
      queue:pop()
      local err = RedisError_SERVER(data, add, task[CMD])
      cb(self._self, decoder(err))
    elseif typ == INT then
      queue:pop()
      cb(self._self, decoder(nil, data))
    elseif typ == BULK then
      if data == -1 then
        queue:pop()
        cb(self._self, decoder(nil, nil))
      else
        task[STATE], task[DATA] = 'BULK', data
      end
    elseif typ == ARR then
      if data == -1 then
        queue:pop()

        if task[CMD] == 'EXEC' then
          while true do
            local task = self._txn:pop()
            if not task then break end
            task[CB](self._self, decoder())
          end
        end

        cb(self._self, decoder())
      else
        task[STATE], task[DATA] = 'ARR', array_context(data)
        return task
      end
    else
      return nil, RedisError_EPROTO(line)
    end
  end
end

function RedisCmdStream:_handle_async_message(payload)
  local msg = payload[1]

  if not msg then
    self:halt(RedisError_EPROTO('unexpected message in pubsub mode: <UNKNOWN>'))
    return
  end

  if SUBSCRIBE_COMMANDS[msg] then
    local channels = payload[3]
    if channels == 0 then self._sub = nil end
  end

  if SUBSCRIBE_MESSAGES[msg] then
    if self._on_message then self._on_message(self._self, unpack(payload)) end
  else
    self:halt(RedisError_EPROTO('unexpected message in pubsub mode: ' .. msg))
  end
end

function RedisCmdStream:execute()
  while true do
    local task, err = self:_next_data_task()
    if not task then 
      if err then self:halt(err) end
      return
    end

    local cb, decoder = task[CB], task[DECODER]
    if task[STATE] == 'BULK' then
      local data = self._buffer:read_n(task[DATA])
      if not data then return end
      task[STATE], task[DATA] = 'BULK_EOL', data
    elseif task[STATE] == 'ARR' then
      if not self:_decode_array(task, task[DATA]) then
        return
      end

      if SUBSCRIBE_COMMANDS[ task[CMD] ]
        or (self._sub and task[DATA][1] and SUBSCRIBE_MESSAGES[ task[DATA][1] ])
      then
        -- There posssible several states
        -- - 1 We in RR mode and send [p]subscribe with single channels
        -- - 2 We in RR mode and send [p]subscribe with multiple channels
        -- - 3 We in RR mode and send [p]unsubscribe with single channels
        -- - 4 We in RR mode and send [p]unsubscribe with multiple channels
        -- - 5 We in PS mode and send [p]subscribe with single channels
        -- - 6 We in PS mode and send [p]subscribe with multiple channels
        -- - 7 We in PS mode and send [p]unsubscribe with single channels
        -- - 7.1 We unsubscribe from not last channel and we still in PS mode
        -- - 7.2 We unsubscribe from last channel and switch to RR mode
        -- - 8 We in PS mode and send [p]unsubscribe with multiple channels
        -- - 8.1 We unsubscribe from not last channels and we still in PS mode
        -- - 8.2 We unsubscribe from not last channel using not last one in list
        -- -     e.g. we have subscriptions to `a`, `b` and `c` and send command
        -- -     `unsubscribe a b c d e`.
        -- - 8.3 We unsubscribe from not last channel using last one in list
        -- -     e.g. we have subscriptions to `a`, `b` and `c` and send command
        -- -     `unsubscribe a b d e c`.
        -- 
        -- * PP - PubSub RR - RequesResponse
        -- 
        -- Main problem with unsubscribe command. It sends async messages
        -- even if server not in pubsub mode. E.g. if we call `unsubscribe a b`
        -- and then `get a`. Server send response
        --   {'unsubscribe' 'a', 0}
        --   {'unsubscribe' 'b', 0}
        --   'some_a_value'
        -- And I do not see any easy way how to distinct asyn message from real
        -- response. E.g. `redis-cli` just returns `{'unsubscribe' 'b', 0}` as
        -- result for `get a` command. And `some_a_value` as result for any next
        -- command. And so on. 
        -- So I strongly recomended do not use `unsubscribe` with multiple channels
        -- or do not use same connection for pubsub and for regular data transmition
        -- 
        -- Because of that all 8.x can lead to unexpected/not correct results

        local payload = task[DATA]

        if (task ~= self._sub) or (self._sub == nil) then
          -- we handle real command response.
          -- we handle first message as result. This is correct only if use subscribe/unsubscribe
          -- with only one channel. but if you send `subscribe a b` and then `subscribe c`
          -- then you get `[subscribe b 2]` as result for your second subscribe.

          local msg = payload[1]

          if SUBSCRIBE_COMMANDS[ msg ] and not self._sub then
            local channels = payload[3]
            if channels > 0 then
              self._sub = {nil, nil, nil, task[CMD], decoder or pass}
            end
          end

          if msg == string.lower(task[CMD]) then
            self._queue:pop()
            cb(self._self, decoder(nil, task[DATA]))
          else
            -- This is not response to command. so we have to `restart` task again
            task[DATA], task[STATE] = nil
          end
        else 
          -- task is fake task so we have reset it by hand
          task[DATA], task[STATE] = nil
        end

        --! @fixme do not call anythig if `cb` calls `halt`
        self:_handle_async_message(payload)
      else
        self._queue:pop()
        if task[CMD] == 'EXEC' then
          local t, i, e = task[DATA], 1, {}
          while true do
            local task = self._txn:pop()
            if not task then break end

            local err, data
            if IsRedisError(t[i]) then
              err, data = task[DECODER](t[i])
            else
              err, data = task[DECODER](nil, t[i])
            end

            task[CB](self._self, err, data)

            e[i], t[i] = decoder(err, data)

            i = i + 1
          end
          cb(self._self, nil, t, e)
        else
          cb(self._self, decoder(nil, task[DATA]))
        end
      end
    end

    if task[STATE] == 'BULK_EOL' then
      local eol = self._buffer:read_n(#EOL)
      if not eol then return end
      assert(eol == EOL, eol)
      assert(task == self._queue:pop())
      cb(self._self, decoder(nil, task[DATA]))
    end

  end
end

function RedisCmdStream:append(data)
  self._buffer:append(data)
  return self
end

function RedisCmdStream:encode_command(cmd)
  if type(cmd) == 'string' then return cmd .. EOL, cmd end
  return encode_cmd(cmd), cmd[1]
end

function RedisCmdStream:command(cmd, cb, decoder)
  assert(is_callable(cb))
  local cmd, cmd_name = self:encode_command(cmd)
  if self._on_command(self._self, cmd) then
    self._queue:push{cb, nil, nil, cmd_name, decoder or pass}
  end
  return self._self
end

function RedisCmdStream:pipeline_command(cmd, cb, decoder)
  assert(is_callable(cb))
  local cmd, cmd_name = self:encode_command(cmd)
  local task = {cb, nil, nil, cmd_name, decoder or pass}
  return cmd, task
end

function RedisCmdStream:pipeline(cmd, tasks, multiple)
  if self:_on_command(flat(cmd)) then
    if multiple then
      for i = 1, #tasks do
        self._queue:push(iclone(tasks[i], 5))
      end
    else
      for i = 1, #tasks do
        self._queue:push(tasks[i])
      end
    end
  end
  return self
end

function RedisCmdStream:halt(err)
  self:reset(err)
  if self._on_halt then self._on_halt(self._self, err) end
  return
end

function RedisCmdStream:reset(err)
  while true do
    local task = self._queue:pop()
    if not task then break end
    task[CB](self._self, err)
  end

  while true do
    local task = self._txn:pop()
    if not task then break end
    task[CB](self._self, err)
  end

  self._buffer:reset()
  return
end

function RedisCmdStream:on_command(handler)
  self._on_command = handler
  return self
end

function RedisCmdStream:on_message(handler)
  self._on_message = handler
  return self
end

function RedisCmdStream:on_halt(handler)
  self._on_halt = handler
  return self
end

end

return {
  new          = RedisCmdStream.new;
  NULL         = NULL;
  error        = RedisError.new;
  IsError      = IsRedisError;
}
