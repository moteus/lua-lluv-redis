package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local RedisStream = require "lluv.redis.stream"
local utils       = require "utils"
local TEST_CASE   = require "lunit".TEST_CASE

local pcall, error, type, table = pcall, error, type, table
local RUN = utils.RUN
local IT, CMD, PASS = utils.IT, utils.CMD, utils.PASS
local nreturn, is_equal = utils.nreturn, utils.is_equal

local ENABLE = true

local _ENV = TEST_CASE'redis stream encoder/decoder' if ENABLE then

local it = IT(_ENV or _M)

local stream

function setup()
  stream = assert(RedisStream.new())
end

it("provide public API", function()
  assert_not_nil(RedisStream.NULL)
  assert_function(stream.execute)
  assert_function(stream.append)
  assert_function(stream.command)
  assert_function(stream.pipeline)
end)

it("should fail without on_command callback", function()
  assert_error(function() stream:command("PING", function() end) end)
end)

it("on_command callback signature", function()
  local f = false
  stream:on_command(function(...)
    f = true
    local n, a, b, c = nreturn(...)
    assert_equal(3, n)
    assert_equal(stream, a)
    if type(b) ~= "string" then assert_table(b) end
    assert_equal(PASS, c)
  end)

  stream:command("PING", PASS)
  assert_true(f)
end)

it("response callback self arg", function()
  local SELF = {}
  stream = RedisStream.new(SELF)
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_equal(SELF, self)
    assert_nil(err)
    res = data
  end)
  stream:append("$5\r\nhello\r\n"):execute()

  assert_string(res)
end)

it("should fail without command callback", function()
  stream:on_command(PASS)
  assert_error(function() stream:command("PING") end)
end)

it("should encode basic command as string", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = assert(cmd)
  end)

  stream:command("PING", PASS)
  assert_equal("PING\r\n", msg)
end)

it("should pass any command", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = assert(cmd)
  end)

  stream:command("***", PASS)
  assert_equal("***\r\n", msg)
end)

it("should encode command with string args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  stream:command({"ECHO", "Hello world"}, PASS)

  assert_equal("*2\r\n$4\r\nECHO\r\n$11\r\nHello world\r\n", msg)
end)

it("should encode command with nil args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  stream:command({"ECHO", RedisStream.NULL}, PASS)

  assert_equal("*2\r\n$4\r\nECHO\r\n*-1\r\n", msg)
end)

it("should encode command with array args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = table.concat{
    "*2\r\n",
      "$4\r\n" .. "ECHO\r\n",
      "*2\r\n",
        "*3\r\n",
          ":1\r\n",
          ":2\r\n",
          ":3\r\n",
        "*3\r\n",
          "$3\r\n" .. "Foo\r\n",
          "$3\r\n" .. "Bar\r\n",
          "$6\r\n" .. "foobar\r\n",
  }
  local arg = {{1,2,3},{'Foo','Bar','foobar'}}

  stream:command({"ECHO", arg} , PASS)

  assert_equal(res, msg)
end)

it("should encode command with array with holes", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = table.concat{
    "*2\r\n",
      "$4\r\n" .. "ECHO\r\n",
      "*3\r\n",
        ":1\r\n",
        "*-1\r\n",
        ":3\r\n",
  }
  local arg = {1, nil, 3, n = 3}

  stream:command({"ECHO", arg} , PASS)

  assert_equal(res, msg)
end)

it("should encode command with empty array", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = table.concat{
    "*2\r\n",
      "$4\r\n" .. "ECHO\r\n",
      "*0\r\n",
  }
  local arg = {}

  stream:command({"ECHO", arg} , PASS)

  assert_equal(res, msg)
end)

it("should decode pass", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("+PONG\r\n"):execute()

  assert_equal("PONG", res)
end)

it("should decode error", function()
  stream:on_command(PASS)

  local err, res
  stream:command("PING", function(self, ...)
    err, res = ...
  end)

  stream:append("-ERR unknown command 'PING2'\r\n"):execute()

  assert_equal("ERR", err)
  assert_equal("unknown command 'PING2'", res)
end)

it("should decode number", function()
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append(":12345\r\n"):execute()

  assert_equal(12345, res)
end)

it("should decode bulk", function()
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("$5\r\nhello\r\n"):execute()

  assert_equal("hello", res)
end)

it("should decode array", function()
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("*2\r\n")
  stream:append("*3\r\n")
  stream:append(":1\r\n")
  stream:append(":2\r\n")
  stream:append(":3\r\n")
  stream:append("*3\r\n")
  stream:append("+Foo\r\n")
  stream:append("-Bar\r\n")
  stream:append("$6\r\n")
  stream:append("foobar\r\n")

  stream:execute()

  assert(is_equal({{1,2,3},{'Foo',{error = 'Bar'},'foobar'}}, res))
end)

it("should decode array by chunks", function()
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  local str = table.concat{
    "*2\r\n",
      "*3\r\n",
        ":1\r\n",
        ":2\r\n",
        ":3\r\n",
      "*3\r\n",
        "+Foo\r\n",
        "-Bar\r\n",
        "$6\r\n",
          "foobar\r\n",
  }

  for i = 1, #str - 1 do
    local ch = str:sub(i, i)
    stream:append(ch):execute()
    assert_nil(res)
  end
  stream:append(str:sub(-1)):execute()

  assert(is_equal({{1,2,3},{'Foo',{error = 'Bar'},'foobar'}}, res))
end)

it("should decode bulk by chunks", function()
  stream:on_command(PASS)

  local res
  stream:command("***", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  local str = "$6\r\n" .. "foobar\r\n"

  for i = 1, #str - 1 do
    local ch = str:sub(i, i)
    stream:append(ch):execute()
    assert_nil(res)
  end
  stream:append(str:sub(-1)):execute()

  assert_equal('foobar', res)
end)

it("pipeline should encode commands with args", function()
  local msg
  stream:on_command(function(self, cmd)
    msg = CMD(cmd)
    return true
  end)

  local cmd1, task1 = stream:pipeline_command({"ECHO", "HELLO"}, PASS)
  local cmd2, task2 = stream:pipeline_command({"ECHO", "WORLD"}, PASS)

  stream:pipeline({cmd1, cmd2}, {task1, task2})

  local res = table.concat{
    "*2\r\n",
      "$4\r\n", "ECHO\r\n",
      "$5\r\n", "HELLO\r\n",
    "*2\r\n",
      "$4\r\n", "ECHO\r\n",
      "$5\r\n", "WORLD\r\n",
  }

  assert_equal(res, msg)
end)

it("pipeline should calls each callback", function()
  local msg
  stream:on_command(function(self, cmd)
    msg = CMD(cmd)
    return true
  end)

  local i = 0
  local cmd1, task1 = stream:pipeline_command("PING",
    function(_, err, data) assert_equal(0, i) i = i + 1 end
  )

  assert_equal("PING\r\n", CMD(cmd1))

  local cmd2, task2 = stream:pipeline_command("PING",
    function(_, err, data) assert_equal(1, i) i = i + 1 end
  )

  assert_equal("PING\r\n", CMD(cmd2))

  assert_nil(msg)

  stream:pipeline({cmd1, cmd2}, {task1, task2})

  assert_equal("PING\r\nPING\r\n", msg)

  stream:append("+PONG1\r\n")
  stream:append("+PONG2\r\n")

  stream:execute()

  assert_equal(2, i)
end)

it("pipeline should decodes stream by chunks", function()
  stream:on_command(PASS)

  local i,res = 0
  local cmd1, task1 = stream:pipeline_command("PING",
    function(_, err, data) assert_equal(0, i) i = i + 1 res = data end
  )

  local cmd2, task2 = stream:pipeline_command("PING",
    function(_, err, data) assert_equal(1, i) i = i + 1 res = data end
  )

  stream:pipeline({cmd1, cmd2}, {task1, task2})

  stream:append("+PONG1\r\n"):execute()
  assert_equal(1, i)
  assert_equal(res, "PONG1")

  stream:append("+PONG2\r\n"):execute()
  assert_equal(2, i)
  assert_equal(res, "PONG2")
end)

it("halt should calls every callback", function()
  local ERR = {}
  local called = {}

  stream:on_command(PASS)

  stream:command("***", function(_, err)
    assert_equal(ERR, err)
    called[1] = true
  end)
  
  stream:command("***", function(_, err)
    assert_equal(ERR, err)
    called[2] = true
  end)
  
  stream:command("***", function(_, err)
    assert_equal(ERR, err)
    called[3] = true
  end)

  stream:halt(ERR)

  assert_true(called[1])
  assert_true(called[2])
  assert_true(called[3])
end)

it("halt should clear stream", function()
  local ERR = {}
  local called = {}

  stream:on_command(PASS)

  assert(stream._buffer:empty()) --! @fixme do not use internal variable
  
  stream:append("+hello\r\n")
  stream:append("+world\r\n")

  assert(not stream._buffer:empty())

  stream:halt(ERR)

  assert(stream._buffer:empty())
end)

it("should decode nil", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("*-1\r\n"):execute()

  assert_nil(res)
end)

it("should decode arrays with holes", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  local str = table.concat{
    "*3\r\n",
      ":1\r\n",
      "*-1\r\n",
      ":3\r\n",
  }

  stream:append(str):execute()

  assert_table(res)
  assert_equal(1,res[1])
  assert_nil(res[2])
  assert_equal(3,res[3])
end)

end -- test case

local _ENV = TEST_CASE'redis stream transaction' if ENABLE then

local it = IT(_ENV or _M)

local stream

function setup()
  stream = assert(RedisStream.new())
end

it('should discard empty txn', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("DISCARD", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:append"+OK\r\n"
  stream:append"+OK\r\n"

  stream:execute()
end)

it('should commit empty txn', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("EXEC", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:append"+OK\r\n"
  stream:append"+OK\r\n"

  stream:execute()
end)

it('should call all callbacks on exec', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    assert_equal(1, res)
  end)

  stream:command("INCR bar", function(self, err, res)
    assert_equal(2, res)
  end)

  stream:command("EXEC", function(self, err, res)
    assert_table(res)
    assert_equal(1, res[1])
    assert_equal(2, res[2])
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"*2\r\n:1\r\n:2\r\n"

  stream:execute()
end)

it('should call all callbacks on discard', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    assert_equal("DISCARD", err)
  end)

  stream:command("INCR bar", function(self, err, res)
    assert_equal("DISCARD", err)
  end)

  stream:command("DISCARD", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+OK\r\n"

  stream:execute()
end)

it('should ingnore failed commands', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    assert_equal("DISCARD", err)
  end)

  stream:command("INCR a b c", function(self, err, res)
    assert_equal("ERR", err)
    assert_equal("wrong number of arguments for 'incr' command", res)
  end)

  stream:command("DISCARD", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"-ERR wrong number of arguments for 'incr' command\r\n"
  stream:append"+OK\r\n"

  stream:execute()
end)

it('should pass error to command callback', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("SET a 3", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("LPOP a", function(self, err, res)
    assert_equal("ERR", err)
    assert_equal("Operation against a key holding the wrong kind of value", res)
  end)

  stream:command("EXEC", function(self, err, res)
    local ERR = {error = 'ERR', info = 'Operation against a key holding the wrong kind of value'}
    assert_table(res)
    assert_equal("OK", res[1])
    assert(is_equal(ERR, res[2]))
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"*2\r\n"
  stream:append"+OK\r\n"
  stream:append"-ERR Operation against a key holding the wrong kind of value\r\n"

  stream:execute()
end)

it('halt should call txn callbacks', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    assert_equal("HALT", err)
  end)

  stream:command("INCR bar", function(self, err, res)
    assert_equal("HALT", err)
  end)

  stream:command("EXEC", function(self, err, res)
    assert_equal("HALT", err)
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"

  stream:execute()

  stream:halt"HALT"

end)

it('should support discard by WATCH command', function()
  stream:on_command(PASS)

  stream:command("WATCH a", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR a", function(self, err, res)
    assert_nil(res)
  end)

  stream:command("EXEC", function(self, err, res)
    assert_nil(res)
  end)

  stream:append"+OK\r\n"
  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"*-1\r\n"

  stream:execute()
end)

end

RUN()
