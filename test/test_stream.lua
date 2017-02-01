package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local RedisStream = require "lluv.redis.stream"
local utils       = require "utils"
local TEST_CASE   = require "lunit".TEST_CASE

local pcall, error, type, table, tostring, print, debug = pcall, error, type, table, tostring, print, debug
local RUN = utils.RUN
local IT, CMD, PASS = utils.IT, utils.CMD, utils.PASS
local nreturn, is_equal = utils.nreturn, utils.is_equal
local C = function(t) return table.concat(t, '\r\n') .. '\r\n' end

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
  assert_function(stream.on_command)
  assert_function(stream.on_halt)
  assert_function(stream.on_message)
end)

it("should fail without on_command callback", function()
  assert_error(function() stream:command("PING", function() end) end)
end)

it("on_command callback signature", function()
  local f = false
  stream:on_command(function(...)
    f = true
    local n, a, b, c = nreturn(...)
    assert_equal(2, n)
    assert_equal(stream, a)
    if type(b) ~= "string" then assert_table(b) end
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

  local res = C{
    "*2",
    "$4",  "ECHO",
    "$11", "Hello world"
  }

  assert_equal(res, msg)
end)

it("should encode command with array args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = C{
    "*7",
    "$4", "ECHO",
    "$1", "1",
    "$1", "2",
    "$1", "3",
    "$3", "Foo",
    "$3", "Bar",
    "$6", "foobar",
  }

  stream:command({"ECHO", {"1","2","3"}, {'Foo','Bar','foobar'}}, PASS)

  assert_equal(res, msg)
end)

it("should encode command with nested array args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = C{
    "*7",
    "$4", "ECHO",
    "$1", "1",
    "$1", "2",
    "$1", "3",
    "$3", "Foo",
    "$3", "Bar",
    "$6", "foobar",
  }

  stream:command({{"ECHO", {"1",{"2","3"}}, {'Foo',{'Bar',{'foobar'}}}}}, PASS)

  assert_equal(res, msg)
end)

it("should encode command with hash args", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = C{
    "*7",
    "$4", "ECHO",
    "$1", "1",
    "$1", "2",
    "$1", "3",
    "$3", "Foo",
    "$3", "Bar",
    "$6", "foobar",
  }

  stream:command({"ECHO", "1","2","3", {Foo = 'Bar'}, 'foobar'}, PASS)

  assert_equal(res, msg)
end)

it("should encode command with empty array", function()
  local msg
  stream:on_command(function(_, cmd)
    msg = CMD(cmd)
  end)

  local res = C{"*1", "$4", "ECHO"}
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
  stream:command("PING2", function(self, ...)
    err, res = ...
  end)

  stream:append("-ERR unknown command 'PING2'\r\n"):execute()

  assert(RedisStream.IsError(err))
  assert_equal(0,                         err:no()  )
  assert_equal('REDIS',                   err:cat() )
  assert_equal('ERR',                     err:name())
  assert_equal("unknown command 'PING2'", err:msg() )
  assert_equal('PING2',                   err:ext() )
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

  local a = assert_table(res)[1]
  assert_table(a)
  assert_equal(1, a[1])
  assert_equal(2, a[2])
  assert_equal(3, a[3])

  local a = assert_table(res)[2]
  assert_table(a)
  assert_equal('Foo',        a[1]       )
  assert(RedisStream.IsError(a[2])      )
  assert_equal('REDIS',      a[2]:cat() )
  assert_equal(0,            a[2]:no()  )
  assert_equal('Bar',        a[2]:name())
  assert_equal('',           a[2]:msg() )
  assert_equal('***',        a[2]:ext() )
  assert_equal('foobar',     a[3])

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

  local a = assert_table(res)[1]
  assert_table(a)
  assert_equal(1, a[1])
  assert_equal(2, a[2])
  assert_equal(3, a[3])

  local a = assert_table(res)[2]
  assert_table(a)
  assert_equal('Foo',        a[1]       )
  assert(RedisStream.IsError(a[2])      )
  assert_equal('REDIS',      a[2]:cat() )
  assert_equal(0,            a[2]:no()  )
  assert_equal('Bar',        a[2]:name())
  assert_equal('',           a[2]:msg() )
  assert_equal('***',        a[2]:ext() )
  assert_equal('foobar',     a[3])
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

it("should decode array nil", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("*-1\r\n"):execute()

  assert_nil(res)
end)

it("should decode bulk nil", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end)

  stream:append("$-1\r\n"):execute()

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
    "*5\r\n",
      ":1\r\n",
      "*-1\r\n",
      ":3\r\n",
      "$-1\r\n",
      ":5\r\n",
  }

  stream:append(str):execute()

  assert_table(res)
  assert_equal(1,res[1])
  assert_nil(res[2])
  assert_equal(3,res[3])
  assert_nil(res[4])
  assert_equal(5,res[5])
end)

it("should use custom result decoder", function()
  stream:on_command(PASS)

  local res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res = data
  end, function(err, res)
    assert_equal("PONG", res)
    return nil, true
  end)

  stream:append("+PONG\r\n"):execute()

  assert_equal(true, res)
end)

it("pass error to exec", function()
  local called
  stream:on_command(PASS)
  stream:command("EXEC", function(cli, err, data)
    called = true
    assert(RedisStream.IsError(err))
    assert_equal(0,                     err:no()  )
    assert_equal('REDIS',               err:cat() )
    assert_equal('ERR',                 err:name())
    assert_equal("EXEC without MULTI",  err:msg() )
    assert_equal('EXEC',                err:ext() )
  end)

  stream:append("-ERR EXEC without MULTI\r\n"):execute()

  assert_true(called)
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
  local called = 0
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    called = assert_equal(0, called) + 1
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    called = assert_equal(1, called) + 1
    assert_equal(1, res)
  end)

  stream:command("INCR bar", function(self, err, res)
    called = assert_equal(2, called) + 1
    assert_equal(2, res)
  end)

  stream:command("EXEC", function(self, err, res)
    called = assert_equal(3, called) + 1
    assert_table(res)
    assert_equal(1, res[1])
    assert_equal(2, res[2])
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"*2\r\n:1\r\n:2\r\n"

  stream:execute()
  assert_equal(4, called)
end)

it('should call all callbacks on discard', function()
  local called = 0
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    called = assert_equal(0, called) + 1
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    called = assert_equal(1, called) + 1
    assert_equal("DISCARD", err)
  end)

  stream:command("INCR bar", function(self, err, res)
    called = assert_equal(2, called) + 1
    assert_equal("DISCARD", err)
  end)

  stream:command("DISCARD", function(self, err, res)
    called = assert_equal(3, called) + 1
    assert_equal("OK", res)
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+OK\r\n"

  stream:execute()
  assert_equal(4, called)
end)

it('should call all callbacks on watch fail', function()
  local called = 0
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    called = assert_equal(0, called) + 1
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    called = assert_equal(1, called) + 1
    assert_nil(err)
    assert_nil(res)
  end)

  stream:command("INCR bar", function(self, err, res)
    called = assert_equal(2, called) + 1
    assert_nil(err)
    assert_nil(res)
  end)

  stream:command("EXEC", function(self, err, res)
    called = assert_equal(3, called) + 1
    assert_nil(err)
    assert_nil(res)
  end)

  stream:append"+OK\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"+QUEUED\r\n"
  stream:append"*-1\r\n"

  stream:execute()
  assert_equal(4, called)
end)

it('should ingnore failed commands', function()
  stream:on_command(PASS)

  stream:command("MULTI", function(self, err, res)
    assert_equal("OK", res)
  end)

  stream:command("INCR foo", function(self, err, res)
    assert_equal("DISCARD", err)
  end)

  stream:command({"INCR", "a", "b", "c"}, function(self, err, res)
    assert(RedisStream.IsError(err))
    assert_equal(0,                                               err:no()  )
    assert_equal('REDIS',                                         err:cat() )
    assert_equal('ERR',                                           err:name())
    assert_equal("wrong number of arguments for 'incr' command",  err:msg() )
    assert_equal('INCR',                                          err:ext() )
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
    assert(RedisStream.IsError(err))
    assert_equal(0,                                                         err:no()  )
    assert_equal('REDIS',                                                   err:cat() )
    assert_equal('ERR',                                                     err:name())
    assert_equal("Operation against a key holding the wrong kind of value", err:msg() )
    assert_equal('EXEC',                                                    err:ext() )
  end)

  stream:command("EXEC", function(self, err, res, cmd_err)
    local ERR = {error = 'ERR', info = 'Operation against a key holding the wrong kind of value'}
    assert_table(res)
    assert_table(cmd_err)
    assert_nil  (           cmd_err[1])
    assert_equal("OK",          res[1])

    local err = cmd_err[2]
    assert(RedisStream.IsError(err))
    assert_equal(0,         err:no()  )
    assert_equal('REDIS',   err:cat() )
    assert_equal(ERR.error, err:name())
    assert_equal(ERR.info,  err:msg() )
    assert_equal('EXEC',    err:ext() )
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

local _ENV = TEST_CASE'redis pub/sub protocol' if ENABLE then

local it = IT(_ENV or _M)

local stream

function setup()
  stream = assert(RedisStream.new())
end

it('should subscribe', function()
  local typ, ch, msg, called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":1\r\n"
  stream:execute()

  assert_true(called)
  assert_equal("subscribe", typ)
  assert_equal("hello",     ch)
  assert_equal(1,           msg)

  called = false

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$4\r\n"
  stream:append"text\r\n"
  stream:execute()
  
  assert_true(called)
  assert_equal("message",  typ)
  assert_equal("hello",    ch)
  assert_equal("text",     msg)
end)

it('should accept message before subscribe response', function()
  local typ, ch, msg, called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$4\r\n"
  stream:append"text\r\n"
  stream:execute()

  assert_true(called)
  assert_equal("message",  typ)
  assert_equal("hello",    ch)
  assert_equal("text",     msg)

  called = false

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":1\r\n"
  stream:execute()

  assert_true(called)
  assert_equal("subscribe", typ)
  assert_equal("hello",     ch)
  assert_equal(1,           msg)
end)

it('should halt with unexpected messages', function()
  local halt_called
  stream:on_command(PASS):on_message(PASS)
  :on_halt(function(self, err)
    halt_called = true
    assert(RedisStream.IsError(err))
    assert_equal('REDIS',  err:cat())
    assert_equal('EPROTO', err:name())
    assert_match('REDIS',  tostring(err))
    assert_match('EPROTO', tostring(err))
  end)

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"

  assert_pass(function()
    stream:execute()
  end)
  assert_true(halt_called)
end)

it('should fail with message after unsubscribe', function()
  local typ, ch, msg, called, halt_called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)
  :on_halt(function(self, err)
    halt_called = true
    assert(RedisStream.IsError(err))
    assert_equal('REDIS',  err:cat())
    assert_equal('EPROTO', err:name())
    assert_match('REDIS',  tostring(err))
    assert_match('EPROTO', tostring(err))
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":2\r\n"
  stream:execute()

  called = false

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:execute()

  assert_true(called)
  assert_equal("hello",   ch)
  assert_equal("message", msg)

  stream:command({"UNSUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$11\r\n"
  stream:append"unsubscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":0\r\n"
  stream:execute()

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  assert_pass(function()
    stream:execute()
  end)
  assert_true(halt_called)
end)

it('should proceed error responses in sub mode', function()
  local typ, ch, msg, called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  local ping_called

  stream:command("PING", function(self, err, data)
    assert(not ping_called)
    ping_called = 1

    assert(RedisStream.IsError(err))
    assert_equal(0,                                                                   err:no()  )
    assert_equal('REDIS',                                                             err:cat() )
    assert_equal('ERR',                                                               err:name())
    assert_equal("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context", err:msg() )
    assert_equal('PING',                                                              err:ext() )
  end)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":2\r\n"
  stream:append"-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context\r\n"
  stream:execute()

  assert_true(called)
  assert_equal("subscribe", typ)
  assert_equal("hello",     ch)
  assert_equal(2,           msg)

  called = false

  assert_equal(1, ping_called)

  stream:command("PING", function(self, err, data)
    assert_equal(1, ping_called)
    ping_called = 2
    assert(RedisStream.IsError(err))
    assert_equal(0,                                                                   err:no()  )
    assert_equal('REDIS',                                                             err:cat() )
    assert_equal('ERR',                                                               err:name())
    assert_equal("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context", err:msg() )
    assert_equal('PING',                                                              err:ext() )
  end)

  stream:command("PING", function(self, err, data)
    assert_equal(2, ping_called)
    ping_called = 3
    assert(RedisStream.IsError(err))
    assert_equal(0,                                                                   err:no()  )
    assert_equal('REDIS',                                                             err:cat() )
    assert_equal('ERR',                                                               err:name())
    assert_equal("only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context", err:msg() )
    assert_equal('PING',                                                              err:ext() )
  end)

  stream:append"-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context\r\n"
  stream:execute()
  assert_equal(2, ping_called)

  stream:append"*3\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  stream:execute()
  assert_equal(2, ping_called)
  assert_true(called)
  assert_equal("message", typ)
  assert_equal("hello",   ch)
  assert_equal("message", msg)

  stream:append"-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context\r\n"
  stream:execute()
  assert_equal(3, ping_called)

end)

it('should fail with invalid async array message', function()
  local typ, ch, msg, called, halt_called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)
  :on_halt(function(self, err)
    halt_called = true
    assert(RedisStream.IsError(err))
    assert_equal('REDIS',  err:cat())
    assert_equal('EPROTO', err:name())
    assert_match('REDIS',  tostring(err))
    assert_match('EPROTO', tostring(err))
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":2\r\n"
  stream:execute()

  called = false

  stream:append"*3\r\n"
  stream:append"$11\r\n"
  stream:append"unsupported\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  assert_pass(function()
    stream:execute()
  end)

  assert_true(halt_called)
end)

it('should fail with invalid async empty array message', function()
  local typ, ch, msg, called, halt_called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)
  :on_halt(function(self, err)
    halt_called = true
    assert(RedisStream.IsError(err))
    assert_equal('REDIS',  err:cat())
    assert_equal('EPROTO', err:name())
    assert_match('REDIS',  tostring(err))
    assert_match('EPROTO', tostring(err))
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":2\r\n"
  stream:execute()

  called = false

  stream:append"*3\r\n"
  stream:append"$-1\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append"$7\r\n"
  stream:append"message\r\n"
  assert_pass(function()
    stream:execute()
  end)

  assert_true(halt_called)
end)

it('should fail with invalid async int message', function()
  local typ, ch, msg, called, halt_called
  stream:on_command(PASS)
  :on_message(function(self, mtyp, channel, message)
    called = true
    typ, ch, msg = mtyp, channel, message
  end)
  :on_halt(function(self, err)
    halt_called = true
    assert(RedisStream.IsError(err))
    assert_equal('REDIS',  err:cat())
    assert_equal('EPROTO', err:name())
    assert_match('REDIS',  tostring(err))
    assert_match('EPROTO', tostring(err))
  end)

  stream:command({"SUBSCRIBE", "hello"}, PASS)

  stream:append"*3\r\n"
  stream:append"$9\r\n"
  stream:append"subscribe\r\n"
  stream:append"$5\r\n"
  stream:append"hello\r\n"
  stream:append":2\r\n"
  stream:execute()

  stream:append":47\r\n"

  assert_error(function()
    stream:execute()
  end)

  assert_nil(halt_called)
end)

end

local ENABLE = true

local _ENV = TEST_CASE'redis stream callback' if ENABLE then

local it = IT(_ENV or _M)

local stream

function setup()
  stream = assert(RedisStream.new())
end

it("should remove task after bulk nil", function()
  stream:on_command(PASS)

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called1 = true
  end)

  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called2 = true
  end)

  stream:append("$-1\r\n"):execute()

  assert_true(called1)
  assert_nil (called2)

  called1, called2 = nil

  stream:append("$-1\r\n"):execute()

  assert_nil (called1)
  assert_true(called2)
end)

it("should remove task after array nil", function()
  stream:on_command(PASS)

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called1 = true
  end)

  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called2 = true
  end)

  stream:append("*-1\r\n"):execute()

  assert_true(called1)
  assert_nil (called2)

  called1, called2 = nil

  stream:append("*-1\r\n"):execute()

  assert_nil (called1)
  assert_true(called2)
end)

it("should remove task after array", function()
  stream:on_command(PASS)

  local a = C{ "*2",
    "$5", "HELLO",
    ":123456"
  }

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called1 = true
    res     = data
  end)

  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called2 = true
    res     = data
  end)

  stream:append(a):execute()

  assert_true(called1)
  assert_nil (called2)
  assert_table(res)
  assert_equal('HELLO', res[1])
  assert_equal(123456,  res[2])

  called1, called2 = nil

  stream:append(a):execute()

  assert_nil (called1)
  assert_true(called2)
  assert_table(res)
  assert_equal('HELLO', res[1])
  assert_equal(123456,  res[2])
end)

it("should remove task after bulk", function()
  stream:on_command(PASS)

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res     = data
    called1 = true
  end)

  stream:command("PING", function(self, err, data)
    assert_nil(err)
    res     = data
    called2 = true
  end)

  stream:append(C{"$5", "HELLO"}):execute()

  assert_true  (called1)
  assert_nil   (called2)
  assert_equal ('HELLO', res)

  called1, called2 = nil

  stream:append(C{"$5", "WORLD"}):execute()

  assert_nil   (called1)
  assert_true  (called2)
  assert_equal ('WORLD', res)
end)

it("should remove task after error", function()
  stream:on_command(PASS)

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    res     = err
    called1 = true
  end)

  stream:command("PING", function(self, err, data)
    res     = err
    called2 = true
  end)

  stream:append("-GENERAL hello\r\n"):execute()

  assert_true  (called1)
  assert_nil   (called2)
  assert(res)
  assert_equal ('GENERAL', res:name())

  called1, called2 = nil

  stream:append("-ERROR hello\r\n"):execute()

  assert_nil   (called1)
  assert_true  (called2)
  assert       (res)
  assert_equal ('ERROR', res:name())
end)

it("should remove task after pass", function()
  stream:on_command(PASS)

  local called1, called2, res
  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called1 = true
  end)

  stream:command("PING", function(self, err, data)
    assert_nil(err)
    called2 = true
  end)

  stream:append("+OK\r\n"):execute()

  assert_true(called1)
  assert_nil (called2)

  called1, called2 = nil

  stream:append("+OK\r\n"):execute()

  assert_nil (called1)
  assert_true(called2)
end)
end


RUN()
