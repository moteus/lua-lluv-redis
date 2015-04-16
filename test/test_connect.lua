package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local Redis = require "lluv.redis"
local uv    = require "lluv"

local TEST_PORT = '5555'
local TEST_ADDRESS = '127.0.0.1:' .. TEST_PORT

local function TcpServer(host, port, cb)
  if not cb then
    host, port, cb = '127.0.0.1', host, port
  end

  return uv.tcp():bind(host, port, function(srv, err)
    if err then
      srv:close()
      return cb(srv, err)
    end

    srv:listen(function(srv, err)
      if err then
        srv:close()
        return cb(srv, err)
      end
      cb(srv:accept())
    end)
  end)
end

local C = function(t) return table.concat(t, '\r\n') .. '\r\n' end

local EOF = uv.error('LIBUV', uv.EOF)

local function test_1()
  io.write("Test 1 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:close()
  end)

  local cli = Redis.Connection.new(TEST_ADDRESS)

  local c = 1

  uv.timer():start(1000, function()

  cli:open(function(s, err)
    assert(s == cli)
    assert(not err, tostring(err))
    assert(1 == c) c = c + 1
  end)

  cli:open(function(s, err)
    assert(s == cli)
    assert(not err, tostring(err))
    assert(2 == c) c = c + 1
    cli:close()
    srv:close()
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 3)

  io.write("OK\n")
end

local function test_2()
  io.write("Test 2 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
    end)

    local res = C{"$2", "OK"}
    cli:write(res)
  end)

  local cli = Redis.Connection.new(TEST_ADDRESS)

  local c = 1

  uv.timer():start(1000, function()

  cli:open(function(s, err)
    assert(not err, tostring(err))
    assert(1 == c) c = c + 1
    cli:set("A", "10", function(s, err, res)
      assert(not err, tostring(err))
      assert(res == 'OK')
      assert(2 == c) c = c + 1
      cli:close(function()
        srv:close()
      end)
    end)
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 3)

  io.write("OK\n")
end

local function test_3()
  io.write("Test 3 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
    end)

    local res = C{"$2", "OK"}
    cli:write(res)
  end)

  local cli = Redis.Connection.new(TEST_ADDRESS)

  local c = 1

  uv.timer():start(1000, function()

  cli:open(function(s, err)
    assert(not err, tostring(err))
    assert(1 == c) c = c + 1
  end)

  cli:set("A", "10", function(s, err, res)
    assert(not err, tostring(err))
    assert(res == 'OK')
    assert(2 == c) c = c + 1
    cli:close(function()
      srv:close()
    end)
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 3)

  io.write("OK\n")
end

local function test_4()
  io.write("Test 4 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
    end)

    local res = C{"$2", "OK"}
    cli:write(res)
  end)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new(TEST_ADDRESS)

  cli:open(function(s, err)
    assert(s == cli)
    assert(err == EOF, tostring(err))
    assert(1 == c, c) c = c + 1
  end)

  cli:set("A", "10", function(s, err, res)
    assert(s == cli)
    assert(err == EOF, tostring(err))
    assert(res == nil)
    assert(2 == c, c) c = c + 1
  end)

  cli:close(function(s, err)
    assert(3 == c, c) c = c + 1
    assert(s == cli)
    assert(not err, tostring(err))
    srv:close()
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 4)

  io.write("OK\n")
end

local function test_5()
  -- Interrupt Connection on first command
  -- Call all callbacks with same error
  -- on_error handler calls only once
  ------------------------------------------
  io.write("Test 5 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
    end)

    local res = C{"2", "OK"}
    cli:write(res)
  end)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new(TEST_ADDRESS)

  cli:open(function(s, err)
    assert(s == cli)
    assert(1 == c, c) c = c + 1
  end)

  cli:set("A", "10", function(s, err, res)
    assert(s == cli)
    assert(err)
    assert(err:name() == 'EPROTO', tostring(err))
    assert(res == nil)
    assert(2 == c, c) c = c + 1
  end)

  cli:set("A", "10", function(s, err, res)
    assert(s == cli)
    assert(err)
    assert(err:name() == 'EPROTO', tostring(err))
    assert(res == nil)
    assert(3 == c, c) c = c + 1
  end)

  cli:on_error(function(s, err)
    assert(s == cli)
    assert(err)
    assert(err:name() == 'EPROTO', tostring(err))
    assert(4 == c, c) c = c + 1
    assert(s._cnn:closing())
    cli:close(function()
      srv:close()
    end)
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 5)

  io.write("OK\n")
end

test_1()
test_2()
test_3()
test_4()
test_5()