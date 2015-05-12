package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local Redis = require "lluv.redis"
local uv    = require "lluv"
local ut    = require "lluv.utils"

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
  -- Open multiple times
  -- Open already opened connection
  ---------------------------------------
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
    cli:open(function(s, err)
      assert(s == cli)
      assert(not err, tostring(err))
      assert(3 == c) c = c + 1
      cli:close()
      srv:close()
    end)
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 4)

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

local function test_6()
  -- Multiple open. First open cb close cnn.
  -- Other open callbacks end with error.
  -- Try open during closing (open fail)
  ------------------------------------------
  io.write("Test 6 - ")

  local srv = TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
    end)
  end)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new(TEST_ADDRESS)

  cli:open(function(s, err)
    assert(s == cli)
    assert(1 == c, c) c = c + 1
    cli:close(function(s, err)
      srv:close()
      assert(4 == c, c) c = c + 1
      assert(not err, tostring(err))
    end)
    cli:open(function(s, err)
      assert(3 == c, c) c = c + 1
      assert(err == EOF, tostring(err))
    end)
  end)

  cli:open(function(s, err)
    assert(2 == c, c) c = c + 1
    assert(err == EOF, tostring(err))
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 5)

  io.write("OK\n")
end

local Req = ut.class() do

function Req:__init(req, res)
  self._t = req
  self._d = res
  self._i = 1
  return self
end

function Req:next(str)
  local n = self._t[self._i]
  if n == str then
    self._i = self._i + 1
    return true
  end

  return nil, n
end

function Req:final()
  if nil == self._t[self._i] then
    return self._d or true
  end
end

end

local function ReqTestServer(port, req)
  return TcpServer(TEST_PORT, function(cli, err)
    assert(not err, tostring(err))
    local buffer = ut.Buffer.new('\r\n')
    cli:start_read(function(_, err, data)
      if err then return cli:close() end
      buffer:append(data)
      while true do
        local line = buffer:read_line()
        if not line then break end
        local ok, exp = req:next(line)
        assert(ok, 'Expected: ' .. tostring(exp) .. ' but got:' .. line)
        local res = req:final()
        if res then cli:write(res) end
      end
    end)
  end)
end

local function test_7()
  -- Auth and select on open
  ------------------------------------------
  io.write("Test 7 - ")

  local req = Req.new({
  "*2",
    "$4",
    "AUTH",
    "$6",
    "123456",
  "*2",
    "$6",
    "SELECT",
    "$2",
    "15",
  }, {
    C{"$2", "OK"},
    C{"$2", "OK"}
  })

  local srv = ReqTestServer(TEST_PORT, req)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new{
    server = TEST_ADDRESS;
    db     = 15;
    pass   = '123456';
  }

  cli:open(function(s, err, data)
    assert(s == cli)
    assert(1 == c, c) c = c + 1
    assert(not err, tostring(err))
    assert(data == 'OK')
    cli:close()
    srv:close()
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 2)

  io.write("OK\n")
end

local function test_8()
  -- Auth and select on open
  -- Fail on Auth
  ------------------------------------------
  io.write("Test 8 - ")

  local req = Req.new({
  "*2",
    "$4",
    "AUTH",
    "$6",
    "123456",
  "*2",
    "$6",
    "SELECT",
    "$2",
    "15",
  }, {
    C{"-ERR Auth fail"},
    C{"$2", "OK"}
  })

  local srv = ReqTestServer(TEST_PORT, req)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new{
    server = TEST_ADDRESS;
    db     = 15;
    pass   = '123456';
  }

  cli:open(function(s, err, data)
    assert(s == cli)
    assert(1 == c, c) c = c + 1
    assert(err)
    assert(err:cat() == 'REDIS', tostring(err))
    assert(err:msg() == 'Auth fail', tostring(err))
    assert(not data, data)

    cli:close()
    srv:close()
  end)

  end)

  uv.run(debug.traceback)

  assert(c == 2)

  io.write("OK\n")
end

local function test_9()
  -- Auth and select on open
  -- No commands before open pass
  ------------------------------------------
  io.write("Test 9 - ")

  local req = Req.new({
  "*2",
    "$4",
    "AUTH",
    "$6",
    "123456",
  "*2",
    "$6",
    "SELECT",
    "$2",
    "15",
  }, {
    C{"-ERR Auth fail"},
    C{"$2", "OK"}
  })

  local srv = ReqTestServer(TEST_PORT, req)

  local c = 1

  uv.timer():start(1000, function()

  local cli = Redis.Connection.new{
    server = TEST_ADDRESS;
    db     = 15;
    pass   = '123456';
  }

  cli:open(function(s, err, data)
    assert(s == cli)
    assert(1 == c, c) c = c + 1
    assert(err)
    assert(err:cat() == 'REDIS', tostring(err))
    assert(err:msg() == 'Auth fail', tostring(err))
    assert(not data, data)

    cli:close()
    srv:close()
  end)

  cli:set('A', "10")

  end)

  uv.run(debug.traceback)

  assert(c == 2)

  io.write("OK\n")
end

test_1()
test_2()
test_3()
test_4()
test_5()
test_6()
test_7()
test_8()
test_9()
