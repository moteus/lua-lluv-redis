package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local RedisStream    = require "lluv.redis.stream"
local RedisCommander = require "lluv.redis.commander"
local utils          = require "utils"
local TEST_CASE      = require "lunit".TEST_CASE

local pcall, error, type, table, ipairs, print = pcall, error, type, table, ipairs, print
local RUN = utils.RUN
local IT, CMD, PASS = utils.IT, utils.CMD, utils.PASS
local nreturn, is_equal = utils.nreturn, utils.is_equal

local C = function(t) return table.concat(t, '\r\n') .. '\r\n' end
local S = function(s) return C{"$" .. #s, s} end
local I = function(i) return C{":" .. tostring(i)} end
local A = function(a)
  local t = {"*" .. #a}
  for i, s in ipairs(a) do
    t[#t + 1] = "$" .. #s
    t[#t + 1] = s
  end
  return C(t)
end
local E = function(m) return C {"-" .. m} end

local ENABLE = true

local _ENV = TEST_CASE'redis command encoder/decoder' if ENABLE then

local it = IT(_ENV or _M)

local stream, command
local SELF = {}

function setup()
  stream  = assert(RedisStream.new(SELF))
  command = assert(RedisCommander.new(stream))
end

local test = {
  { "PING",
    function(cb) command:ping(cb) end;
    "PING\r\n",
    "+PONG\r\n",
    "PONG"
  };
  { "ECHO",
    function(cb) command:echo("HELLO", cb) end;
    A{"ECHO", "HELLO"},
    S"HELLO",
    "HELLO"
  };
  { "EXISTS true",
    function(cb) command:exists("key", cb) end;
    A{"EXISTS", "key"},
    C{":1"},
    true
  };
  { "EXISTS false",
    function(cb) command:exists("key", cb) end;
    C{"*2", "$6", "EXISTS", "$3", "key"},
    C{":0"},
    false
  };
  { "SET #1",
    function(cb) command:set("key", "value", cb) end;
    A{"SET", "key", "value"},
    C{"+OK"},
    "OK"
  };
  { "EVAL #1",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", "2", "key1", "key2", "first", "second", cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "2", "key1", "key2", "first", "second"
    },
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "EVAL #2",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", "2", {"key1", "key2"}, {"first", "second"}, cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "2", "key1", "key2", "first", "second"
    },
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "EVAL #3.1",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", "0", {"first", "second"}, cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "0", "first", "second"
    },
    -- This is invalid response, but we test only request in this test case
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "EVAL #3.2",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", {}, {"first", "second"}, cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "0", "first", "second"
    },
    -- This is invalid response, but we test only request in this test case
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "EVAL #3.3",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", {"first", "second"}, cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "2", "first", "second"
    },
    -- This is invalid response, but we test only request in this test case
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "EVAL #4",
    function(cb) command:eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", {"key1", "key2"}, {"first", "second"}, cb) end;
    A{
      "EVAL",
      "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
      "2", "key1", "key2", "first", "second"
    },
    A{"key1", "key2", "first", "second"},
    {"key1", "key2", "first", "second", n=4}
  };
  { "SORT #1",
    function(cb) command:sort("list", cb) end;
    A{"SORT","list"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SORT #2",
    function(cb) command:sort("list", "DESC", cb) end;
    A{"SORT","list", "DESC"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SORT #3",
    function(cb) command:sort("list", {"DESC", "ALPHA"}, cb) end;
    A{"SORT","list", "DESC", "ALPHA"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SORT #4",
    function(cb) command:sort("list", {desc = true, alpha = true}, cb) end;
    A{"SORT", "list", "DESC", "ALPHA"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SORT #5",
    function(cb) command:sort("list", {by = "weight_*", get = "object_*"}, cb) end;
    A{"SORT", "list", "BY", "weight_*", "GET", "object_*"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SORT #6",
    function(cb) command:sort("list", {
      by = "weight_*",
      get = {"object_*", "#"},
      limit = {"1", "10"},
      sort = 'asc',
      alpha = true,
      store = "key1"
    }, cb) end;
    A{"SORT", "list", "BY", "weight_*", "LIMIT", "1", "10",
      "GET", "object_*", "GET", "#", "ASC", "ALPHA", "STORE", "key1"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SCAN #1",
    function(cb) command:scan("0", cb) end;
    A{"SCAN", "0"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SCAN #2",
    function(cb) command:scan("0", {match = "a", count = "10"}, cb) end;
    A{"SCAN", "0", "MATCH", "a", "COUNT", "10"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "SCAN #3",
    function(cb) command:scan("0", function(self, err, ...)
      if err then cb(self, err, ...)
      else cb(self, err, {...}) end
    end) end;
    A{"SCAN", "0"},
    table.concat{"*2\r\n", S"17",
      A{ "key:611", "key:711", "key:118", "key:117", "key:311", "key:112",
         "key:111", "key:110", "key:113", "key:211", "key:411", "key:115",
         "key:116", "key:114", "key:119", "key:811", "key:511", "key:11"
      }
    }, -- ignore result
    {"17",
      { "key:611", "key:711", "key:118", "key:117", "key:311", "key:112",
        "key:111", "key:110", "key:113", "key:211", "key:411", "key:115",
        "key:116", "key:114", "key:119", "key:811", "key:511", "key:11",
        n=18
      }
    }
  };
  { "ZRANGEBYSCORE #1",
    function(cb) command:zrangebyscore("myzset", "1", "2", cb) end;
    A{"ZRANGEBYSCORE", "myzset", "1", "2"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "ZRANGEBYSCORE #2",
    function(cb) command:zrangebyscore("myzset", "1", "2", {withscores = true, limit = {"10", "20"}}, cb) end;
    A{"ZRANGEBYSCORE", "myzset", "1", "2", "WITHSCORES", "LIMIT", "10", "20"},
    C{"*-1"}, -- ignore result
    nil
  };
  { "INFO",
    function(cb) command:info(cb) end;
    C{"INFO"},
    S[[
# Replication
connected_slaves:1
slave1:id,address,port,state

# CPU
used_cpu_sys:1078.20
used_cpu_user:883.16
used_cpu_sys_children:26.32
used_cpu_user_children:319.44

# Cluster
cluster_enabled:0

# Keyspace
db0:keys=3210,expires=3,avg_ttl=128712033923
]];
    {
      replication={
      connected_slaves='1',
        [1] = {
          id    = 'id',
          port  = 'port',
          state = 'state',
          ip    = 'address'
        }
      };
      cpu={
        used_cpu_user_children = '319.44',
        used_cpu_sys           = '1078.20',
        used_cpu_sys_children  = '26.32',
        used_cpu_user          = '883.16'
      },
      keyspace={
        [0]={
          expires = '3',
          avg_ttl = '128712033923',
          keys    = '3210'
        }
      },
      cluster={
        cluster_enabled='0'
      },
    }
  };
  { "SCRIPT EXISTS",
    function(cb) command:script_exists("sha#1", "sha#2", "sha#3", cb) end;
    A{"SCRIPT", "EXISTS", "sha#1", "sha#2", "sha#3"},
    C{"*3", ":1", ":0", ":1"},
    {true, false, true, n=3},
  };
  { "WAIT nowait",
    function(cb) command:wait('1', '0', cb) end;
    A{"WAIT", "1", "0"},
    I(1),
    1
  };
  { "WAIT timeout",
    function(cb) command:wait('5', '3000', cb) end;
    A{"WAIT", "5", "3000"},
    I(2),
    2
  };
  { "TOUCH ERROR",
    function(cb) command:touch('key1', cb) end;
    A{"TOUCH", "key1"},
    E("ERR Unknown or disabled command 'TOUCH'"),
    nil,
    RedisStream.error(0, 'ERR', "Unknown or disabled command 'TOUCH'"),
  };
  { "TOUCH boolean",
    function(cb) command:touch('key1', cb) end;
    A{"TOUCH", "key1"},
    C{"+OK"},
    true
  };
  { "TOUCH integer",
    function(cb) command:touch('key1', 'key2', cb) end;
    A{"TOUCH", "key1", "key2", },
    I(1),
    1
  };
  { "UNLINK ERROR",
    function(cb) command:unlink('key1', cb) end;
    A{"UNLINK", "key1"},
    E("ERR Unknown or disabled command 'TOUCH'"),
    nil,
    RedisStream.error(0, 'ERR', "Unknown or disabled command 'UNLINK'"),
  };
  { "UNLINK boolean",
    function(cb) command:unlink('key1', cb) end;
    A{"UNLINK", "key1"},
    C{"+OK"},
    true
  };
  { "UNLINK integer",
    function(cb) command:unlink('key1', 'key2', cb) end;
    A{"UNLINK", "key1", "key2", },
    I(1),
    1
  };
  { "SWAPDB ERROR",
    function(cb) command:swapdb('1', '0', cb) end;
    A{"SWAPDB", "1", "0"},
    E("ERR Unknown or disabled command 'SWAPDB'"),
    nil,
    RedisStream.error(0, 'ERR', "Unknown or disabled command 'SWAPDB'"),
  };
  { "SWAPDB",
    function(cb) command:swapdb('1', '0', cb) end;
    A{"SWAPDB", "1", "0"},
    C{"+OK"},
    true
  };
}

for _, t in ipairs(test) do
  local NAME, FN, REQUEST, RESPONSE, RESULT, ERR = t[1], t[2], t[3], t[4], t[5], t[6]

  it( NAME .. " command", function()
    local msg, called

    stream:on_command(function(self, cmd)
      assert_equal(SELF, self)
      msg = CMD(cmd)
      return true
    end)

    FN(function(self, err, data, ...)
      called = true
      if type(RESULT) == "table" then
        assert_table(data)
        assert(is_equal(RESULT, data))
      else
        assert_equal(RESULT, data)
      end
      assert_equal(ERR, err)
    end)

    assert_equal(REQUEST, msg)

    stream:append(RESPONSE):execute()

    assert_true(called)

  end)
end

it("echo no args", function()
  stream:on_command(PASS)
  assert_pass(function()
    command:echo(PASS)
  end)
end)

it("echo with multiple args", function()
  stream:on_command(PASS)
  assert_pass(function()
    command:echo("HELLO", "WORLD", PASS)
  end)
end)

it("echo with multiple args and without cb", function()
  stream:on_command(PASS)
  assert_pass(function()
    command:echo("HELLO", "WORLD")
  end)
end)

it("command callback should get correct self", function()
  stream:on_command(PASS)
  command:ping(function(self, err, data)
    assert_equal(SELF, self)
    assert_equal("PONG", data)
  end)
  stream:append("+PONG\r\n"):execute()
end)

it("multiple args with single argument", function()
  local cmd, called
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  command:del("a", function(self, err, n)
    called = true
    assert_equal(1, n)
  end)

  local res = C{"*2", "$3", "DEL", "$1", "a"}
  assert_equal(res, cmd)

  stream:append(":1\r\n"):execute()

  assert_true(called)
end)

it("multiple args with single argument without callback", function()
  local cmd
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  command:del("a")

  local res = C{"*2", "$3", "DEL", "$1", "a"}
  assert_equal(res, cmd)
end)

it("multiple args with two arguments", function()
  local cmd, called
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  command:del("a", "b", function(self, err, n)
    called = true
    assert_equal(1, n)
  end)

  local res = C{"*3", "$3", "DEL", "$1", "a", "$1", "b"}
  assert_equal(res, cmd)

  stream:append(":1\r\n"):execute()

  assert_true(called)
end)

it("multiple args with two arguments without callback", function()
  local cmd
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  command:del("a", "b")

  local res = C{"*3", "$3", "DEL", "$1", "a", "$1", "b"}
  assert_equal(res, cmd)
end)

it("multiple args as array", function()
  local cmd, called
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  command:del({"a", "b"}, function(self, err, n)
    called = true
    assert_equal(1, n)
  end)

  local res = C{"*3", "$3", "DEL", "$1", "a", "$1", "b"}
  assert_equal(res, cmd)

  stream:append(":1\r\n"):execute()

  assert_true(called)
end)

it("result as hash", function()
  local cmd, called
  stream:on_command(PASS)

  command:hgetall("h", function(self, err, t)
    called = true
    assert_table(t)
    assert_equal("v1", t.f1)
    assert_equal("v2", t.f2)
  end)

  local res = C{
    "*4",
      "$2","f1",
      "$2","v1",
      "$2","f2",
      "$2","v2",
  }
  stream:append(res):execute()

  assert_true(called)
end)

end

local _ENV = TEST_CASE'redis pipeline command' if ENABLE then

local it = IT(_ENV or _M)

local stream, command, pipeline
local SELF = {}

function setup()
  stream   = assert(RedisStream.new(SELF))
  command  = assert(RedisCommander.new(stream))
  pipeline = assert(command:pipeline())
end

it('should execute multiple command', function()
  local called, cmd = 0
  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)
  
  pipeline:ping(function()
    called = assert_equal(0, called) + 1
  end)

  pipeline:ping(function()
    called = assert_equal(1, called) + 1
  end)

  assert_nil(cmd)

  pipeline:execute()

  local res = C{"PING", "PING"}
  assert_equal(res, cmd)

  stream:append"+PONG\r\n"
  stream:append"+PONG\r\n"
  stream:execute()

  assert_equal(2, called)
end)

it('should execute multiple times', function()
  local called, cmd = 0
  local req = C{
    "*2", "$4", "ECHO", "$9", "message#1",
    "*2", "$4", "ECHO", "$9", "message#2",
  }
  local res = C{
    "$9", "message#1",
    "$9", "message#2",
  }

  stream:on_command(function(_, msg)
    cmd = CMD(msg)
    return true
  end)

  pipeline:echo("message#1", function(self, err, data)
    called = assert_equal(0, called) + 1
    assert_equal("message#1", data)
  end)

  pipeline:echo("message#2", function()
    called = assert_equal(1, called) + 1
  end)

  assert_nil(cmd)

  pipeline:execute(true)
  assert_equal(req, cmd)

  stream:append(res)
  stream:execute()
  assert_equal(2, called)

  called, cmd = 0

  pipeline:execute(true)
  assert_equal(req, cmd)

  stream:append(res)
  stream:execute()
  assert_equal(2, called)
end)

end

RUN()
