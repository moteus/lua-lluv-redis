package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local RedisStream    = require "lluv.redis.stream"
local RedisCommander = require "lluv.redis.commander"
local utils          = require "utils"
local TEST_CASE      = require "lunit".TEST_CASE

local pcall, error, type, table, ipairs = pcall, error, type, table, ipairs
local RUN = utils.RUN
local IT, CMD, PASS = utils.IT, utils.CMD, utils.PASS
local nreturn, is_equal = utils.nreturn, utils.is_equal

local ENABLE = true

local _ENV = TEST_CASE'redis command encoder/decoder' if ENABLE then

local it = IT(_ENV or _M)

local stream, command
local SELF = {}

function setup()
  stream  = assert(RedisStream.new(SELF))
  command = assert(RedisCommander.new(stream))
end

local C = function(t) return table.concat(t, '\r\n') .. '\r\n' end

local test = {
  { "PING",
    function(cb) command:ping(cb) end;
    "PING\r\n",
    "+PONG\r\n",
    "PONG"
  };
  { "ECHO",
    function(cb) command:echo("HELLO", cb) end;
    C{"*2", "$4", "ECHO", "$5", "HELLO"},
    C{"$5", "HELLO"},
    "HELLO"
  };
  { "EXISTS true",
    function(cb) command:exists("key", cb) end;
    C{"*2", "$6", "EXISTS", "$3", "key"},
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
    C{"*3", "$3", "SET", "$3", "key", "$5", "value"},
    C{"+OK"},
    true
  };
}

for _, t in ipairs(test) do
  local NAME, FN, REQUEST, RESPONSE, RESULT = t[1], t[2], t[3], t[4], t[5]

  it( NAME .. " command", function()
    local msg, called

    stream:on_command(function(self, cmd)
      assert_equal(stream, self)
      msg = CMD(cmd)
      return true
    end)
    
    FN(function(self, err, data)
      called = true
      assert_equal(SELF, self)
      assert_equal(RESULT, data)
    end)

    assert_equal(REQUEST, msg)

    stream:append(RESPONSE):execute()

    assert_true(called)

  end)
end

it("echo no args", function()
  stream:on_command(PASS)
  assert_error(function()
    command:echo(PASS)
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

end

RUN()
