package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local RedisStream    = require "lluv.redis.stream"
local RedisCommander = require "lluv.redis.commander"
local utils          = require "utils"
local TEST_CASE      = require "lunit".TEST_CASE

local pcall, error, type, table = pcall, error, type, table
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

it("ping command", function()
  local msg
  stream:on_command(function(_, cmd) msg = CMD(cmd) end)
  command:ping(PASS)
  assert_equal("PING\r\n", msg)
end)

it("echo command", function()
  local msg
  stream:on_command(function(_, cmd) msg = CMD(cmd) end)
  command:echo("hello", PASS)
  local res = table.concat{
    "*2\r\n",
      "$4\r\n", "ECHO\r\n",
      "$5\r\n", "hello\r\n",
  }
  assert_equal(res, msg)
end)

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