package.path = "..\\src\\lua\\?.lua;" .. package.path

pcall(require, "luacov")

local uv             = require "lluv"
local redis          = require "lluv.redis"

local utils          = require "utils"
local TEST_CASE      = require "lunit".TEST_CASE

local pcall, error, type, table, ipairs = pcall, error, type, table, ipairs
local RUN = utils.RUN
local IT, CMD, PASS = utils.IT, utils.CMD, utils.PASS
local nreturn, is_equal = utils.nreturn, utils.is_equal

local ENABLE = true

local _ENV = TEST_CASE'redis lluv client' if ENABLE then

local it = IT(_ENV or _M)

local client

function setup()
  client = redis.Connection.new()
end

function teardown()
  client:close()
  uv.run()
end

it("should connect", function()
  client:open(function(self, err)
    assert_equal(client, self)
    assert_nil(err)

    uv.stop()
  end)
  uv.run()
end)

it("should ping", function()
  client:open(function(self, err)

    self:ping(function(self, err, data)
      assert_equal(self, client)
      assert_nil(err)
      assert_equal("PONG", data)

      uv.stop()
    end)
  end)
  uv.run()
end)

end

RUN()
