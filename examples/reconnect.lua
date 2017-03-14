local uv    = require "lluv"
local Redis = require "lluv.redis"

local cnn = Redis.Connection.new{
  reconnect = 5
}

cnn:on('reconnect', function(self)
  print(self, '- connected')
end)

cnn:on('disconnect', function(self, _, err)
  print(self, string.format('- disconnected (%s)', err and tostring(err) or 'NO ERROR'))
end)

uv.signal():start(uv.SIGINT, function()
  print('Close app')
  cnn:close()
end):unref()

cnn:open()

uv.run()