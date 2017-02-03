local uv    = require "lluv"
local Redis = require "lluv.redis"

local function reconnect(cnn, interval, on_connect, on_disconnect)
  local timer = uv.timer():start(0, interval, function(self)
    self:stop()
    cnn:open()
  end):stop()

  local connected = not cnn:closed()

  cnn:on('close', function(self, event, ...)
    local flag = connected

    connected = false

    if flag then on_disconnect(self, ...) end

    if timer:closed() or timer:closing() then
      return
    end

    timer:again()
  end)

  cnn:on('ready', function(self, event, ...)
    connected = true
    on_connect(self, ...)
  end)

  if not connected then
    cnn:open()
  end

  return timer
end

local cnn = Redis.Connection.new()

local rcnn = reconnect(cnn, 5000, function(cnn)
  print(cnn, '- connected')
end, function(cnn, err)
  print(cnn, string.format('- disconnected (%s)', err and tostring(err) or 'NO ERROR'))
end)

uv.signal():start(uv.SIGINT, function()
  print('Close app')
  rcnn:close()
  cnn:close()
end):unref()

uv.run()