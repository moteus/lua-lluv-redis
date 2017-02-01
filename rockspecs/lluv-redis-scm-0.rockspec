package = "lluv-redis"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-lluv-redis/archive/master.zip",
  dir = "lua-lluv-redis-master",
}

description = {
  summary    = "Implement Redis client sockets for lluv library.",
  homepage   = "https://github.com/moteus/lua-lluv-redis",
  license    = "MIT/X11",
  maintainer = "Alexey Melnichuk",
  detailed   = [[
  ]],
}

dependencies = {
  "lua >= 5.1, < 5.4",
  "lluv > 0.1.1",
  "eventemitter",
}

build = {
  copy_directories = {'examples', 'test'},

  type = "builtin",

  modules = {
    ["lluv.redis"           ] = "src/lua/lluv/redis.lua",
    ["lluv.redis.stream"    ] = "src/lua/lluv/redis/stream.lua",
    ["lluv.redis.commander" ] = "src/lua/lluv/redis/commander.lua",
  }
}
