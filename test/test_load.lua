local redis = require "lluv.redis"

print("------------------------------------")
print("Module    name: " .. redis._NAME);
print("Module version: " .. redis._VERSION);
print("Lua    version: " .. (_G.jit and _G.jit.version or _G._VERSION))
print("------------------------------------")
print("")

