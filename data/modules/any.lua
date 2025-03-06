local nk = require("nakama")
local du = require("debug_utils")

local function before_any(ctx, payload) 
  print(du.print_r(payload))
  --[[
    invokeMS, 通过GRPC进行远端接口调用
     参数：
    cid: string 方法名称
    name: string 服务名称
    header: map[string]string 
    query: map[string][]string
    context: map[string]string
    content: string 内容
  ]]--
  print(du.print_r(nk.get_peer().invokeMS("chat", "say_hello", {header = "11", htest="vvv"}, {query = {"vvvv", "cccc"}},{u8="u9"}, "gogogogo")))
  
  --[[
    sendMS, 通过GRPC进行远端流方法调用
    参数：
    cid: string 方法名称
    name: string 服务名称
    header: map[string]string 
    query: map[string][]string
    context: map[string]string
    content: string 内容
  ]]--
  print(du.print_r(nk.get_peer().sendMS("vuuu", "say_hello", {header = "11", htest="vvv"}, {query = {"vvvv", "cccc"}},{u8="u9"}, "hhh000000")))
  return payload
end

local function after_any(ctx, payload, vk)
  -- print(du.print_r(payload))
  -- print(du.print_r(vk))
  return payload
end

-- [[ 注册any方法调用前 ]] --
nk.register_req_before(before_any, "any")
-- [[ 注册any方法调用后 ]] --
nk.register_req_after(after_any, "any")