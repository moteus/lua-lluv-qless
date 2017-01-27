local QLessClient = require "lluv.qless.client"

local QLess = {
  new = QLessClient.new;
  Client = QLessClient;
}

return QLess