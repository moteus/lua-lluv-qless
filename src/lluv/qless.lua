local QLessClient = require "qless.client"

local QLess = {
  new = QLessClient.new;
  Client = QLessClient;
}

return QLess