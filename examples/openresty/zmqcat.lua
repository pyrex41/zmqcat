-- resty/zmqcat.lua — OpenResty client for a local zmqcat sidecar.
--
--     local zmqcat = require "resty.zmqcat"
--     local c, err = zmqcat.connect()
--     c:put("jobs", '{"run":true}')
--     local msg, err = c:take("jobs")
--
-- Default socket: unix:/tmp/zmqcat-<uid>.sock  (set ZMQCAT_LISTEN)

local cjson = require "cjson.safe"

local _M = { _VERSION = "0.1.0" }
local mt = { __index = _M }

local MAGIC = "ZMQC"

local function be32(n)
  local b1 = math.floor(n / 16777216) % 256
  local b2 = math.floor(n / 65536) % 256
  local b3 = math.floor(n / 256) % 256
  local b4 = n % 256
  return string.char(b1, b2, b3, b4)
end

local function un_be32(s)
  local b1, b2, b3, b4 = s:byte(1, 4)
  return b1 * 16777216 + b2 * 65536 + b3 * 256 + b4
end

local function default_listen()
  local env = os.getenv("ZMQCAT_LISTEN")
  if env and env ~= "" then
    return env
  end
  -- ngx.worker.pid is not uid; unix path is owned by the serve process user.
  return "unix:/tmp/zmqcat-" .. (os.getenv("UID") or "0") .. ".sock"
end

local function parse(listen)
  listen = listen or default_listen()
  if listen:sub(1, 7) == "unix://" then
    return "unix:" .. listen:sub(8)
  end
  if listen:sub(1, 6) == "tcp://" then
    return listen:sub(7)
  end
  if listen:sub(1, 1) == "/" then
    return "unix:" .. listen
  end
  if listen:sub(1, 5) == "unix:" then
    return listen
  end
  return listen
end

local function write_frame(sock, obj)
  local raw = cjson.encode(obj)
  if not raw then
    return nil, "encode"
  end
  return sock:send(MAGIC .. be32(#raw) .. raw)
end

local function read_frame(sock)
  local hdr, err = sock:receive(8)
  if not hdr then
    return nil, err
  end
  if hdr:sub(1, 4) ~= MAGIC then
    return nil, "bad magic"
  end
  local n = un_be32(hdr:sub(5, 8))
  if n <= 0 or n > 2 * 1024 * 1024 then
    return nil, "bad length"
  end
  local raw, err2 = sock:receive(n)
  if not raw then
    return nil, err2
  end
  local obj, jerr = cjson.decode(raw)
  if not obj then
    return nil, jerr
  end
  return obj
end

function _M.connect(listen, name)
  local sock = ngx.socket.tcp()
  sock:settimeouts(1000, 60000, 60000)
  local addr = parse(listen)
  local ok, err = sock:connect(addr)
  if not ok then
    return nil, err
  end
  -- Correlation ids double as mailbox message ids, so they must be unique
  -- across clients: a per-connection counter alone would let two workers
  -- deduplicate each other's requests.
  local prefix = string.format("%08x%08x", math.random(0, 0xffffffff), ngx.now() * 1000 % 0xffffffff)
  local self = setmetatable({ sock = sock, name = name or "openresty", prefix = prefix, seq = 0 }, mt)
  local f, herr = self:hello(self.name)
  if not f then
    sock:close()
    return nil, herr
  end
  return self
end

function _M:hello(name)
  return self:rpc({ op = "hello", from = name or self.name })
end

function _M:put(mailbox, text)
  return self:rpc({ op = "put", name = mailbox, from = self.name, text = text or "" })
end

function _M:take(mailbox)
  return self:rpc({ op = "take", name = mailbox, from = self.name })
end

function _M:pub(topic, text)
  return self:rpc({ op = "pub", name = topic, from = self.name, text = text or "" })
end

function _M:sub(prefix)
  return self:rpc({ op = "sub", name = prefix or "", from = self.name })
end

function _M:ping()
  return self:rpc({ op = "ping" })
end

function _M:ready(service)
  return self:rpc({ op = "ready", name = service, from = self.name })
end

function _M:next_id()
  self.seq = self.seq + 1
  return self.prefix .. "-" .. self.seq
end

-- req requires a correlation id; the hub rejects the frame without one.
function _M:req(service, text, id)
  return self:rpc({ op = "req", name = service, from = self.name, text = text or "", id = id or self:next_id() })
end

function _M:rep(id, text, name)
  return self:rpc({ op = "rep", id = id, name = name or "", from = self.name, text = text or "" })
end

function _M:reserve(mailbox, lease)
  return self:rpc({ op = "reserve", name = mailbox, from = self.name, lease = lease or 60 })
end

function _M:ack(delivery)
  return self:rpc({ op = "ack", delivery = delivery })
end

function _M:nack(delivery)
  return self:rpc({ op = "nack", delivery = delivery })
end

function _M:rpc(obj)
  local ok, err = write_frame(self.sock, obj)
  if not ok then
    return nil, err
  end
  local f, rerr = self:recv()
  if not f then
    return nil, rerr
  end
  if f.op == "err" then
    return nil, f.error or "zmqcat error"
  end
  return f
end

function _M:recv()
  while true do
    local f, err = read_frame(self.sock)
    if not f then
      return nil, err
    end
    if f.op == "ping" then
      write_frame(self.sock, { op = "pong", id = f.id or "" })
    else
      return f
    end
  end
end

function _M:close()
  if self.sock then
    self.sock:close()
  end
end

return _M
