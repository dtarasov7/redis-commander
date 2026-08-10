local namespace = ARGV[1]
local first_index = tonumber(ARGV[2])
local count = tonumber(ARGV[3])

if not namespace or not first_index or not count then
    return redis.error_reply("expected namespace, first_index and count")
end

local type_names = {"string", "hash", "list", "set", "zset", "stream"}

for offset = 0, count - 1 do
    local index = first_index + offset
    local type_index = ((index - 1) % #type_names) + 1
    local type_name = type_names[type_index]
    local key = string.format("%s:%s:%05d", namespace, type_name, index)
    local value = string.format("value-%05d", index)

    if type_name == "string" then
        redis.call("SET", key, value)
    elseif type_name == "hash" then
        redis.call("HSET", key, "id", index, "value", value, "group", index % 100)
    elseif type_name == "list" then
        redis.call("RPUSH", key, value .. "-1", value .. "-2", value .. "-3")
    elseif type_name == "set" then
        redis.call("SADD", key, value .. "-a", value .. "-b", value .. "-c")
    elseif type_name == "zset" then
        redis.call("ZADD", key, 1, value .. "-low", 2, value .. "-high")
    else
        redis.call("XADD", key, "*", "id", index, "value", value)
    end
end

if first_index == 1 then
    for index, expected_type in ipairs(type_names) do
        local key = string.format("%s:%s:%05d", namespace, expected_type, index)
        local actual_type = redis.call("TYPE", key).ok
        if actual_type ~= expected_type then
            return redis.error_reply(
                string.format("%s has type %s, expected %s", key, actual_type, expected_type)
            )
        end
    end
end

return count
