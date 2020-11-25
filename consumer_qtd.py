import redis
import json

r = redis.Redis()
# lendo do sensor apenas o último dado
#print(r.xread({"sensores": "$"}, count=1, block=50000))

# https://redis-py.readthedocs.io/en/stable/_modules/redis/client.html
# Streams commands

print(r.xinfo_stream("sensores"))
first_id = r.xinfo_stream("sensores")["first-entry"][0]
last_id = r.xinfo_stream("sensores")["last-generated-id"]
# janela por tempo
# 1606268179702 => contém milissegundos
# 1606268841    => geralmente em segundos

# Janela deslizante em quantidade
print("--------- Janela deslizante em Quantidade ------------")
first = int(first_id.decode("utf-8").split("-")[0])
max = int(last_id.decode("utf-8").split("-")[0])

count = 0
next_id = first
# print(r.xrange("sensores", min=first_id, count=2))
while(True):
    range = r.xrange("sensores", min=next_id, count=2)
    if len(range) == 0:
        break

    count += len(range)
    next_id = range[-1][0].decode("utf-8")
    next_id = next_id.split("-")[0]
    next_id = next_id + "-1"

print("Total: %s" % count)