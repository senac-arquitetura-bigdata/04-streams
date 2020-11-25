import redis
import json

#r = redis.Redis(host='redis-17852.c15.us-east-1-2.ec2.cloud.redislabs.com', port=17852, db=0, password='E3IENTnwAiFZQhIJMj4nMxSEvjuiJOdu')
r = redis.Redis()
# lendo do sensor apenas o último dado

# https://redis-py.readthedocs.io/en/stable/_modules/redis/client.html
# Streams commands

print(r.xinfo_stream("sensores"))
first_id = r.xinfo_stream("sensores")["first-entry"][0]
last_id = r.xinfo_stream("sensores")["last-generated-id"]
# janela por tempo
# 1606268179702 => contém milissegundos
# 1606268841    => geralmente em segundos

#########  Janela deslizante em tempo #########
print("--------- Janela deslizante em Tempo ------------")
first = int(first_id.decode("utf-8").split("-")[0])
next = first + 5000 # janela de 5 segundos

max = int(last_id.decode("utf-8").split("-")[0])

print("Primeiro : %s " % first)
print("Próximo : %s " % next)
print("Último : %s " % max)
count = 0
while(True):
    range = r.xrange("sensores", min=first, max=next)
    count += len(range)
    first = next + 1
    next = next + 5000
    if next > max:
        next = max
    if first > max:
        break

print("Total: %s" % count)