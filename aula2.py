import redis
import json
import time

r = redis.Redis()

last_id = r.xinfo_stream("sensores-consolidado")['last-entry'][0]
print(r.xinfo_stream("sensores-consolidado"))
print(r.xrange("sensores-consolidado"))

#print(r.xinfo_stream("sensores"))
first_id = r.xinfo_stream("sensores2")['first-entry'][0]
last_id = r.xinfo_stream("sensores2")['last-entry'][0]
#print(first_id)
#print(last_id)
# 1606268179702 + 5000
# janela de 5s => 
first = int(first_id.decode("utf-8").split("-")[0])
print("Primeiro %s" % first)

count = 0
loops = 0
next_id = first
sensores = {'soma_s1': 0, 'soma_s2': 0, 'soma_s3': 0, 'soma_s4': 0, 
            'qtd_s1': 0, 'qtd_s2': 0, 'qtd_s3': 0, 'qtd_s4': 0}

while(True):
    dados = r.xrange("sensores2", min=next_id, count=2)
    if len(dados) == 0:
        break
    count += len(dados)

    next_id = dados[-1][0].decode("utf-8")
    next_id = next_id.split("-")[0]
    next_id = next_id + "-1"

    loops += 1

    for item in dados:
        sensor = item[1][b'sensor'].decode("utf-8")
        temp = int(item[1][b'temp'])
        sensores["soma_" + sensor] += temp
        sensores["qtd_" + sensor] += 1

print("Média sensor s2: %s" % (sensores['soma_s2']/sensores['qtd_s2']) )
print(loops)
print(count)
sensores['ultimo_timestamp'] = next_id
print(sensores)

r.xadd("sensores-consolidado", sensores)