import redis
import json
import time

r = redis.Redis()

#print(r.xinfo_stream("sensores"))
first_id = r.xinfo_stream("sensores2")['first-entry'][0]
last_id = r.xinfo_stream("sensores2")['last-entry'][0]
#print(first_id)
#print(last_id)
# 1606268179702 + 5000
# janela de 5s => 
first = int(first_id.decode("utf-8").split("-")[0])
next = first + 5000
last = int(last_id.decode("utf-8").split("-")[0])
print("Primeiro %s" % first)
print("Próximo %s" % next)
print("Ultimo %s" % last)

count = 0
agora = int(time.time()*1000)
loops = 0
while(True):
    dados = r.xrange("sensores2",min=first,max=next)
    count += len(dados)
    # atualização de janela
    first = next + 1
    next = next + 1000
    # critério de parada
    if first > agora:
        break

    # redução de janela
    agora = int(time.time()*1000)
    if next > agora:
        next = agora
    loops += 1

print(loops)
print(count)