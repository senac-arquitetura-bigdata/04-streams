import redis
import time
import random
from faker import Faker

#r = redis.Redis(host='redis-17852.c15.us-east-1-2.ec2.cloud.redislabs.com', port=17852, db=0, password='E3IENTnwAiFZQhIJMj4nMxSEvjuiJOdu')
r = redis.Redis()

fake = Faker()

while(True):
  output = {"sensor": fake.random_element(elements=('s1', 's2', 's3', 's4')),
            "temp": fake.randomize_nb_elements(number=100, ge=True, min=120)}
  print(output)         
  print(r.xadd("sensores", output))
  time.sleep(random.randint(1, 4))