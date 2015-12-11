#!/usr/bin/python
import sys
import time
import datetime
import random
from pytz import timezone

line_count = int(sys.argv[1])
timestr = time.strftime("%Y%m%d-%H%M%S")

f = open('../source/access_log_'+timestr+'.log','w')
 
# ips  
with open('ips.txt') as ips_file:
	ips = ips_file.read().splitlines()

# referers
with open('referers.txt') as referers_file:
	referers = referers_file.read().splitlines()

# resources
with open('resources.txt') as resources_file:
	resources = resources_file.read().splitlines()

# user agents  
with open('user_agents.txt') as user_agents_file:
	useragents = user_agents_file.read().splitlines()

# codes
with open('codes.txt') as codes_file:
	codes = codes_file.read().splitlines()

# requests
with open('requests.txt') as requests_file:
	requests = requests_file.read().splitlines()

event_time = datetime.datetime(2013,10,10).replace(tzinfo=timezone('UTC'))
 
for i in xrange(0,line_count):
	increment = datetime.timedelta(seconds=random.randint(30,300))
	event_time += increment
	uri = random.choice(resources)
	if uri.find("Store")>0:
		uri += `random.randint(1000,1500)`
	ip = random.choice(ips)
	useragent = random.choice(useragents)
	referer = random.choice(referers)
	code = random.choice(codes)
	request= random.choice(requests)
	f.write('%s - - [%s] "%s %s HTTP/1.0" %s %s "%s" "%s" \n' % (random.choice(ips),event_time.strftime('%d/%b/%Y:%H:%M:%S %z'),request,uri,code,random.randint(2000,5000),referer,useragent))