#!/usr/bin/env python
# coding: utf-8
from faker import Faker
import datetime
import numpy
from random import randrange
from tzlocal import get_localzone
import random
import sys
import time
import os.path

faker = Faker()
BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
filename = os.path.join(BASE_DIR,"logFile.log")
f = open(filename, "a")
print(filename)

for i in range(1,100):
	response = ["200","404","500","301"]
	verb=["GET","POST","DELETE","PUT"]
	resources=["/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore","/apps/cart.jsp?appID="]
	ualist = [faker.firefox, faker.chrome, faker.safari, faker.internet_explorer, faker.opera]
	ip = faker.ipv4()
	otime = datetime.datetime.now()
	dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
	timestr = time.strftime("%Y%m%d-%H%M%S")
	dt = otime.strftime('%d/%b/%Y:%H:%M:%S')
	local = get_localzone()
	tz = datetime.datetime.now(local).strftime('%z')
	vrb = numpy.random.choice(verb,p=[0.6,0.1,0.1,0.2])
	uri = numpy.random.choice(resources)
	resp = numpy.random.choice(response,p=[0.9,0.04,0.02,0.04])
	byt = int(random.gauss(50, 5000))
	referer = faker.uri()
	useragent = numpy.random.choice(ualist,p=[0.5,0.3,0.1,0.05,0.05] )()
	name = faker.name()


	sys.stdout.write('%s - "%s" [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip,name,dt,tz,vrb,uri,resp,byt,referer,useragent))
	f.write('%s - "%s" [%s %s] "%s %s HTTP/1.0" %s %s "%s" "%s"\n' % (ip,name,dt,tz,vrb,uri,resp,byt,referer,useragent))




