from datetime import datetime, time, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import airflow.operators.sqlite_operator
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.hooks.dbapi_hook import DbApiHook
import sqlite3
import pandas as pd
import os, sys
import os.path

#Default parameter
conn_id = 'sqlite_default'
BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
loggenerator = os.path.join(BASE_DIR, "LogGenerationScript.py")


def createdb():
	dest = SqliteHook(sqlite_conn_id='sqlite2')
	dest_conn = dest.get_conn()
	dest_conn.execute('''CREATE TABLE if not exists ORDERS(OrderId text, DocumentNo text, OrderDate text, CatalogId text)''')
	dest_conn.execute('''CREATE TABLE if not exists CUSTOMERS(DocumentNo text, FullName text, Device text, Counttry real)''')
	dest_conn.execute('''CREATE TABLE if not exists CATALOG(CatalogId text, ProductId text, CUID text)''')
	dest_conn.execute('''CREATE TABLE if not exists PRODUCTS(ProductId text, ProductName text, CUID text)''')
	dest_conn.execute('''CREATE TABLE if not exists LEADS(Id text, CustomerId text)''')
	dest_conn.execute('''CREATE TABLE if not exists LOGS(ClientIp text, UserName text, Time text)''')


def createlog():
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


def updatedb():
	#read extract file
	import re
	import re, datetime
	BASE_DIR = os.path.dirname(os.path.abspath("__file__"))
	extract_file = pd.read_csv(os.path.join(BASE_DIR, "SourceData.txt"))
	
	lead_file_name = os.path.join(BASE_DIR, "Marketing_Lead.xlsx")
	LeadFile = pd.read_excel(lead_file_name)

	log_file_name = os.path.join(BASE_DIR, "logFile.log")
	LogFile = pd.DataFrame(columns=["ClientIP","UserName","Time"])
	with open(log_file_name, "r") as logfile:
		Lines = logfile.readlines()
		Linelist =[]
		for line in Lines:
			line.strip()
			userregex = "|".join([r'(\"[^\d\W]+[^\S][^\d\W]+\")',r'(\"[^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+\")',r'(\"[^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+\")',r'(\"[^\d\W]+\.[^\S][^\d\W]+[^\S][^\d\W]+\")',r'(\"[^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+\.\")',r'(\"[^\d\W]+\.[^\S][^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+\.\")',r'(\"[^\d\W]+\.[^\S][^\d\W]+[^\S][^\d\W]+[^\S][^\d\W]+\")'])
			matchHost = re.search(r'([(\d\.)]+)',line).group(0)
			matchUser = re.search(userregex,line).group(0)
			matchTime = re.search(r'(?P<time>\[.*?\])', line).group(0)
			print(matchHost)
			print(matchUser)
			print(matchTime)
			listitem = (matchHost,matchUser,matchTime)
			Linelist.append(listitem)
		LogfileDF = pd.DataFrame(Linelist,columns=["ClientIP","UserName","Time"])
		print(LogfileDF)

	
	LeadFileDF = LeadFile[["Id","Company_Id"]]
	OrdersDF = extract_file[["OrderId","DocumentNo","OrderDate","CatalogId"]]
	CustomersDF = extract_file[["DocumentNo","FullName","Device","Country"]]
	ProductsDF = extract_file[["ProductId","ProductName","CUID.1"]]
	CatalogDF = extract_file[["CatalogId","ProductId","CUID"]]

	#write update logic for customers - only those that are not already existing, catalog - only those that are new
	dest = SqliteHook(sqlite_conn_id='sqlite2')
	dest_conn = dest.get_conn()
	sqlOrders = 'INSERT OR REPLACE INTO ORDERS(OrderId, DocumentNo, OrderDate, CatalogId) VALUES (?, ?, ?, ?)'
	dataOrders = OrdersDF.values
	dest_conn.executemany(sqlOrders,dataOrders)
	dest_conn.commit()
	
	sqlCustomers = 'INSERT OR REPLACE INTO CUSTOMERS(DocumentNo, FullName, Device, Counttry) VALUES (?, ?, ?, ?)'
	dataCust = CustomersDF.values
	dest_conn.executemany(sqlCustomers,dataCust)
	dest_conn.commit()
	
	sqlProducts = 'INSERT OR REPLACE INTO PRODUCTS(ProductId, ProductName, CUID) VALUES (?, ?, ?)'
	dataProducts = ProductsDF.values
	dest_conn.executemany(sqlProducts,dataProducts)
	dest_conn.commit()
	
	sqlCatalog = 'INSERT OR REPLACE INTO CATALOG(CatalogId, ProductId, CUID) VALUES (?, ?, ?)'
	dataCatalog = CatalogDF.values
	dest_conn.executemany(sqlCatalog,dataCatalog)
	dest_conn.commit()
	
	sqlLogs = 'INSERT OR REPLACE INTO LOGS(ClientIP, UserName, Time) VALUES (?, ?, ?)'
	dataLog = LogfileDF.values
	dest_conn.executemany(sqlLogs,dataLog)
	dest_conn.commit()
	
	sqlLead = 'INSERT OR REPLACE INTO LEADS(Id, CustomerId) VALUES (?, ?)'
	dataLeadFile = LeadFileDF.values
	dest_conn.executemany(sqlLead,dataLeadFile)
	dest_conn.commit()
	

def getdf():
	import os.path
	h = SqliteHook(conn_id)
	df = h.get_pandas_df("SELECT A.DocumentNo, A.FullName, A.Device, A.Country, B.OrderId, B.DocumentNo, B.OrderDate, B.CatalogId,C.CatalogId, C.ProductId, C.CUID, D.ProductId, D.ProductName, D.CUID FROM CUSTOMERS as A inner join ORDERS as B on A.DocumentNo = B.DocumentNo inner join CATALOG as C on C.CatalogId = B.CatalogId inner join PRODUCTS AS D ON C.CUID = D.CUID")
	print(df)
	df.to_csv(os.path.join(BASE_DIR, "SourceData.txt"),index=False)
		
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['arpita.b.saha@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('tutorial', 
	default_args=default_args,
	schedule_interval="@once") 

	
CreateLog = PythonOperator(
	task_id='generate_log',
    python_callable=createlog,
    dag=dag)
	
ExtracttoDF = PythonOperator(
	task_id='sqlite_to_df', 
	python_callable=getdf, 
	dag=dag)
	
LoadTask = PythonOperator(
	task_id='Destinationdb', 
	python_callable=createdb, 
	dag=dag)

UpsertTask = PythonOperator(
	task_id='Destinationdb_Upsert', 
	python_callable=updatedb, 
	dag=dag)


dag.doc_md = __doc__

ExtracttoDF.doc_md = """\
Extract data from source DB
"""

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

[ExtracttoDF,CreateLog]  >> LoadTask >> UpsertTask
