import xml.sax
import requests
import json
import eventlet
from eventlet.green import urllib
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
import time
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from google import search

alftoken='Bgi0FHKWwO_Or_TC4OXSthRauj4a'

def google_search(q, stop):
	s=search(q+' stubhub events', stop=stop)
	event_list=[url[url.find('/event/')+7:-1] for url in s if 'stubhub.com' and '/event/' in url]
	event_list=[i for i in event_list if i.isdigit()]
	return event_list

		
def api_events(eventId):
	time.sleep(6)
	header={'Authorization': 'Bearer %s' % alftoken}
	url='https://api.stubhub.com/catalog/events/v2/%s' % (eventId)
	print 'url', url
	u=requests.get(url, headers=header)
	xml_soup = bs(u.text, 'xml')
	try:
		eventName=xml_soup.primaryAct.string
	except:
		eventName=xml_soup.description.string
	eventId=int(xml_soup.id.string)
	try:
		zipCode=xml_soup.zipCode.string
	except:
		zipCode=''
	try:
		city=xml_soup.city.string
	except:
		city=''
	try:
		state=xml_soup.state.string
	except:
		state=''
	try:
		address=xml_soup.address1.string
	except:
		address=''
	fmtUTC='%Y-%m-%dT%H:%M:%SZ'
	fmtLocal='%Y-%m-%dT%H:%M:%S-%f:00'
	timeZone=xml_soup.timezone.string
	tsLocal=xml_soup.eventDateLocal.string
	tsUTC=xml_soup.eventDateUTC.string
	timeLocal=datetime.strptime(tsLocal, fmtLocal) #.strftime('%Y-%m-%dT%H:%M:%S')
	timeUTC=datetime.strptime(tsUTC, fmtUTC)
	try:
		venueName=xml_soup.venue.find('name').string
	except:
		venueName=''
	try:
		category=xml_soup.primaryCategory.find('name').string
	except:
		category=''
	return eventId, eventName, zipCode, address, city, state, timeZone, timeLocal, timeUTC, venueName, category



def api_inventory(eventId, page=0):
	rows=100
	header={'Authorization': 'Bearer %s' % alftoken}
	payload={'eventid': eventId,
			'start': page,
			'rows': rows}
	url='https://api.stubhub.com/search/inventory/v1'
	try:
		u = requests.get(url , params=payload, headers=header)
		print '1-get request success', eventId
	except: 
		success='failed'
		print '1-event', eventId , 'failed'
		return	
	if u.content=='':
		data=''
		success='empty event'
		return data, success, eventId
	else:
		data=json.loads(u.content)
		dt=datetime.now()
		if len(str(dt.hour))==1:
			hr1='0'
		else:
			hr1=''
		if len(str(dt.minute))==1:
			min1='0'
		else:
			min1=''
		if len(str(dt.second))==1:
			sec1='0'
		else:
			sec1=''
		data['request_time']=str(dt.year)+str(dt.month)+str(dt.day)+hr1+str(dt.hour)+min1+str(dt.minute)+sec1+str(dt.second)
		success='success'

		parse_data(data)
		return data, success, eventId
		
def pull_data(event_list):
	pool = eventlet.GreenPool()
	k=0
	start_hour=datetime.now().strftime('%y%m%d%H')
	f=open('output/json%s.txt' % start_hour, 'w')
	while True:	
		if start_hour != datetime.now().strftime('%y%m%d%H'):
			f.close()
			start_hour=datetime.now().strftime('%y%m%d%H')
			f=open('output/json%s.txt' % start_hour, 'w')
		for data, success, eventId in pool.imap(api_inventory, [i for i in event_list]):
			print success, 'iteration', k, 'at', datetime.now().strftime('%y%m%d %H%M%S'), 'to file', 'output/json%s.txt' % start_hour, eventId
			if data != '':
				json.dump(data, f)
				f.write(' ||| ')
			else:
				continue	
		k=k+1
		time.sleep(30)
	f.close()	


def create_schema():
	clear_schema()
	db=sqlite3.connect('events.sqlite3')
	c=db.cursor()
	events="""create table events 
				(eventId bigint primary key,
				eventName text,
				zipCode text,
				address text,
				city text,
				state text,
				timeZone text,
				timeLocal timestamp,
				timeUTC timestamp,
				venueName text,
				category text);"""
	c.execute(events)
	print 'events created'

	event_catalog="""create table event_catalog 
				(eventId bigint,
					rows bigint,
					totalListings bigint,
					maxQuantity bigint,
					zone_stats CHARACTER(20), 
					pricingSummary CHARACTER(20), 
					totalTickets integer,
					minQuantity integer,
					request_time bigint);"""
	c.execute(event_catalog)
	print 'event_catalog created'

	listings="""create table listings 
				(listingId bigint, 
				deliveryFee float, 
				seatNumbers int, 
				row int, 
				totalCost float, 
				serviceFee float, 
				zoneId text, 
				score float, 
				ticketSplit int, 
				sectionId text, 	
				sellerOwnInd bigint, 
				ticketClass text, 
				splitOption text, 
				zoneName text, 
				sectionName text, 
				dirtyTicketInd boolean, 
				sellerSectionName text, 
				quantity bigint,
				faceValueCurrency text,
				faceValueAmount float, 
				currentPriceCurrency text,
				currentPriceCurrencyAmount float,
				eventId bigint,
				request_time bigint);"""
	c.execute(listings)
	print 'listings created'

	db.commit()
	db.close()
	db=sqlite3.connect('events.sqlite3')
	c=db.cursor()
	c.execute("""select name from sqlite_master where type='table';""")
	r=c.fetchall()
	print 'new schema', r

def clear_schema():		
	db=sqlite3.connect('events.sqlite3')
	c=db.cursor()
	c.execute("""select name from sqlite_master where type='table';""")
	r=c.fetchall()
	df=pd.DataFrame(r, columns=[i[0] for i in c.description])
	c.close()
	for i in df['name']:
		q="""drop table %s;""" %i
		db=sqlite3.connect('events.sqlite3')
		c=db.cursor()
		c.execute(q)
		c.close()
	


def load_metadata(event_list):
	#event_list=[9384912, 9445834, 9408007, 9376053, 9376054, 9376056, 9376057, 9376059, 9449920, 9392368, 9435512, 9370892, 9298555, 9341424, 9370473]
	for event in event_list:
		eventId, eventName, zipCode, address, city, state, timeZone, timeLocal, timeUTC, venueName, category=api_events(event)
		db=sqlite3.connect('events.sqlite3')
		c=db.cursor()
		c.execute("""insert into events values(?,?,?,?,?,?,?,?,?,?,?)""", (eventId, eventName, zipCode, address, city, state, timeZone, timeLocal, timeUTC, venueName, category))
		db.commit()
		db.close()
	
					

def parse_data(j):
	if j['totalTickets']==None:
		print '2-No tickets for event', j['eventId']
		return	
	else:
		j['request_time']=int(j['request_time'])
		db=sqlite3.connect('events.sqlite3')
		c=db.cursor()
		c.execute("""pragma table_info(event_catalog)""")
		r=c.fetchall()
		c.close()
		
		event_columns=[col[1] for col in r]
		column_types=[col[2] for col in r]
		vals=tuple([j[val].encode('ascii') if type(j[val])==unicode else j[val] for val in event_columns])
		db=sqlite3.connect('events.sqlite3')
		c=db.cursor()
		c.execute("""insert into event_catalog values(?,?,?,?,?,?,?,?,?)""", vals)
		db.commit()
		db.close()
		
		db=sqlite3.connect('events.sqlite3')
		c=db.cursor()
		c.execute("""pragma table_info(listings)""")
		r=c.fetchall()
		c.close()

		event_columns=[col[1] for col in r]
		e=event_columns
		event_columns.remove('faceValueCurrency')
		event_columns.remove('faceValueAmount')
		event_columns.remove('currentPriceCurrency')
		event_columns.remove('currentPriceCurrencyAmount')
		event_columns.remove('eventId')
		event_columns.remove('request_time')
		
		eventId=j['eventId']
		request_time=j['request_time']

		for l in j['listing']:		
			vals=[l[val] for val in event_columns]
			if 'faceValue' in l.keys() and l['faceValue']!=None:
				vals.append('0')	
				vals.append('0')
			else:
				vals.append('0')	
				vals.append('0')
			vals.append(l['currentPrice']['currency'])
			vals.append(l['currentPrice']['amount'])
			vals.append(eventId)
			vals.append(request_time)
			vals=tuple(vals)
			db=sqlite3.connect('events.sqlite3')
			c=db.cursor()
			c.execute("""insert into listings values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", vals)
			db.commit()
			db.close()
		print '2-data written to db for event', eventId


q="""select a.eventId, 
			b.eventName, 
			b.venueName, 
			a.request_time, 
			min(a.currentPriceCurrencyAmount) as minPrice, 
			max(a.currentPriceCurrencyAmount) as maxPrice 
	from listings a
	left join events b
	on a.eventId=b.eventId
	group by a.eventId, b.eventName, a.request_time, b.venueName;"""
def query(q=q):			
	db=sqlite3.connect('events.sqlite3')
	c=db.cursor()
	c.execute(q)
	r=c.fetchall()
	df=pd.DataFrame(r, columns=[i[0] for i in c.description])
	c.close()
	return df


def main():
	create_schema()
	print 'schema created'
	event_list=google_search('new york', 40)+google_search('austin, tx', 40)+google_search('chicago', 40)+google_search('boston', 40)+google_search('san francisco', 40)+google_search('los angeles', 40)
	event_list=list(set(event_list))
	print 'event list', event_list
	load_metadata(event_list)
	pull_data(event_list)
	

if __name__ == '__main__':
	main(sys.argv[1:])	
