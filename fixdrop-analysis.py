#!/usr/bin/python

# In a perfect world...
#import pyjion; pyjion.enable()

from __future__ import annotations
from multiprocessing import Process, cpu_count
from typing import Any
from pymongo import MongoClient
from pymemcache.client.base import Client

import os, sys, re, subprocess, signal
import gzip, json, math
import zmq, random, time, datetime
import base64, psutil
import pytz, pathlib, shlex

db_Host = 'mongodb.example.com'
db_User = 'admin'
db_Pass = '' # Leave blank for user input

assert sys.version_info >= (3, 10)
(filelist,selected) = ({0:False},0)
(client,db,IO,err,fp) = (False,False,0,0,None)
threads = cpu_count()

def handler(num, _) -> int | None:
	if num == signal.SIGINT:
		print('\n')
		sys.exit(1)
	return 1

signames = ['SIGINT','SIGHUP','SIGQUIT','SIGUSR1']
sigmap = dict((getattr(signal, k), k) for k in signames)
for name in signames:
	signal.signal(getattr(signal, name), handler)

class debug:
	_start = None
	f = None

	@staticmethod
	def str_repeat(string: str, times: int) -> str:
		return (string * (int(times/len(string))+1))[:times] if times > 0 else ''

	@staticmethod
	def Timelog(data: str, state: bool=None):
		if debug.f == None:
			debug.f = open('/tmp/debug.timelog.txt', 'w')
		if state == True:
			debug._start = time.time()
			t = str(datetime.datetime.now())
			debug.f.write('['+t+debug.str_repeat(' ', 26-len(t))+'] PID:'+str(os.getpid())+' DATA:'+data+'\n')
		elif state == None:
			t = str(datetime.datetime.now())
			debug.f.write('['+t+debug.str_repeat(' ', 26-len(t))+'] \t\t DATA:'+data+'\n')
		else:
			t = str(datetime.datetime.now())
			e = str((time.time() - debug._start))
			debug.f.write('['+t+debug.str_repeat(' ', 26-len(t))+'] Elapsed:'+e+' DATA:'+data+'\n')
			debug._start = None

def number_format(num: int | float, dec: int) -> str:
	return re.sub(r'^(\d+\.\d{,'+str(dec)+'})\d*$',r'\1',str(num))

def fsize(filePath: int | str) -> str:
	if isinstance(filePath, int):
		bytes = filePath
	else:
		bytes = os.stat(filePath).st_size

	if bytes > (1024*1024*1024):
		return number_format(bytes / (1024*1024*1024), 2) + ' GB'
	elif bytes > (1024*1024):
		return number_format(bytes / (1024*1024), 2) + ' MB'
	elif bytes > 1024:
		return number_format(bytes / 1024, 2) + ' KB'
	else:
		return str(bytes) + ' bytes'

def forTable(table: str, utf8_in: str) -> bool:
	(i,t,v) = ('INSERT INTO ',table,'VALUES')
	if utf8_in[0:len(i)] != i:
		return False
	if utf8_in[len(i)+1:len(i+t)+1] != table:
		return False
	if utf8_in[len(i+t)+3:len(i+t+v)+3] != v:
		return False
	return True

def format_timestamp(value: int | float) -> str:
	(millisec,seconds,minutes,hours) = (0,0,0,0)
	millisec = math.floor(value/1000)
	seconds = math.floor(millisec/1000)
	minutes = math.floor(seconds/60)
	seconds = seconds - minutes * 60
	millisec = millisec - minutes * 60000
	hours = math.floor(minutes/60)
	minutes = minutes - hours * 60
	return str(hours).zfill(2)+':'+str(minutes).zfill(2)+':'+str(round(millisec/1000, 3)).zfill(6)

def bindConflict() -> bool:
	stream = os.popen("netstat -tulpn | egrep ':5559|:5560'")
	output = stream.read()
	if output.find(':5559') == -1 and output.find(':5560') == -1:
		return False
	pprint(color.RED + 'Application already running' + color.END)
	return True

def match(src: int | str, des: int | str) -> str:
	return color.GREEN if src == des else color.RED

def str_repeat(string: str, times: int) -> str:
	return (string * (int(times/len(string))+1))[:times] if int > 0 else ''

def pstats(o: list, n: int | list, br: bool=False, documents: list=False) -> None:
	(TransactTime,OrderID,reading)=('...',0,True)
	try:
		if o[3].FIX['TransactTime'] != False:
			TransactTime = o[3].FIX['TransactTime']
	except (Exception) as e:
		pass
	try:
		if o[3].FIX['OrderID'][9:]:
			OrderID = int(o[3].FIX['OrderID'][9:])
	except (Exception) as e:
		pass
	if isinstance(n, list):
		n, reading = n
	sys.stdout.write('  □ ')
	sys.stdout.write('Trades: ' + (color.BOLD if documents==False else match(o[0].r, documents[0])) + '{:,}'.format(o[0].r) + color.END + ', ')
	sys.stdout.write('Orders: ' + (color.BOLD if documents==False else match(o[1].r, documents[1])) + '{:,}'.format(o[1].r) + color.END + ', ')
	sys.stdout.write('Securities: ' + (color.BOLD if documents==False else match(o[2].r, documents[2])) + '{:,}'.format(o[2].r) + color.END + ', ')
	if OrderID > 0:
		sys.stdout.write('TransactTime: ' + (color.BOLD if documents==False else match(o[3].r, documents[3])) + TransactTime + color.END + ', ')
		sys.stdout.write('OrderID: ' + (color.BOLD if documents==False else match(o[3].r, documents[3])) + '{:,}'.format(OrderID) + color.END + ', ')
	sys.stdout.write('' if n == -1 else 'Reading: ' + (color.BOLD if reading else color.GREEN) + fsize(n) + color.END + ' (IO: ' + color.BOLD + '{:,}'.format(IO) + color.END+'')
	if err > 0:
		sys.stdout.write(', ' + color.RED + 'Error: '+'{:,}'.format(err) + color.END)
	sys.stdout.write(')                ')
	sys.stdout.write('\r' if br == False else '\n')
	sys.stdout.flush()

def bstats(docs: int, n: int, br: bool=False):
	perc = math.ceil((n / docs) * 100)
	sys.stdout.write('  □ ')
	sys.stdout.write('Building order book |')
	sys.stdout.write(str_repeat('█', perc))
	sys.stdout.write(str_repeat('░', 100 - perc))
	sys.stdout.write('| '+number_format((n / docs) * 100, 2)+'%')
	sys.stdout.write('\r' if br == False else '\n')
	sys.stdout.flush()

def run_command(command: str) -> int | None:
	process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
	while True:
		output = process.stdout.readline().strip().decode('utf8', 'strict')
		if output == '' and process.poll() is not None:
			break
		if output:
			pprint(output)
	rc = process.poll()
	return rc

def pprint(*txt) -> None:
	for t in txt:
		sys.stdout.write(str(t))
		sys.stdout.write(' ')
	sys.stdout.write('\n')
	sys.stdout.flush()

def index() -> list:
	(tables,indexes)=(['TRADE', 'FIX_ORDER', 'SECURITY', 'MESSAGE'],[])
	indexes.append([[("LIQUIDITY", 1), ("TRADE_ID", 1)], {"unique":True, "name":"TRADE_ID"}])
	indexes.append([[("ORDER_ID", 1)], {"unique":True, "name":"ORDER_ID"}])
	indexes.append([[("SECURITY_ID", 1)], {"unique":True, "name":"SECURITY_ID"}])
	indexes.append([[("OrderID", 1)], {"unique":True, "name":"OrderID"}])
	return tables,indexes

def tableKeys(table: str) -> list:
	switcher={
		'TRADE':[
			'TRADE_ID','TRADE_TYPE','ORDER_ID','VOLUME',
			'PRICE','TRADE_TIME','LIQUIDITY','SUB_ORDER_ID',
			'ORDER_QTY','CHARGE'
		],
		'FIX_ORDER':[
			'ORDER_ID','CONNECTION_ID','CLIENT_ID','SECURITY_ID',
			'VOLUME','PRICE','ORDER_TYPE','ORDER_SIDE','EXEC_INST',
			'PEG_DIFFERENCE','MAX_FLOOR','TIME_IN_FORCE','EX_DESTINATION',
			'POST_ONLY','ORDER_VISIBLE','EBBO_FLAG','ROUTING_STRATEGY',
			'CLIENT_ORDER_ID','ACCOUNT','ORDER_CAPACITY','EXEC_BROKER',
			'ON_BEHALF_OF_COMP_ID','ON_BEHALF_OF_SUB_ID','NO_MATCH_ID',
			'ACCOUNT_TYPE_ID','USER_ID','PROGRAM_TRADE','JITNEY',
			'ANONYMOUS','REGULATION_ID','BYPASS','PROTECTION',
			'PROTECTION_PRICE_IMPR','CL_ID','CROSS_TYPE','ORD_TIME',
			'SHORT_MARKING_EXEMPT','BOOK_CODE','BOOK_MATCH','STP_OPT',
			'STP_KEY'
		],
		'SECURITY':[
			'SECURITY_ID','SYMBOL','SECURITY_DESC','SECURITY_TYPE',
			'ISIN','OTHER_ID','TRADING_STATE','CURRENCY','MIN_SIZE',
			'DATE_LISTED','PERMANENTLY_DELISTED','CLASS_ID','MARKET_ID',
			'SHORTABLE','ME_SERVER_ID','HALTED','MARKET_INDEX_ID',
			'LISTED_COMPANY_ID','DIVIDEND_IND','ODD_LOT','SETTLEMENT_TERM',
			'CHARGE_TIER','TICK_SZ_RULE','PRICE_THRESHOLD'
		],
		'MESSAGE':[
			'CONNECTION_ID', 'SEQ_NO', 'MSG_BODY', 'MSG_TYPE', 'MSG_TIME'
		]
	}
	return switcher.get(table, [])

class color:
	PURPLE = '\033[95m'
	CYAN = '\033[96m'
	DARKCYAN = '\033[36m'
	BLUE = '\033[94m'
	GREEN = '\033[92m'
	YELLOW = '\033[93m'
	RED = '\033[91m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'
	END = '\033[0m'

class JsonSerde(object):
	def serialize(self, key, value):
		if isinstance(value, str):
			return value.encode('utf-8'), 1
		return json.dumps(value).encode('utf-8'), 2

	def deserialize(self, key, value, flags):
	   if flags == 1:
		   return value.decode('utf-8')
	   if flags == 2:
		   return json.loads(value.decode('utf-8'))
	   raise Exception("Unknown serialization format")

class Db:
	__instance = None
	@staticmethod
	def getInstance():
		if Db.__instance == None:
			Db()
		return Db.__instance.mData
	def __init__(self):
		if Db.__instance != None:
			raise Exception("This class is a singleton!")
		else:
			if len(str(db_User)) > 0:
				Db.__instance = MongoClient("mongodb://"+db_User+":"+db_Pass+"@"+db_Host+":27017/")
			else:
				Db.__instance = MongoClient("mongodb://"+db_Host+":27017/")
	@staticmethod
	def disconnect():
		if Db.__instance != None:
			Db.__instance.close()
			Db.__instance = None

class M:
	__instance = None
	@staticmethod
	def getInstance():
		if M.__instance == None:
			M()
		return M.__instance
	def __init__(self):
		if M.__instance != None:
			raise Exception("This class is a singleton!")
		else:
			M.__instance = Client('localhost', serde=JsonSerde())
	@staticmethod
	def disconnect():
		if M.__instance != None:
			M.__instance.close()
			M.__instance = None

class getQuery:
	@staticmethod
	def new_order_extended() -> list:
		query = {}
		query["$and"] = [
			{
				u"TransactTime": {
					u"$ne": False
				}
			},
			{
				u"TransactTime": {
					u"$gt": 0.0
				}
			},
			{
				u"CloseTime": False
			},
			{
				u"OrigClOrdID": False
			}
		]
		projection = {}
		projection["TransactTime"] = 1.0
		projection["ExecuteTime"] = 1.0
		projection["CloseTime"] = 1.0
		projection["ClOrdID"] = 1.0
		projection["Orders"] = 1.0
		return [query, projection]

	def order_attached(ClOrdID: str) -> list:
		query = {}
		query["ExecType"] = False
		query["Orders.OrigClOrdID"] = u""+ClOrdID
		projection = {}
		projection["TransactTime"] = 1.0
		projection["ExecuteTime"] = 1.0
		projection["CloseTime"] = 1.0
		projection["ClOrdID"] = 1.0
		projection["Orders"] = 1.0
		return [query, projection]

class FIX44:
	@staticmethod
	def order_side(val: str) -> str:
		switcher={
			'1':'Buy',
			'2':'Sell',
			'5':'Short Sell',
			'6':'Short Sell Exempt',
			'8':'Cross',
			'9':'Cross Short',
			'A':'Cross Short Exempt'
		}
		return switcher.get(val, val)

	@staticmethod
	def order_type(val: str) -> str:
		switcher={
			'1':'Market',
			'2':'Limit',
			'3':'Stop',
			'4':'Stop limit',
			'5':'Market on close',
			'6':'With or without',
			'7':'Limit or better',
			'8':'Limit with or without',
			'9':'On basis',
			'A':'On close',
			'B':'Limit on close',
			'C':'Forex - Market',
			'D':'Previously quoted',
			'E':'Previously indicated',
			'F':'Forex - Limit',
			'G':'Forex - Swap',
			'H':'Forex - Previously Quoted',
			'I':'Funari',
			'J':'Market If Touched (MIT)',
			'K':'Market with Leftover as Limit',
			'L':'Previous Fund Valuation Point',
			'M':'Next Fund Valuation Point',
			'P':'Pegged'
		}
		return switcher.get(val, val)
	
	@staticmethod
	def exec_type(val: str) -> str:
		switcher={
			'0':'New',
			'1':'Partial fill',
			'2':'Fill',
			'3':'Done for day',
			'4':'Canceled',
			'5':'Replaced',
			'6':'Pending Cancel',
			'7':'Stopped',
			'8':'Rejected',
			'9':'Suspended',
			'A':'Pending New',
			'B':'Calculated',
			'C':'Expired',
			'D':'Restated',
			'E':'Pending Replace',
			'F':'Trade',
			'G':'Trade Correct',
			'H':'Trade Cancel',
			'I':'Order Status'
		}
		return switcher.get(val, val)

class Worker:
	@staticmethod
	def start(process: str, parent: int, filter: str | int=0) -> None:
		match process:
			case 'com':
				process = 'device'
				p = Forwarder(process, parent, filter)
				getattr(p, process)()
				raise SystemExit

			case 'io':
				process = 'write'
				p = Forwarder(process, parent, filter)
				getattr(p, process)()
				raise SystemExit

			case 'orders_trades':
				process = 'subscriber'
				p = Forwarder(process, parent, filter)
				getattr(p, process)()
				Worker._parse_orders(p, parent)
				raise SystemExit

			case _:
				raise SystemExit

	@staticmethod
	def fork(process: str) -> Forwarder:
		# Start worker threads
		for i in range(1,threads+1):
			Process(target=Worker.start, args=(process, os.getpid(), i)).start()

		# Start the I/O thread. Needed for the workers
		Process(target=Worker.start, args=('io', os.getpid(), 0)).start()

		# Open communication with Forwarder ("com" thread)
		workers = Forwarder('subscriber', os.getpid(), os.getpid())
		workers.subscriber()
		workers.io.server()
		return workers

	@staticmethod
	def _parse_orders(p: Forwarder, parent: int) -> None:
		(o,v,n,idx,_idx) = ([
			Data('TRADE', tableKeys('TRADE')),
			Data('FIX_ORDER', tableKeys('FIX_ORDER')),
			Data('SECURITY', tableKeys('SECURITY')),
			Data('MESSAGE', tableKeys('MESSAGE'))
		],'',0,0,0)
		def _index(_n: int, _idx: int) -> list:
			if _n!=_idx:
				o[_idx].inc(1)
			return [_n,_n]

		while True:
			string = p.get()
			if len(string) == 0:
				time.sleep(1)
				continue

			try:
				utf8_in = base64.b64decode(string).decode()
			except (Exception) as e:
				pprint(color.RED+e+color.END)
				if len(string) > 50:
					pprint('SQL data: ' + color.BOLD + string[0:50] + color.END + '...')
					pprint('Received: ' + color.BOLD + p.recv[0:50] + color.END + '...')
				else:
					pprint('SQL data: ' + color.BOLD + string + color.END)
					pprint('Received: ' + color.BOLD + p.recv + color.END)
				utf8_in = ''

			(_VALUES,table) = ('','')
			for i in range(len(o)):
				if forTable(o[i].table, utf8_in) == True:
					idx, _idx = _index(i, _idx)
					table = o[i].table
					break
			if table == '':
				continue

			VALUES = utf8_in[utf8_in.find('(')+1:len(utf8_in)-2]
			while len(VALUES) > 0:
				if _VALUES == VALUES:
					pprint(color.RED + 'Error, unchanged: ' + VALUES[0:150] + color.END)
					pprint(color.RED, os.getpid(), o[idx].k, len(o[idx].keys), json.dumps(o[idx].get()), color.END)
					o[idx].reset()
					break
				_VALUES = VALUES
				VALUES = Worker._parse_sql(VALUES, o[idx])
				if len(o[idx].get()) == len(o[idx].keys):
					p.io.send(o[idx].table+" "+json.dumps(o[idx].get()))
					p.io.send(json.dumps({'index':idx, 'table':o[idx].table, 'value':1}), parent)
					o[idx].reset()

	@staticmethod
	def _parse_sql(VALUES: str, data: Data) -> str:
		if VALUES[0:1] == ';':
			VALUES = data.inc(1)
		elif VALUES[0:1] == "(" or VALUES[0:1] == ")":
			data.inc(1)
			VALUES = VALUES[1:len(VALUES)-0]
		elif VALUES[0:1] == "'":
			VALUES = VALUES[1:len(VALUES)-0]
			if VALUES[0:1] == "'":
				v = ''
				VALUES = VALUES[2:len(VALUES)]
			else:
				v = VALUES[0:VALUES.find("'")]
				while VALUES[len(v)+1:len(v)+2] != ',' and VALUES[len(v)+1:len(v)+2] != ')':
					v = VALUES[0:VALUES.find("'", len(v)+1)]
				VALUES = VALUES[len(str(v))+2:len(VALUES)-0]
			VALUES = VALUES if VALUES[0:1] != ',' else VALUES[1:len(VALUES)-0]
			data.set(str(v) if v != 'NULL' else False)
			data.index(1)
		else:
			v = VALUES[0:VALUES.find(')')] if VALUES.find(',') == -1 else VALUES[0:VALUES.find(',')]
			VALUES = VALUES[len(str(v))+1:len(VALUES)-0]
			if v[len(str(v))-1:len(str(v))] == ')':
				v = v[0:len(str(v))-1]
			if str(v).find('.') > 0:
				data.set(float(v))
			elif str(v) == 'NULL':
				data.set(False)
			else:
				data.set(int(v))
			data.index(1)
		return VALUES

class Forwarder:
	(w,recv) = ({},'')
	(p,m,f,pid,io) = (0,'',0,0,False)
	(context,socket) = (False,False)
	def __init__(self, Process: str, Parent: int, Filter: str | int=0) -> None:
		(self.m,self.p,self.f) = (Process,Parent,Filter)
		self.pid = os.getpid()

	def pp(self, *argv) -> str | None:
		if self.m == 'subscriber':
			try:
				argv[0]
			except (Exception) as e:
				block = True
			else:
				block = argv[0]
			try:
				argv[1]
			except (Exception) as e:
				fullText = False
			else:
				fullText = argv[1]
			return getattr(self, 'get')(block, fullText)
		else:
			try:
				argv[0]
			except (Exception) as e:
				buffer = ''
			else:
				buffer = argv[0]
			try:
				argv[1]
			except (Exception) as e:
				t = False
			else:
				t = argv[1]
			return getattr(self, 'send')(buffer, t)

	def device(self) -> None:
		try:
			context = zmq.Context(1)
			# Socket facing clients
			frontend = context.socket(zmq.SUB)
			frontend.bind("tcp://*:5559")
			frontend.setsockopt_string(zmq.SUBSCRIBE, "")
			# Socket facing services
			backend = context.socket(zmq.PUB)
			backend.bind("tcp://*:5560")

			zmq.device(zmq.FORWARDER, frontend, backend)
		except (UnboundLocalError) as e:
			pprint(color.RED + 'Bind error. Bringing down zmq device.' + color.END)
		finally:
			frontend.close()
			backend.close()
			context.term()

	def server(self) -> None:
		port = '5559'
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.PUB)
		self.socket.connect("tcp://localhost:%s" % port)

	def subscriber(self) -> None:
		port = '5560'
		# Socket to talk to server
		self.context = zmq.Context()
		self.socket = self.context.socket(zmq.SUB)
		self.socket.connect("tcp://localhost:%s" % port)
		if self.f == self.p:
			M.getInstance().delete('FP', True)
			self.socket.setsockopt_string(zmq.SUBSCRIBE, 'P')
		else:
			M.getInstance().delete('F'+str(self.f), True)
			self.socket.setsockopt_string(zmq.SUBSCRIBE, str(self.f))

		self.io = Forwarder('server', self.p, 0)
		self.io.server()

	def send(self, buffer: str, t: str | int=None) -> None:
		if self.p == self.pid:
			topic = random.randrange(1,threads+1) if t==None else t
		else:
			if t == self.p:
				topic = 'P'
			else:
				topic = 0 if t==None or t==self.f else t

		try:
			self.w['F'+str(topic)]
		except (Exception) as e:
			self.w['F'+str(topic)] = 0
		t = math.ceil(time.time())
		if t > self.w['F'+str(topic)]:
			q = int(M.getInstance().get('F'+str(topic), 0))
			if q < 250:
				self.w['F'+str(topic)] = t + 3
			else:
				while int(M.getInstance().get('F'+str(topic), 0)) > 250:
					time.sleep(0.15)
		M.getInstance().incr('F'+str(topic), 1, True)
		self.socket.send_string("%s %s" % (str(topic), buffer))
	
	def get(self, block: bool=True, fullText: bool=False) -> str:
		(buffer,string) = ('','')
		if block == True:
			string = self.socket.recv()
		else:
			try:
				string = self.socket.recv(flags=zmq.NOBLOCK)
			except zmq.Again as e:
				pass

		if len(string) > 0:
			string = string.decode("utf-8")
			self.recv = string
			topic, buffer = string.split(' ', 1)
			M.getInstance().decr('F'+str(topic), 1, True)
			if buffer == 'EXIT':
				if self.io!=False and self.pid!=self.p:
					self.io.send(str(self.pid) + ' Goodbye', self.p)
				self.exit()
		if fullText == False:
			return buffer if len(string) else ''
		else:
			return string if len(string) else ''

	def stats(self, o: list, tables: list) -> list:
		global IO, err
		string = 'A'
		while len(string) > 0:
			string = self.get(False)
			if len(string) == 0:
				return o

			r = json.loads(string)
			try:
				if r['IO'] != None:
					IO = IO + int(r['IO'])
			except (Exception) as e:
				pass
			try:
				if r['err'] != None:
					err = err + int(r['err'])
			except (Exception) as e:
				pass

			try:
				if r['index'] != None:
					o[r['index']].r = o[r['index']].r + int(r['value'])
			except (Exception) as e:
				pass

			try:
				if r['OrderID']:
					if int(r['IO']) == 0:
						o[tables.index('MESSAGE')].r = o[tables.index('MESSAGE')].r - 1
					try:
						id = int(r['OrderID'][9:])
					except (Exception) as e:
						id = 0
					if id > 0:
						o[3].FIX['OrderID'] = r['OrderID']
					if r['TransactTime'] != False:
						o[3].FIX['TransactTime'] = r['TransactTime']
			except (Exception) as e:
				pass
		return o

	def write(self) -> None:
		date = filelist[selected].split('.',3)[2].split('-',1)[0]
		fName='/tmp/'+filelist[selected][7:-7]+'--%s.json'
		o = {
			'TRADE':Data('TRADE', [], fName),
			'FIX_ORDER':Data('FIX_ORDER', [], fName),
			'SECURITY':Data('SECURITY', [], fName),
			'MESSAGE':Data('MESSAGE', [])
		}
		tables,indexes = index()
		ids = {}
		for i in range(len(indexes)):
			ids[tables[i]] = []
			for x in range(len(indexes[i][0])):
				ids[tables[i]].append(indexes[i][0][x][0])

		def unix_timestamp(timestring: str) -> float:
			d = datetime.datetime.strptime(timestring, '%Y%m%d-%H:%M:%S.%f' if timestring.find('.')>0 else '%Y%m%d-%H:%M:%S')
			d = pytz.timezone('UTC').localize(d)
			d = d.astimezone(pytz.utc)
			return d.timestamp()

		def query(obj: dict, table: str, r: dict=False) -> dict:
			r = {} if r == False else r.copy()
			if table == 'MESSAGE':
				_FIX = obj['MSG_BODY'].split('|')
				r = {
					'OrderID':False, 'Symbol':False, 'Side':False, 'OrdType':False, 'ExecType':False,
					'TransactTime':False, 'ExecuteTime':False, 'CloseTime':False,
					'ClOrdID':False, 'OrigClOrdID':False, 'ExecBroker':False, 'TraderID':False,
					'Orders':[]
				}
				order = {
					'OrderID':False, 'ClOrdID':False, 'OrigClOrdID':False,
					'Price':False, 'OrderQty':False, 'LeavesQty':False, 'TransactTime':False, 'Text':False
				}
				for i in range(len(_FIX)):
					if _FIX[i].find('=') == -1:
						continue
					k, v = _FIX[i].split('=',1)
					FIX[k] = v
					match k:
						case '37':
							r['OrderID'] = v
							o = M.getInstance().get('Orders.' + r['OrderID'] + '.' + str(self.pid))
							r['Orders'] = [] if o == None else o
						case '40':
							r['OrdType'] = FIX44.order_type(v)
						case '54':
							r['Side'] = FIX44.order_side(v)
						case '55':
							r['Symbol'] = v
						case '11':
							r['ClOrdID'] = v
						case '41':
							r['OrigClOrdID'] = v
						case '76':
							r['ExecBroker'] = v
						case '6751':
							r['TraderID'] = v
						case '58':
							order['Text'] = v
							order['OrderID'] = FIX['37']
							order['ClOrdID'] = FIX['11']
							try:
								order['OrigClOrdID'] = FIX['41']
							except (Exception) as e:
								order['OrigClOrdID'] = False
							if v in ['New Order ACK']:
								r['ClOrdID'] = FIX['11']
								r['OrigClOrdID'] = False
								r['TransactTime'] = unix_timestamp(order['TransactTime'])
								r['ExecType'] = FIX44.exec_type(FIX['150'])
							elif v in ['Client Cancel']:
								r['CloseTime'] = unix_timestamp(order['TransactTime'])
							elif v in ['Order Fill','Partial Fill']:
								r['ExecuteTime'] = unix_timestamp(order['TransactTime'])
						case '150':
							order['ExecType'] = FIX44.exec_type(v)
						case '44':
							order['Price'] = float(v)
						case '38':
							order['OrderQty'] = int(v)
						case '151':
							order['LeavesQty'] = int(v)
						case '32':
							order['LastQty'] = int(v)
						case '60':
							order['TransactTime'] = v
							order['UnixTimestamp'] = unix_timestamp(v)
				r['Orders'].append(order)
				if r['OrderID'] != False:
					M.getInstance().set('Orders.' + r['OrderID'] + '.' + str(self.pid), r['Orders'], 86400)
				obj = r.copy()

			for key in obj:
				if key in ids[table]:
					try:
						if r['_id.'+table]:
							r['_id.'+table] = str(r['_id.'+table])+'-'+str(obj[key])
					except (Exception) as e:
						r['_id.'+table] = obj[key]
				r[key] = obj[key]
			try:
				if r['_id.'+table]:
					v = r['_id.'+table]
					r.pop('_id.'+table, None)
					r['_id'] = v
			except (Exception) as e:
				pass
			return r

		self.subscriber()
		while True:
			string = self.get(True, True)
			if len(string) == 0:
				time.sleep(1)
				continue

			results = {'err':1}
			_, table, buffer = string.split(' ', 2)
			try:
				o[table].data[o[table].r] = json.loads(buffer)
				o[table].write()
			except (Exception) as e:
				pprint('\n' + color.RED + 'Write Error:', e, color.END)
			try:
				(FIX,v) = ({},{})
				r = query(json.loads(buffer), table)
				if table == 'MESSAGE':
					_i = 0
					k = str(r['_id']) + '.' + str(self.pid)
					if M.getInstance().get(k) == None:
						(_i,s) = (1,Db.getInstance()[table+'_'+date].insert_one(r))
						M.getInstance().set(k, True, 86400)
					else:
						v = r.copy()
						v.pop('_id', None)
						for _k in v.copy():
							if _k == 'Orders':
								continue
							if v[_k] == False:
								v.pop(_k, None)
						v = {'$set':v}
						s = Db.getInstance()[table+'_'+date].update_one({'_id':r['_id']}, v)
						M.getInstance().set(k, True, 86400)
					results = {'IO':_i, 'OrderID':FIX['37'], 'TransactTime':FIX['60'] if r['TransactTime']!=False else False}
				else:
					s = Db.getInstance()[table+'_'+date].insert_one(r)
					results = {'IO':1, 'table':table}
			except (Exception, TypeError) as e:
				pprint('\n' + color.RED + 'Database Error:', e, color.END)
				pprint(color.YELLOW + 'Query:', json.dumps(r, indent=4, sort_keys=True), color.END)
		
			self.io.send(json.dumps(results), self.p)

	def exit(self) -> None:
		self.socket.close()
		self.context.term()
		if self.io != False:
			self.socket.close()
			self.context.term()
		if self.p != self.pid:
			raise SystemExit

class Data:
	(table,keys,indexes) = ('',[],'')
	(r,k,v,f) = (0,0,'',False)
	(FIX,data) = ({},{r:{}})

	def __init__(self, table: str, keys: list, fName: str=False) -> None:
		(self.table,self.keys) = (table,keys)
		if fName != False:
			self.f = open(fName % table, 'w')

	def inc(self, x: int) -> str:
		if len(self.get()) > 0:
			pprint(color.RED + 'Uncached: ' + json.dumps(self.get()) + color.END)
		self.r = self.r + x
		(self.k,self.v) = (0,'')
		self.data[self.r] = {}
		return ''

	def set(self, val: any) -> None:
		self.data[self.r][self.keys[self.k]] = val

	def get(self) -> dict:
		return self.data[self.r]

	def reset(self) -> None:
		self.k = 0
		self.data[self.r] = {}

	def write(self) -> None:
		if self.f != False:
			self.f.write(str(json.dumps(self.data[self.r]))+"\n")

	def index(self, x: int) -> None:
		self.k = x if x == 0 else self.k + x

class Build:
	(path,filelist,selected,tables) = ('/dumps',{0:False},0,[])

	def __init__(self) -> None:
		if os.environ.get('extractAll') == None:
			# Start communication thread. We will always need this
			Process(target=Worker.start, args=('com', os.getpid())).start()
		else:
			# We will use the parent com thread
			self.selected = int(os.environ.get('extractAll'))
			selected = self.selected

	def selectFile(self) -> list:
		(i,files) = (0,os.listdir(self.path))
		for item in files:
			if item[0:5] != 'omega':
				continue
			if item[-7:] != '.sql.gz':
				continue

			i = i + 1
			self.filelist[i] = self.path + '/' + item
			pprint(str(i) + '): ' + item + ' (' + fsize(self.path + '/' + item) + ')')

		pprint(color.CYAN+'A): Process all files'+color.END)
		pprint('\n')

		while self.selected == 0:
			n = input('Select file: ')
			if n == 'A':
				break
			if int(n) in self.filelist and self.filelist[int(n)]:
				self.selected = int(n)

		if n =='A':
			os.environ['db_Pass'] = db_Pass
			for i in range(1,len(list)):
				os.environ['extractAll'] = i
				run_command(pathlib.Path(__file__))
			return [self.filelist, 0]

		pprint('\n')
		pprint('Processing:', self.filelist[self.selected], '...')
		return [self.filelist, self.selected]

	def sum(self, o: list) -> str:
		(r,self.tables) = (0,[])
		for i in range(len(o)):
			r = r + o[i].r
			self.tables.append({
				'table':o[i].table,
				'rows':o[i].r
			})
		return str(r)

	def validateData(self, tables: list, indexes: list, date: str) -> list | bool:
		(drop,documents) = (False,[])
		for i in range(len(tables)):
			if len(Db.getInstance().list_collection_names(filter={'name':tables[i]+'_'+date})) == 0:
				drop = True
				break
			try:
				Db.getInstance()[tables[i]+'_'+date].create_index(indexes[i][0], unique=True)
			except (Exception) as e:
				pprint('Index error on "'+color.BOLD+tables[i]+'_'+date+color.END+'": '+color.RED, e, color.END)
				drop = True
				break
			documents.append(Db.getInstance()[tables[i]+'_'+date].count_documents({}))
		if drop == False:
			Db.disconnect()
			return documents

		for i in range(len(tables)):
			pprint('  ■ Dropping collection "'+color.BOLD+tables[i]+'_'+date+color.END+'"'+color.END)
			Db.getInstance().drop_collection(tables[i]+'_'+date)
		Db.disconnect()
		return False

	def extract(self, date: str) -> bool:
		global IO
		(timeStamp,o)=('',[])
		tables,indexes = index()
		for i in range(len(tables)):
			o.append(Data(tables[i], []))
			o[i].indexes = indexes[i]

		documents = self.validateData(tables, indexes, date)
		if documents != False:
			for i in range(len(tables)):
				o[i].r = documents[i]
			answer = '' if os.environ.get('extractAll') == None else 'y'
			while answer not in ['y','N']:
				answer = input('Data already exists. Would you like to use the saved data? [y/N]: ')
			if answer == 'N':
				for i in range(len(tables)):
					Db.getInstance().drop_collection(tables[i]+'_'+date)
					o[i].r = 0
				Db.disconnect()
				pprint('\n')
			else:
				return True
		
		(workers,n) = (Worker.fork('orders_trades'),0)
		with gzip.open(self.filelist[self.selected], 'rb') as fp:
			for buffer in fp:
				if timeStamp != str(time.time()).split('.',1)[0]:
					while float(psutil.virtual_memory()[2]) > 90:
						time.sleep(1)
						pstats(o, n)
					timeStamp = str(time.time()).split('.',1)[0]

				buffer = buffer.replace(b'\x01', b'|')
				buffer = buffer.replace(b'\\0', b'NULL')
				utf8_in = buffer.decode('utf8', 'strict')
				if len(utf8_in) > 0:
					n = n + len(utf8_in)
					o = workers.stats(o, tables)
					pstats(o, n)

					for i in range(len(o)):
						if forTable(o[i].table, utf8_in) == True:
							workers.io.send(base64.b64encode(utf8_in.encode('utf-8')).decode())
							break

		s = ''
		fp.close()
		fp = None
		time.sleep(1)
		while s!=self.sum(o) or s!=str(IO):
			s = self.sum(o)
			o = workers.stats(o, tables)
			pstats(o, [n, False])
			time.sleep(1)

		for i in range(threads+1):
			workers.io.send('EXIT', i)
		exits = 0
		while exits < threads+1:
			string = workers.get()
			if string.find('Goodbye') > 0:
				pid, string = string.split(' ', 1)
				(exits,pid) = (exits+1,int(pid))
				os.waitpid(-1, 0)

		workers.exit()
		(err,documents) = (0,self.validateData(tables, indexes, date))

		if documents == False:
			pprint('\n'+color.RED+'Error building data'+color.END)
			pstats(o, [n, False], True)
			return False
			
		pstats(o, [n, False], True, documents)
		for i in range(len(o)):
			if o[i].r != documents[i]:
				err = err + 1
				pprint(color.RED+o[i].table+' parsed value: '+str(o[i].r)+', database value: '+str(documents[i])+color.END)
		if err > 0:
			return self.extract(date)
		return True

	def book(self, date: str) -> None:
		global IO, err
		(table,IO,err) = ('MESSAGE',0,0)
		if Db.getInstance().list_collection_names(filter={'name':table+'_'+date}) == []:
			return
		query, projection = getQuery.new_order_extended()
		(docs,q) = (0,Db.getInstance()[table+'_'+date].find(query, projection = projection))
		try:
			(n,s) = (0,q.explain())
			if s['ok']:
				docs = s['executionStats']['nReturned']
		except (Exception) as e:
			pass

		def setExecuteTime(time: float | bool) -> None:
			if time == False:
				return
			if order['ExecuteTime'] == False:
				order['ExecuteTime'] = time
				return
			if time > order['ExecuteTime']:
				order['ExecuteTime'] = time
			return

		def setCloseTime(time: float | bool) -> None:
			if time == False:
				return
			if order['CloseTime'] == False:
				order['CloseTime'] = time
				return
			if time > order['CloseTime']:
				order['CloseTime'] = time
			return

		if docs > 0:
			for d in q:
				n = n + 1
				bstats(docs, n)
				order = d.copy()
				(ClOrdID,edocs,rm) = ([order['ClOrdID']],1,[])
				for OrdID in ClOrdID:
					query, projection = getQuery.order_attached(OrdID)
					(edocs,eq) = (0,Db.getInstance()[table+'_'+date].find(query, projection = projection))
					try:
						s = eq.explain()
						edocs = s['executionStats']['nReturned'] if s['ok'] else 0
					except (Exception) as e:
						pass
					if edocs == 0:
						continue
					for ed in eq:
						e_order = ed.copy()
						rm.append(e_order['_id'])
						setExecuteTime(e_order['ExecuteTime'])
						setCloseTime(e_order['CloseTime'])
						for ord in e_order['Orders']:
							if ord['ClOrdID'] not in ClOrdID:
								ClOrdID.append(ord['ClOrdID'])
							order['Orders'].append(ord)
				v = order.copy()
				v.pop('_id', None)
				try:
					Db.getInstance()[table+'_'+date].update_one({'_id':order['_id']}, {'$set':v})
					if len(rm) > 0:
						Db.getInstance()[table+'_'+date].delete_one({'_id':{'$in':rm}})
				except (Exception, TypeError) as e:
					pprint('\n' + color.RED + 'Database Error:', e, color.END)
					pprint(color.YELLOW + 'Update:', {'_id':order['_id']}, json.dumps(v, indent=4, sort_keys=True), color.END)
					pprint(color.YELLOW + 'Delete:', json.dumps({'_id':{'$in':rm}}, indent=4, sort_keys=True), color.END)
					err = err + 1
		bstats(docs, n, True)

	def analyze(self, date: str) -> None:
		self.book(date)

		#while True:
		#	time.sleep(1)

if __name__ == '__main__':
	def run() -> None:
		global db_Pass, filelist, selected, IO

		IO = 0
		if len(db_User) > 0 and len(db_Pass) == 0:
			_db_Pass = os.environ.get('db_Pass')
			while True:
				db_Pass = _db_Pass if _db_Pass!=None else input('Database password: ')
				try:
					stats = Db.getInstance().command("dbstats")
				except (Exception) as e:
					pprint(color.RED, e, color.END)
					Db.disconnect()
					db_Pass = ''
				else:
					Db.disconnect()
					if stats['ok'] == 1:
						break

		b = Build()
		filelist, selected = b.selectFile()
		if selected == 0:
			return
		date = b.filelist[b.selected].split('.',3)[2].split('-',1)[0]
		if b.extract(date) == True:
			if os.environ.get('extractAll') == None:
				b.analyze(date)

	if bindConflict() == True:
		raise SystemExit
	run()			

	ppid = os.getpid()
	for process in psutil.process_iter():
		_ppid = process.ppid()
		if _ppid == ppid:
			_pid = process.pid
			if sys.platform == 'win32':
				process.terminate()
			else:
				os.system('kill -9 {0}'.format(_pid))
	try:
		sys.exit(0)
	except SystemExit:
		os._exit(0)
