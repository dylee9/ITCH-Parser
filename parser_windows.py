#parser.py

import gzip
import struct
import os
import datetime
import pandas

'''
=====================================================================
						IMPLEMENTATION
=====================================================================
Author: Jason Lee
The program parses the NASDAQ ITCH 5.0 data file and outputs the 
hourly moving volume weighted average price (VWAP) for each stock
traded during each hour. This is done by reading the leading message
type letter and parsing according to the length of the corresponding
message length. The program uses the Python struct library to parse 
the message into processable data, which is then processed using
the pandas library.
'''

class Parser():

	def __init__(self, msg_sizes, filename):
		'''
		input:
			msg_sizes -> dict:
				Hashtable of msg type letter mapped to msg length

			filename -> string:
				String of filename of type (.gz) of NASDAQ ITCH 5.0 Data

		output:
			Parser object
		'''

		self.binary_data = gzip.open(filename,'rb')
		self.msg_size_table = msg_sizes
		self.data = []
		self.currentHour = None

	def read_binary_msg_type(self):
		#Read one byte of binary encoding
		msg_type = self.binary_data.read(1)
		return msg_type

	def read_binary(self, msg_size):
		#Read msg_size bytes of binary encoding
		msg_data = self.binary_data.read(msg_size)
		return msg_data

	def close_binary(self):
		#Close opened file to free up space
		self.binary_data.close()

	def check_for_trade_msg(self, msg_type):
		# #Check whether a trade has been executed
		# #If so, store trade data (time, stock, shares, price)
		# if msg_type in self.msg_size_table:
		# 	msg = self.binary_data.read(self.msg_size_table[msg_type])

		# #'P' message type refers to a Non-Cross Trade Message
		# if msg_type == 'P':
		# 	self.process_trade(msg)

		try:
			#If byte is a message type, read it's message
			msg = self.binary_data.read(self.msg_size_table[msg_type])

			#'P' message type refers to a Non-Cross Trade Message
			#Store trade data
			if msg_type == b'P':
				self.process_trade(msg)
		except KeyError:
			pass

	def convert_time(self, timestamp):
		#Parse timestamp into hour data
		time = datetime.datetime.fromtimestamp(timestamp / 1e9)
		time = time.strftime('%H')
		return time

	def parse(self, msg):
		#Parse binary msg encoding
		temp = struct.unpack('>4s6sQcI8sIQ', msg)
		temptime = struct.pack('>2s6s', b'\x00\x00', temp[1])
		timestamp = struct.unpack('>Q', temptime)[0]

		#Trim whitespace of stock symbol
		try:
			stock = (temp[5].strip()).decode("utf-8")
		except UnicodeDecodeError:
			stock = "N/A"
		shares = temp[4]

		#Adjust prices
		price = float(temp[6]) / 10000
		return (timestamp, [stock , shares, price])

	def process_trade(self, msg):
		timestamp, parsed = self.parse(msg)
		hour = int(self.convert_time(timestamp))

		if self.currentHour == None:
			#For first loop, currentHour is not set yet
			self.currentHour = hour

		elif (self.currentHour + 1) % 24 == hour:
			#New hour has been reached and we must output data from the past hour
			#Create dataframe from pandas module to calculate VWAP column
			panda = pandas.DataFrame(self.data, columns = ["Stock", "Shares", "Price"])
			panda["Total"] = panda["Shares"] * panda["Price"]
			panda = panda.groupby(panda["Stock"])["Shares", "Total"].sum()
			panda["VWAP"] = (panda["Total"] / panda["Shares"]).round(2)

			#Reset index and output stock symbol and it's vwap
			panda = panda.reset_index()
			panda = panda[["Stock", "VWAP"]]

			panda.to_csv(str(self.currentHour) + "00.txt", sep=' ', index=False)
			print("time: " + str(self.currentHour) + ":00")
			print(panda)

			#Reset temp data
			self.data = []
			self.currentHour = hour

		#Store trade data
		self.data.append(parsed)

if __name__ == '__main__':

	NASDAQ_MSG_SIZES = {
		b'S' : 11,
		b'R' : 38,
		b'H' : 24,
		b'Y' : 19,
		b'L' : 25,
		b'V' : 34,
		b'W' : 11,
		b'K' : 27,
		b'A' : 35,
		b'F' : 39,
		b'E' : 30,
		b'C' : 35,
		b'X' : 22,
		b'D' : 18,
		b'U' : 34,
		b'P' : 43,
		b'Q' : 39,
		b'B' : 18,
		b'I' : 49,
		b'N' : 19
	}

	NASDAQ_FILENAME = '01302019.NASDAQ_ITCH50.gz'

	Parser = Parser(NASDAQ_MSG_SIZES, NASDAQ_FILENAME)
	msg_type = Parser.read_binary_msg_type()

	while(msg_type):
		Parser.check_for_trade_msg(msg_type)
		msg_type = Parser.read_binary_msg_type()

	Parser.close_binary()
	Parser.save_excel()




