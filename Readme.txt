Testing Samples:

A) Accessing server directly

The contents of the server can be viewed by opening terminal and python interpreter

python

import xmlrpclib

from xmlrpclib import Binary

s = xmlrpclib.ServerProxy('http://localhost:51234)

s.get_content() #this lists the contents in the server

B) Corruption of Data in the file System:

Open a new terminal, keeping the file system running and corrupt the data by proxying into one 

of the servers

Open Terminal

python

import xmlrpclib

from xmlrpclib import Binary

s = xmlrpclib.ServerProxy('http://localhost:51234)

s.get_content() #this lists the contents in the particular server

s.list_content() # returns a list of keys present in the server

s.corrupt(‘key_value’)# use one of the keys present in the list. This would corrupt the value

C) Bringing down a server:

Open a new terminal, keeping the file system running and corrupt the data by proxying into one 

of the servers

Open Terminal

python

import xmlrpclib

from xmlrpclib import Binary

s = xmlrpclib.ServerProxy('http://localhost:51234)

s.get_content() #this lists the contents in the particular server

s.terminate() #shuts down the server