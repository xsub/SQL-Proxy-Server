#!/usr/bin/env python

# (c) 2014-2015 LINBEDDED Pawel Suchanecki
#               pawel.suchanecki@gmail.com

import os
import re
import socket
import sys

# rcvd msg
data = '';

# ip
ip = ''

# globals used before START
rdy_file = sys.argv[2] + ".rdy"

def is_valid_ipv4(ip_str):
    """
    Check the validity of an IPv4 address
    """
    try:
        socket.inet_pton(socket.AF_INET, ip_str)
    except AttributeError:
        try:
            socket.inet_aton(ip_str)
        except socket.error:
            return False
        return ip_str.count('.') == 3
    except socket.error:
        return False
    return True

def write_error_msg (pdata):
	print sys.argv[2]
	f = open(sys.argv[2], "w+")
	f.writelines(str(pdata))
	f.close()
	# rdy file
	r = open(rdy_file, 'w')
	r.writelines("1")
	r.close()
	sys.exit(1)

# START

# read config
conf = open ('/tmp/SERVER_IP', 'r')
for line in conf:
	ip=line.rstrip('\n')
conf.close

# validate ip
if is_valid_ipv4(ip):
	pass
else:
	write_error_msg ("DB_ERROR: adres IP jest nieprawidlowy\n");

# remove dest file
if os.path.isfile(sys.argv[2]):
	#print "removing file: %s\n" % sys.argv[2]
	os.remove(sys.argv[2])


if os.path.isfile(rdy_file):
	os.remove(rdy_file)



try:

	# create a TCP/IP socket
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# set timeout
	sock.settimeout(3)

	# connect the socket to the port where the server is listening
	server_address = (ip, 9999)

	# print >>sys.stdout, 'connecting to %s port %s' % server_address
	sock.connect(server_address)

	# After the connection is established
	# data can be sent through the socket with sendall()
	# and received with recv(), just as in the server.

    	# Send data
	#message = 'BASE:pomiary#SELECT * from pomiary order by ID_ZAP;'
	message = sys.argv[1]
	#print >>sys.stderr, 'sending "%s"' % message
	sock.sendall(message)

    # Look for the response
	amount_received = 0

	m = re.search ('x', 'y');

	while ( m == None ):
		data += sock.recv(16)
		amount_received += len(data)
		m = re.search ('(DB_OK|DB_ERROR)', str(data))

	#print >>sys.stdout, '%s' % data
	f = open(sys.argv[2], "w+")
	f.writelines(str(data))
	f.close()

	# rdy file
	open(rdy_file, 'w').close()

except socket.error as msg:
	data = 'DB_ERROR: socket.error: ' + str(msg) + "\n"
	write_error_msg(data)

except socket.herror as msg:
	data = 'DB_ERROR: socket.herror: ' + str(msg) + "\n"
	write_error_msg(data)

except socket.gaierror as msg:
	data = 'DB_ERROR: socket.gaierror: ' + str(msg) + "\n"
	write_error_msg(data)

except socket.timeout as msg:
	data = 'DB_ERROR: socket.timeout: ' + str(msg) + "\n"
	write_error_msg(data)


finally:
	#print >>sys.stderr, 'closing socket'
	sock.close()

