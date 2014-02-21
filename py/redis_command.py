#!/usr/bin/env python
import re
import sys
import getopt
import socket

def usage():
	usage_info = [
		("-h", "Display this help page"),
		("-X", "Enable parallel mode"),
		("-c", "The command to send to redis instance"),
		("-p", "Output the result as 'hostport: result'"),
		("-f", "Output with formatting"),
	]
	format_info = [
		("host",     "The hostname we sent the request to"),
		("port",     "The port we sent the request to"),
		("hostport", "A concatenation of host:port"),
		("response", "The whole response to your request"),
	]
	print "Usage:"
	print "\t%s [options] hostlist" % (sys.argv[0])
	print
	print "Options:"
	for entry in usage_info:
		print "\t%-8s %s" % (entry[0], entry[1])
	print
	print "Notes:"
	print "\thostlist is a list of hostname:port separated by space"
	print
	print "Formatting:"
	print "\tA format specifier looks like %{thing}"
	print "\tThe following specifiers are always available:"
	for entry in format_info:
		print "\t\t%-16s: %s" % (entry[0], entry[1])
	print
	print "\tYou can also use any specifier that matches a key from the",
	print "info command"
	print
	print "Examples:"
	print "\t%s -c 'SET mykey Hello' -p localhost:6379" % (sys.argv[0])
	print "\t%s -c 'GET mykey' -p localhost:6379" % (sys.argv[0])
	print "\t%s -c info -f '%%{uptime_in_seconds}' localhost:{6379..6400}" % \
	      (sys.argv[0])
	print "\t%s -c info -f '%%{hostport}: %%{role}' localhost:{6379..6400}" % \
	      (sys.argv[0])

class RedisException(Exception):
	pass

def send_command(host, request, result_buffer):
	# send the command and get a response
	s = socket.socket()
	s.connect((host.split(":")[0], int(host.split(":")[1])))
	s.sendall("%s\r\n" % (request))

	# we're going to pull a few bytes off the wire, the first byte is
	# significant as it tells us what the rest of the bytes mean
	response_head = s.recv(128)

	# use the first byte to figure out what to do next
	if response_head[0] in ["+", "-"]:
		# add the response_head to the response
		result_buffer["response"] += response_head

		# if there's no terminating \r\n, we need more data
		if -1 == result_buffer["response"].rfind("\r\n"):
			result_buffer["response"] += s.recv(4096)

		# account for errors
		if response_head[0] == "-":
			raise RedisException()

	elif response_head[0] == "$":
		# figure out the size of the response by looking at the next few bytes
		index = response_head.find("\r\n", 1)
		response_length = int(response_head[1:index])

		# read the rest of the bytes, but account for the fact that we already
		# got len(response_head) bytes!  the \r\n is still there too, so skip it
		result_buffer["response"] = response_head[index+2:] + \
		                            s.recv(response_length)

		# this might be able to go into a separate function later, but for now
		# the only special case we have is the 'info' command
		if request == "info":
			for line in result_buffer["response"].split("\r\n"):
				if len(line) > 0 and line[0] != "#":
					result_buffer[line.split(":")[0]] = line.split(":")[1]

	s.close()

def main():
	options  = "hXpc:f:"
	commands = 0
	parallel = False
	hosts = []
	# re for hostname:port
	host_re = re.compile(r"^[A-Za-z0-9.-]+:[0-9]+$")
	# re for %{tagname}
	tag_re = re.compile(r"%{([a-zA-Z0-9_-]+)}")

	# first pass, find/process global options
	try:
		opts, args = getopt.getopt(sys.argv[1:], options)
	except getopt.GetoptError:
		usage()
		sys.exit(1)
	for o, a in opts:
		# we're not dealing w/ prints yet
		if o in ("-p", "-f"):
			continue
		# for commands, just count them to decide on optimizations
		if o == "-c":
			commands += 1
		if o == "-h":
			usage()
			sys.exit()
		if o == "-X":
			parallel = True

	# next, let's put together a list of hosts
	for host in args:
		if host_re.match(host):
			hosts.append(host)
		else:
			print "[%s] is not in the form of <host>:<port>" % (host)

	# make sure we have at least one host to work on
	if len(hosts) < 1:
		print "No hosts specified"
		sys.exit(1)

	# TODO: handle pre-connections here

	# run the commands on each host
	result_buffer = {}
	for host in hosts:
		# pre-populate result_buffer with always-available entries
		result_buffer["host"] = host.split(":")[0]
		result_buffer["port"] = host.split(":")[1]
		result_buffer["hostport"] = host
		result_buffer["response"] = ""

		for o, a in opts:
			# these were already dealt with
			if o in ("-h", "-X"):
				continue
			# send a command
			elif o == "-c":
				try:
					send_command(host, a, result_buffer)
				except RedisException:
					print "%s: Invalid command" % (host)
					result_buffer.clear()
					break
			# print output
			elif o == "-p":
				print "%s: %s" % (host, result_buffer["response"])
			# print output with formatting
			elif o == "-f":
				try:
					print re.sub(tag_re, lambda m: result_buffer[m.group(1)], a)
				except KeyError:
					print "Invalid format string!"
					sys.exit(1)

		# after each host, we clear the buffer
		result_buffer.clear()

if __name__ == "__main__":
	main()
