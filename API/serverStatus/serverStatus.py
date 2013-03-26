#!/usr/bin/python

import sys
import time
import json
import config
import datetime
import urllib
import urllib2
from urlparse import urlparse
from socket import socket

CARBON_SERVER = config.CARBON_SERVER
CARBON_PORT = config.CARBON_PORT
GRAPHITE_PREFIX = config.GRAPHITE_PREFIX
API_SERVER = config.API_SERVER
API_PORT = config.API_PORT
API_KEY = config.API_KEY

class serverStatus( object ):
  def __init__( self ):
    pass
  
  def get_serverStatus_API( self ):

    out = {}

    api_key = urllib.urlencode({'api_key': API_KEY})
    url = "http://%s:%s/serverStatus" % ( API_SERVER, API_PORT )
    
    try:
      out = json.loads(urllib2.urlopen(url, api_key).read())["data"]
    except Exception, e:
      print "Unable to fetch data: %s" % (e)
      sys.exit(1)
    
    return out

  def put_graphite( self, data, thedate ):

    self.sock = socket()
    self.sock.connect( (CARBON_SERVER,CARBON_PORT) )

    lines = []

    for i in data['opcounters']:
      # format for graphite is: metric_path value timestamp
      str = "%s.%s %s %s" % ( GRAPHITE_PREFIX, i, data['opcounters'][i], thedate)
      lines.append(str)
  
    message = '\n'.join(lines) + '\n' #all lines must end in a newline
    print "sending message to graphite: %s\n" % (GRAPHITE_PREFIX)
    print message
    print
    
    try:
      self.sock.sendall(message)
    except Exception, e:
      print "unable to send to graphite: %s" % (e)
      sys.exit(1)

  def test_sock(self):

    self.sock = socket()
    try:
      self.sock.connect( (CARBON_SERVER,CARBON_PORT) )
      return True
    except:
      print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT }
      sys.exit(1)

if __name__ == "__main__":

  delay = 5       # change me to whatever makes sense for you

  p = serverStatus()

  if p.test_sock():
  
    while True:

      p.put_graphite( p.get_serverStatus_API(), time.time() )
      time.sleep( delay )
