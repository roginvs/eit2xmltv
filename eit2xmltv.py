# -*- coding: utf-8 -*-
import sys
import ConfigParser
import eitreader
import xmltv_generator
import re
import socket
import struct
import time
import datetime
import traceback

config_file_name = 'eit2xmltv.cfg'

sys.stdout.write(u'Started at %s\n' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
config = ConfigParser.RawConfigParser()
sys.stdout.write (u'Loading configuration from %s.. ' % config_file_name)
if not config.read(config_file_name):
    sys.stdout.write(u'Error!.\n')
    exit(1)                
else:
    sys.stdout.write(u'ok\n')
xmltv = xmltv_generator.xmltv()

successfull_xmltvids = []

for try_num in range(1,int(config.get('eit2xmltv','retry_count'))+1):
  xmltvids_to_do = list (x for x in sorted(config.options('sources')) if not x in successfull_xmltvids)
  if (xmltvids_to_do):
      sys.stdout.write (u'\nTry to get EIT #%s (from %s).\n' % (try_num,config.get('eit2xmltv','retry_count')))
      for xmltvid in xmltvids_to_do:    
        try:      
            link = config.get('sources',xmltvid)    
            sys.stdout.write(u"Watching '%s'.." % link)
            r = re.search('^udp://@(.+):(\d+)$',link)
            group = r.group(1)
            port = int(r.group(2))
            begin_time = time.time()
            last_time_eit_checked = begin_time

            eit = eitreader.eitreader()
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 188*7*1000)
            sock.bind(('', port))
            mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            sock.settimeout(int(config.get('eit2xmltv','socket_timeout')))
            check_eit_period = int(config.get('eit2xmltv','check_eit_period'))
            check_eit_timeout = int(config.get('eit2xmltv','check_eit_timeout'))
            
            #f = open('temp-%s.ts' % group,'wb')
            while True:
              iptv_data = sock.recv(188*7)
             # f.write(iptv_data)
              for i in range(0,7):
                  eit.load_ts_packet(iptv_data[188*i:188*i+188])
              if time.time() - last_time_eit_checked > check_eit_period:
                  if eit.check_eit_schedule_fullness():
                      break
                  last_time_eit_checked = time.time()
                  sys.stdout.write('.')
              if time.time() - begin_time > check_eit_timeout:                  
                  if eit.is_some_eit_present():
                      if config.get('eit2xmltv','apply_eit_with_errors'):
                          break
                      else:
                          raise Exception("No EIT without errors present!")                          
                  else:
                      raise Exception ("No EIT at all!")
            events = eit.return_eit_events()            
            if eit.errors():
                 sys.stdout.write (u'(Errors: %s) ' % eit.errors())                                
            channel_name = events.keys()[0]   #TODO: MPTS check
            xmltv.load_eit(xmltvid,channel_name,events[channel_name])
            sys.stdout.write (u" ok, got channel '%s'.\n" % channel_name)
            successfull_xmltvids.append(xmltvid)
        except:
           exc_type, exc_value, exc_traceback = sys.exc_info()
           exc_string = "%s" % exc_value
           sys.stdout.write (u" FAIL: %s " % exc_string)
           if eit.errors():
               sys.stdout.write (u'(Errors: %s) ' % eit.errors())
           if not eit.pat is None:
             sys.stdout.write(u'(')
             sys.stdout.write (u', '.join (eit.sdt[x]['service_name'] for x in eit.pat.keys() if not eit.sdt is None and x in eit.sdt.keys() and 'service_name' in eit.sdt[x].keys()))
             sys.stdout.write(u') ')
           sys.stdout.write(u'\n')
           if (exc_string != 'No EIT at all!' and exc_string != 'timed out' and exc_string != 'No EIT without errors present!'):
             traceback.print_tb(exc_traceback, limit=5, file=sys.stdout)
        try:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except:
            pass

       
out_file_name = config.get('eit2xmltv','outfilename')
sys.stdout.write(u"Saving to file '%s'.. " % out_file_name)
xmltv.save_xmltvgz(out_file_name)
sys.stdout.write(u'All done!\n')

