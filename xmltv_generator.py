# -*- coding: utf-8 -*-
import eitreader
import datetime
import gzip

class xmltv():
    def __init__(self):
        self._names = {}
        self._events = {}
        self._iconlinks = {}
    def load_eit(self,xmltvid,channel_name,events,icon_link = None):
        self._names[xmltvid] = channel_name
        self._events[xmltvid] = events
        self._iconlinks[xmltvid] = icon_link
        
    def save_xmltvgz(self,filename):                
        # Some specific xml modules may be used, but seems to me this work fine too
        r = u'<?xml version="1.0" encoding="utf-8" ?>\n'
        r += u'  <tv>\n'
        for xmltvid in sorted(self._names.keys()):
           r += u'    <channel id="%s">\n' % xmltvid           
           r += u'       <display-name lang="ru">%s</display-name>\n' % self._names[xmltvid]
           if not self._iconlinks[xmltvid] is None:
               r+= u'      <icon src="%s" />\n' % self._iconlinks[xmltvid]
               
           r += u'    </channel>\n'
        for xmltvid in sorted(self._names.keys()):            
            for event in sorted(self._events[xmltvid],key = lambda x: x['start']):
                #FIXME - timezone auto
                r += u'    <programme start="%s" stop="%s" channel="%s">\n' % (\
                     datetime.datetime.fromtimestamp(event['start']).strftime(u'%Y%m%d%H%M%S +0400'),
                     datetime.datetime.fromtimestamp(event['stop'] ).strftime(u'%Y%m%d%H%M%S +0400'),
                      xmltvid)      
                r += u'      <title lang="ru">%s</title>\n' % event['title']
                r += u'      <desc lang="ru">%s</desc>\n' % event['desc']
                r += u'    </programme>\n'
                
        r += u'  </tv>\n'
        f = gzip.open(filename, 'wb')
        f.write(r.encode('utf-8'))
        f.close()        
        


