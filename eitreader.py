# -*- coding: utf-8 -*-
import sys
import math
import time
import datetime
import calendar



TS_HEADER_BITS=(8,1,1,1,13,2,2,4)
TS_HEADER_NAMES=('sync','tei','pusi','tp','pid','tsc','afc','cc')
EIT_TABLE_BITS = (8,1,1,2,12,16,2,5,1,8,8,16,16,8,8)
EIT_TABLE_NAMES = ('table_id', 'section_syntax_indicator', 'reserved_future_use', 'reserved', 'section_length', 'service_id', 'reserved', 'version_number', 'current_next_indicator', 'section_number', 'last_section_number', 'transport_stream_id', 'original_network_id', 'segment_last_section_number','last_table_id')
EIT_TABLE_EVENTS_BITS = (16,40,24,3,1,12)
EIT_TABLE_EVENTS_NAMES = ('event_id', 'start_time', 'duration', 'running_status', 'free_CA_mode', 'descriptors_loop_length')

SDT_TABLE_BITS=(8,1,1,2,12,16,2,5,1,8,8,16,8)
SDT_TABLE_NAMES = ('table_id', 'section_syntax_indicator', 'reserved_future_use', 'reserved', 'section_length', 'transport_stream_id', 'reserved', 'version_number', 'current_next_indicator', 'section_number', 'last_section_number', 'original_network_id','reserved_future_use')
                   
DESCRIPTORS_DEFINITION = {0x40:'network_name_descriptor', 0x41:'service_list_descriptor', 0x42:'stuffing_descriptor',
                          0x43:'satellite_delivery_system_descriptor', 0x44:'cable_delivery_system_descriptor',
                          0x45:'VBI_data_descriptor', 0x46:'VBI_teletext_descriptor', 0x47:'bouquet_name_descriptor',
                          0x48:'service_descriptor', 0x49:'country_availability_descriptor', 0x4A:'linkage_descriptor',
                          0x4B:'NVOD_reference_descriptor', 0x4C:'time_shifted_service_descriptor', 0x4D:'short_event_descriptor',
                          0x4E:'extended_event_descriptor', 0x4F:'time_shifted_event_descriptor', 0x50:'component_descriptor',
                          0x51:'mosaic_descriptor', 0x52:'stream_identifier_descriptor', 0x53:'CA_identifier_descriptor',
                          0x54:'content_descriptor', 0x55:'parental_rating_descriptor', 0x56:'teletext_descriptor',
                          0x57:'telephone_descriptor', 0x58:'local_time_offset_descriptor', 0x59:'subtitling_descriptor',
                          0x5A:'terrestrial_delivery_system_descriptor', 0x5B:'multilingual_network_name_descriptor',
                          0x5C:'multilingual_bouquet_name_descriptor', 0x5D:'multilingual_service_name_descriptor',
                          0x5E:'multilingual_component_descriptor', 0x5F:'private_data_specifier_descriptor',
                          0x60:'service_move_descriptor', 0x61:'short_smoothing_buffer_descriptor',
                          0x62:'frequency_list_descriptor', 0x63:'partial_transport_stream_descriptor',
                          0x64:'data_broadcast_descriptor', 0x65:'scrambling_descriptor', 0x66:'data_broadcast_id_descriptor',
                          0x67:'transport_stream_descriptor', 0x68:'DSNG_descriptor', 0x69:'PDC_descriptor', 0x6A:'AC-3_descriptor',
                          0x6B:'ancillary_data_descriptor', 0x6C:'cell_list_descriptor', 0x6D:'cell_frequency_link_descriptor',
                          0x6E:'announcement_support_descriptor', 0x6F:'application_signalling_descriptor', 0x70:'adaptation_field_data_descriptor',
                          0x71:'service_identifier_descriptor', 0x72:'service_availability_descriptor', 0x73:'default_authority_descriptor',
                          0x74:'related_content_descriptor', 0x75:'TVA_id_descriptor', 0x76:'content_identifier_descriptor',
                          0x77:'time_slice_fec_identifier_descriptor', 0x78:'ECM_repetition_rate_descriptor',
                          0x79:'S2_satellite_delivery_system_descriptor', 0x7A:'enhanced_AC-3_descriptor', 0x7B:'DTS descriptor',
                          0x7C:'AAC descriptor', 0x7D:'XAIT location descriptor', 0x7E:'FTA_content_management_descriptor', 0x7F:'extension descriptor'}
for i in range(0x80,0xFF):
    DESCRIPTORS_DEFINITION[i]='user defined %s' % hex(i)
EIT_TABLE_EVENTS_DESCRIPTORS=DESCRIPTORS_DEFINITION
EXTENDED_EVENT_DESCRIPTOR_BITS=(4,4,24,8)
EXTENDED_EVENT_DESCRIPTOR_NAMES=('descriptor_number','last_descriptor_number','ISO_639_language_code','length_of_items')

PAT_TABLE_BITS=(8,1,1,2,12,16,2,5,1,8,8)
PAT_TABLE_NAMES=('table_id','section_syntax_indicator','zero','reserved','section_length','transport_stream_id','reserved','version_number','current_next_indicator','section_number','last_section_number')

SDT_TABLE_SERVICE_BITS=(16,6,1,1,3,1,12)
SDT_TABLE_SERVICE_NAMES=('service_id','reserved_future_use','EIT_schedule_flag','EIT_present_following_flag','running_status','free_CA_mode','descriptors_loop_length')


class eitreader():
    def __init__(self):        
        self.eit = {}
        self.pat = None
        self.sdt = None
        self._interesting_pids = [0x0,0x11,0x12]
        self._last_cc = {}
        self._sndu = {}
        self._errors = {}        

    def errors(self):
        return ', '.join("%s: %s" % (x,self._errors[x]) for x in self._errors.keys())

    def _inc_error(self,text):        
        if not text in self._errors.keys():
            self._errors[text] = 1
            #sys.stdout.write ("%s, " % text)
        else:
            self._errors[text] += 1
            #sys.stdout.write ("%s, " % text)
        
    def load_ts_packet(self,data):
        if (len(data)==0):
            return
        if (len(data) != 188):
            raise Exception('Wrong data!')
        
        (header,i) = self._parse_headers(data,TS_HEADER_BITS,TS_HEADER_NAMES)        
        # Don't check i is not null because we already checked that data is 188 length
        
        if header['pusi'] != 0:
            i += 1
            pointer = ord(data[4])
            if pointer != 0:
              i += pointer

        pid = header['pid']
        if pid in self._interesting_pids:
            #self._myhex(data)
            if header['tsc'] != 0:
               self._inc_error("WARN: scrambled pid %s!" % hex(pid))
               return
            
            if not pid in self._last_cc.keys():
                self._last_cc[pid] = None
            if not pid in self._sndu.keys():
                self._sndu[pid] = None
                
            if self._last_cc[pid] != None:        
                 if (((header['cc'] - self._last_cc[pid]) & 15) != 1):
                     self._inc_error ("WARN: pid %s cc error!" % hex(pid))
                     self._sndu[pid] = None
            self._last_cc[pid] = header['cc']
            if header['pusi'] != 0:        
                self._parse_table(pid,self._sndu[pid])                
                self._sndu[pid] = data[i:188]
            else:
                if not self._sndu[pid] is None:
                    self._sndu[pid] += data[i:188]
                        
    def _parse_headers(self,data,header_bits,header_names):
        if len(header_bits) != len(header_names):
             raise Exception('len(header_bits) != len(header_names)')
        total_bits = sum(header_bits)
        if total_bits % 8 != 0:
            raise Exception('Unsupported header_bits!')
        total_bytes = int(total_bits/8)
        if (len(data) < total_bytes):            
            return ('',0)
        header_data = data[:total_bytes]
        header = {}
        # TODO: make normal
        # FIXME: This is very dirty hack
        binary_string = ''
        for x in header_data:
            binary_string += bin(ord(x))[2:].zfill(8)
        for x,y in zip(header_bits,header_names):
            header[y] = int(binary_string[:x],base=2)        
            binary_string = binary_string[x:]
            
        return (header,total_bytes)

    def _myhex(self,data):
        if len(data) > 0:
            print ':'.join ('%02x' % ord(x) for x in data)
        else:
            print "<nodata>"
        print ""
        

    def _parse_descriptors_loop(self,data):
        result = {}        
        while data:
            if (len(data) >= 2):
                desc_id = ord(data[0])
                desc_len = ord(data[1])
                if desc_id != 0xFF:
                  result[DESCRIPTORS_DEFINITION[desc_id]] = data[2:2+desc_len]
                  data = data[desc_len+2:]
                else:
                    self._inc_error('descriptors loop desc id error')                    
                    data = ()                    
            else:
                self._inc_error('descriptors loop parse error')                
                data = ()
        return result


    def _decode_text(self,text):
        CHARACTER_CODING_TABLE = {0x01:'ISO-8859-5',0x02:'ISO-8859-6',0x03:'ISO-8859-7',0x04:'ISO-8859-8',
                                  0x05:'ISO-8859-9',0x06:'ISO-8859-10',0x07:'ISO-8859-11',0x08:None,
                                  0x09:'ISO-8859-13',0x0A:'ISO-8859-14',0x0B:'ISO-8859-15',
                                  0x0C:None,0x0D:None,0x0E:None,0x0F:None,                                  
                                  0x10:None,0x11:'utf_16_be',0x12:'euc_kr',0x13:'gb2312',
                                  0x14:'big5',0x15:'UTF-8', # FIXME and TODO: Fix 0x10 "dynamic" encoding
                                  0x16:None,0x17:None,0x18:None,0x19:None,0x1A:None,0x1B:None,
                                  0x1C:None,0x1D:None,0x1E:None,
                                  0x1F:None
                                  }
                                  
        if text:
            try:
                first_character_number = ord(text[0])
                if (first_character_number >= 0x20 and first_character_number <= 0xFF):
                    text = text.decode('latin1')
                elif not CHARACTER_CODING_TABLE[first_character_number] is None:
                    text = text[1:].decode(CHARACTER_CODING_TABLE[first_character_number])                
                else:
                    text = u''
                    self._inc_error('TODO decode')
            except UnicodeDecodeError:
                self._inc_error('UnicodeDecodeError')
                text = u''
        return text
    

    def _parse_descriptors(self,parsed_loop):        
        if 'service_descriptor' in parsed_loop.keys():        
            service_descriptor = parsed_loop['service_descriptor']
            if (len (service_descriptor) >= 2):
                service_provider_name_length = ord(service_descriptor[1])
                parsed_loop['service_provider_name'] = service_descriptor[2:2+service_provider_name_length]
                if (len (service_descriptor) >= 1 + 2 + service_provider_name_length):
                    service_name_length = ord(service_descriptor[2+service_provider_name_length])
                    parsed_loop['service_name'] = self._decode_text( \
                          service_descriptor[3+service_provider_name_length:3+service_provider_name_length+service_name_length])
                else:
                    self._inc_error('service_descriptor service_name error')                    
            else:
                self._inc_error('service_descriptor error')
                
        if 'short_event_descriptor' in parsed_loop.keys():
          short_event_descriptor_data = parsed_loop['short_event_descriptor']
          if (len(short_event_descriptor_data) >= 4):
              event_name_length = ord(short_event_descriptor_data[3])
              parsed_loop['event_name'] = self._decode_text( short_event_descriptor_data[4:4+event_name_length] )
              if (len (short_event_descriptor_data) >= 1 + 4 + event_name_length):
                  event_text_length = ord(short_event_descriptor_data[4+event_name_length])
                  parsed_loop['event_text'] = self._decode_text( \
                      short_event_descriptor_data[4+event_name_length+1:4+event_name_length+1+event_text_length] )                                
              else:
                  self._inc_error('short_event_descriptor event_text error')                  
          else:
              self._inc_error('short_event_descriptor error')              
              
        if 'extended_event_descriptor' in parsed_loop.keys():
            extended_event_descriptor_data = parsed_loop['extended_event_descriptor']            
            (extended_event_descriptor_header,iii) = self._parse_headers(extended_event_descriptor_data,EXTENDED_EVENT_DESCRIPTOR_BITS,EXTENDED_EVENT_DESCRIPTOR_NAMES)
            if iii:
              items_raw = extended_event_descriptor_data[iii:iii+extended_event_descriptor_header['length_of_items']]
              if (len(extended_event_descriptor_data) >= 1 + iii+extended_event_descriptor_header['length_of_items']):
                  text_length = ord(extended_event_descriptor_data[iii+extended_event_descriptor_header['length_of_items']])        
                  parsed_loop['extended_text'] = self._decode_text( \
                      extended_event_descriptor_data[iii+extended_event_descriptor_header['length_of_items']+1:\
                                                      iii+extended_event_descriptor_header['length_of_items']+1+text_length] )                                                     
              else:
                  self._inc_error('extended_event_descriptor extended_text error')                  
            else:
                self._inc_error('extended_event_descriptor data less than header definition')                
                           
        return parsed_loop


    def _jd_to_date(self,jd):
        jd = jd + 0.5
        F, I = math.modf(jd)
        I = int(I)
        A = math.trunc((I - 1867216.25)/36524.25)
        if I > 2299160:
          B = I + 1 + A - math.trunc(A / 4.)
        else:
          B = I
        C = B + 1524
        D = math.trunc((C - 122.1) / 365.25)
        E = math.trunc(365.25 * D)
        G = math.trunc((C - E) / 30.6001)
        day = int(C - E + F - math.trunc(30.6001 * G))
        if G < 13.5:
         month = G - 1
        else:
         month = G - 13
        if month > 2.5:
         year = D - 4716
        else:
         year = D - 4715
        return year, month, day
    def _mjd_to_jd(self,mjd):
        return mjd + 2400000.5
    def _mjd_to_date(self,mjd):
        return self._jd_to_date(self._mjd_to_jd(mjd))
    def _start_time_to_unix(self,start_time): 
        mjd = (start_time & 0xFFFF000000L) / 0xFFFFFF
        stime = ("%s" % hex(int(start_time & 0xFFFFFF))[2:]).zfill(6)
        (y,m,d) = self._mjd_to_date(mjd)
        st = "%s%02d%02d%s" % (y,m,d,stime)
        dt = datetime.datetime(year=y, month=m, day=d, hour=int(stime[0:2]), minute=int(stime[2:4]), second=int(stime[4:6]),tzinfo=None)
        utime = calendar.timegm(dt.utctimetuple())
        return utime
    def _duration_to_seconds(self,duration):
        dtime = ("%s" % hex(int(duration & 0xFFFFFF))[2:]).zfill(6)
        return int(dtime[0:2])*60*60 + int(dtime[2:4])*60 + int(dtime[4:6])
        


        
    def _parse_table(self,pid,table_data):
        if table_data is None:
          return
        
        table_id = ord(table_data[0])
        if pid == 0x12:
          if ((table_id >= 0x50 and table_id <= 0x5F) or table_id == 0x4E): # EIT: actual TS, event schedule information  + present/following            
            (eit_header,iii) = self._parse_headers(table_data,EIT_TABLE_BITS,EIT_TABLE_NAMES)
            if iii:                
                table_events = table_data[iii:eit_header['section_length']+3-4]# -4 CRC32
                
                if not eit_header['service_id'] in self.eit.keys():
                     self.eit[eit_header['service_id']] = {}
                if not eit_header['table_id'] in self.eit[eit_header['service_id']].keys():
                     self.eit[eit_header['service_id']][eit_header['table_id']]  = {}
                if not eit_header['section_number'] in self.eit[eit_header['service_id']][eit_header['table_id']].keys():
                    self.eit[eit_header['service_id']][eit_header['table_id']][eit_header['section_number']] = {}
                events_dict = {}
                while (table_events):                    
                    (table_event,iii) = self._parse_headers(table_events,EIT_TABLE_EVENTS_BITS,EIT_TABLE_EVENTS_NAMES)
                    if iii:
                        descriptors_loop = table_events[iii:iii+table_event['descriptors_loop_length']]        
                        table_events = table_events[iii+table_event['descriptors_loop_length']:]
                        
                        parsed_event_descriptions = self._parse_descriptors(self._parse_descriptors_loop(descriptors_loop))
                        
                        events_dict[table_event['event_id']] = table_event
                        for x in parsed_event_descriptions.keys():
                          events_dict[table_event['event_id']][x] = parsed_event_descriptions[x]
                    else:
                        self._inc_error('eit events array error')                        
                        table_events = ()
                self.eit[eit_header['service_id']][eit_header['table_id']][eit_header['section_number']]['events'] = events_dict
                self.eit[eit_header['service_id']][eit_header['table_id']][eit_header['section_number']]['table'] = eit_header
            else:
                self._inc_error('eit data less than header definition')
                
        elif pid == 0x0:
           if table_id == 0x0: #PAT
              if self.pat is None:
                 self.pat = {}               
              (pat_header,iii) = self._parse_headers(table_data,PAT_TABLE_BITS,PAT_TABLE_NAMES)
              if iii:
                  pat_programs = table_data[iii:pat_header['section_length']+3-4]
                  for i in xrange(0,len(pat_programs),4): 
                      program_number = ord(pat_programs[i])*256 + ord(pat_programs[i+1])
                      pid_number = (0b00011111 & ord(pat_programs[i+2])) +  ord(pat_programs[i+3])
                      if program_number != 0:
                          self.pat[program_number] = pid_number                      
                      else: #NIT pid detected
                          pass
              else:
                  self._inc_error('pat data less than header definition')
           else:           
              raise Exception('Wrong table id on 0x0 pid')
            
        elif pid == 0x11:
            if table_id == 0x42: # SDT actual TS
                if self.sdt is None:
                     self.sdt = {}
                (sdt_header,iii) = self._parse_headers(table_data,SDT_TABLE_BITS,SDT_TABLE_NAMES)
                if iii:
                    sdt_services = table_data[iii:sdt_header['section_length']+3-4]# -4 CRC32
                    while(sdt_services):
                        (table_service,iii) = self._parse_headers(sdt_services,SDT_TABLE_SERVICE_BITS,SDT_TABLE_SERVICE_NAMES)
                        if iii:
                            descriptors_loop = sdt_services[iii:iii+table_service['descriptors_loop_length']]
                            sdt_services = sdt_services[iii+table_service['descriptors_loop_length']:]
                            
                            parsed_service_descriptions = self._parse_descriptors(self._parse_descriptors_loop(descriptors_loop))
                            
                            self.sdt[table_service['service_id']] = None
                            if 'service_name' in parsed_service_descriptions.keys():
                                self.sdt[table_service['service_id']] = parsed_service_descriptions
                        else:
                            self._inc_error('sdt services error')
                            sdt_services = ()
                else:
                    self._inc_error('sdt data less than header definition')
                      
    
    def _check_eit_schedule_fullness_for_this_service_id(self,service_id):        
        try:       
           for table_id in range(0x50,self.eit[service_id][0x50][0]['table']['last_table_id']+1):               
               for segment_id in range(0,int(self.eit[service_id][table_id][0]['table']['last_section_number'] / 8) + 1):
                   for i in range(8*segment_id,self.eit[service_id][table_id][8*segment_id]['table']['segment_last_section_number']+1):
                       self.eit[service_id][table_id][i]['events']
        except KeyError:
          return False
        return True
    
    def is_some_eit_present(self):
        return not(self.pat is None or self.sdt is None or self.eit is None or list(x for x in self.pat.keys() if x not in self.eit.keys()))
    
    def check_eit_schedule_fullness(self):
        if not self.is_some_eit_present():
            return False
        if self._errors:
            return False
        for service_id in self.pat.keys():            
            if not self._check_eit_schedule_fullness_for_this_service_id(service_id):
                return False        
        return True
    
    def return_eit_events(self):
        if not self.is_some_eit_present():
            self._inc_error ("No EIT at all!")
            return {}
        if not self.check_eit_schedule_fullness():
            self._inc_error ("Warn: EIT is not fully received!")
        result = {}
        for service_id in self.pat.keys():            
            name = hex(service_id)
            if service_id in self.sdt.keys() and 'service_name' in self.sdt[service_id].keys():
                name = self.sdt[service_id]['service_name']
                
            result[name] = []
            for table_id in self.eit[service_id].keys():
                for section_id in self.eit[service_id][table_id].keys():
                    for event in self.eit[service_id][table_id][section_id]['events'].values():
                        #TODO: strip spaces, check for emptyness
                      if (list(x for x in ('start_time','duration','event_name','event_text') if not x in event.keys())):
                          continue
                      start_time_unix = self._start_time_to_unix(event['start_time'])
                      end_time_unix = self._duration_to_seconds(event['duration']) + start_time_unix
                        
                      e = {'start':start_time_unix,'stop':end_time_unix}
                      if 'extended_text' in event.keys() and event['extended_text']:                          
                          e['title'] = u"%s %s" % (event['event_name'],event['event_text'])
                          e['desc'] = event['extended_text']
                      else:                         
                          e['title'] = event['event_name']
                          e['desc'] = event['event_text']
                      result[name].append(e)
            result[name] = sorted(result[name],key = lambda x: x['start'])
        return result


