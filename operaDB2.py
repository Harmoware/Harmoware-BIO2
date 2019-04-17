import xml.etree.ElementTree as ET
import mysql.connector
import pandas as pd
import time
from datetime import datetime, timezone, timedelta

class OperaDB2:
    def __init__(self,file,dbname):
        _host, _port, db, s3, user, pa = self.getDB_XML(file,dbname)
        print('connect to opera2 server')
        self.conn = mysql.connector.connect(
            user=user,
            password=pa,
            host=_host,
            port=_port,
            database=db)
        self.cur = self.conn.cursor()
        self.s3dir = s3
        self.triptable = 'data_logs'

    def getDB_XML(self,file,dbname):
        db_tree = ET.ElementTree(file=file)
        db_root = db_tree.getroot()
        for db_info in db_root.findall('.//'+dbname):
            host = db_info.find('host').text
            port = db_info.find('port').text
            sql = db_info.find('sql').text
            s3 = db_info.find('s3').text
            user = db_info.find('user').text
            _pass = db_info.find('pass').text

        return host, port, sql, s3, user, _pass

    def exe_query(self, query):
        #print( query )
        try:
            self.cur.execute(query)

        except Exception as e:
        #
            self.conn.rollback()
            print( e.pgerror )

    def get_DataFrame(self, query):
        self.exe_query( query )

        column_names = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(columns=column_names)

        for row in self.cur.fetchall():
            df_ = pd.Series(list(row), index=column_names)
            df = df.append(df_, ignore_index=True)

        return column_names, df

    def get_TripListFromTime(self, day, tstart='00:00:00.0000', duration='23:59:59.0000'):
        desire_datetime = day + ' ' + tstart
        desire_length = pd.Timedelta(duration)
        start_datetime = pd.Timestamp(desire_datetime)
        stop_datetime = start_datetime + desire_length

        mysql_datetime = int(time.mktime(start_datetime.timetuple()))*1000
        mysql_stoptime = int(time.mktime(stop_datetime.timetuple()))*1000

        #Create Query
        query_table = 'SELECT * FROM ' + self.triptable
        datetime_conditions = ' WHERE log_date > \'' + str(start_datetime) + '\' AND ' + 'log_date < \'' + str(stop_datetime) + '\''
        query = query_table + datetime_conditions + ';'

        print( query )
        column_names, df_tripLists = self.get_DataFrame( query )
        #print(df_tripLists)
        return df_tripLists

    def get_DataFromLogID(self, tables, log_id):
        #Create Query
        query = 'SELECT * FROM ' + tables
        logid_conditions = ' WHERE data_log_id = ' + str(log_id)
        test_opt = ''
        #test_opt = ' ORDER BY save_time ASC LIMIT 100'
        query = query + logid_conditions + test_opt + ';'
        #query = query + datetime_conditions + ';'
        print(query)

        column_names, df = self.get_DataFrame( query )
        return column_names, df

    def get_TripTime(self, df_tripLists, l):
        start_datetime = df_tripLists['log_date'][l]
        return start_datetime
        #stop_datetime = start_datetime + df_tripLists['length'][l]
        #return start_datetime, stop_datetime

    def get_DataFromTrip(self, tables, tripLists, l, offset=0):
        start_datetime, stop_datetime = self.get_TripTime(tripLists, l)

        #Create Query
        query = 'SELECT * FROM ' + tables
        datetime_conditions = ' WHERE time > ' + str(start_datetime+offset*1000) + ' AND ' + 'time < ' + str(stop_datetime)
        #test_opt = ' ORDER BY time ASC LIMIT 100'
        #query = query_omroncamera + datetime_conditions + test_opt + ';'
        query = query + datetime_conditions + ';'
        #print(query)

        column_names, df = self.get_DataFrame( query )
        return column_names, df

    def get_DataFromTime(self, tables, timeindex, tstart, tend, list_id):
        #Create Query
        query = 'SELECT * FROM ' + tables
        datetime_conditions = timeindex + ' > ' + str(tstart) + ' AND ' + timeindex + ' < ' + str(tend)

        datalogid_conditions = ''
        for id in list_id:
            if( len(datalogid_conditions) == 0 ):
                #initial
                datalogid_conditions = 'data_log_id = \'' + str(id) + '\''
            else:
                datalogid_conditions = datalogid_conditions + ' OR data_log_id = \'' + str(id) + '\''

        query = query + ' WHERE (' + datetime_conditions + ' ) AND ( ' + datalogid_conditions + ' );'
        print(query)
        column_names, df = self.get_DataFrame( query )
        return column_names, df

    def get_DataFromDuration(self, tables, timeindex, tstart, duration, list_id):
        #Create Query
        tend = tstart + duration
        #print(query)
        column_names, df = self.get_DataFromTime( tables, timeindex, tstart, tend, list_id)
        return column_names, df

    def get_DataLogIDFromTime(self, sensor, tstart, tend, oc_id):
        JST = timezone(timedelta(hours=+9), 'JST')
        start_datetime = datetime.fromtimestamp(tstart, JST)
        end_datetime = datetime.fromtimestamp(tend, JST)

        query_table = 'SELECT * FROM ' + self.triptable
        datetime_conditions = 'log_date > \'' + str(start_datetime) + '\' AND ' + 'log_date < \'' + str(end_datetime) + '\''
        sensor_conditions = 'sensor_name = \'' + str(sensor) + '\''
        oc_conditions = 'oc_id = \'' + str(oc_id) + '\''
        query = query_table + ' WHERE ' + datetime_conditions + ' AND ' + sensor_conditions + ' AND ' + oc_conditions + ';'

        print( query )
        column_names, df_tripLists = self.get_DataFrame( query )
        #print(df_tripLists)
        return df_tripLists
