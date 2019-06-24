import pandas as pd
from operaDB2 import OperaDB2
from datetime import datetime, timezone, timedelta

class TtdcGlass:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.sensor_name = 'piglass'
        self.tables = 'piglasses'
        self.StartTime = 'rec_time'
        self.ElpTime = 'serialtime' #msec
        print('Sensor List: RedLEDRAW, InfraredRAW, GreenLED, Axis')
        self.sensors = [ 'RedLEDRAW', 'InfraredRAW', 'GreenLED', 'Axis' ]
        print('Choice sensor in sensor list')

    def get_DataFromDataList(self, DataList):
        df = {}
        column_names = {}
        for s in DataList['sensor']:
            if( s['name'] == self.sensor_name ):
                for ss in self.sensors:
                    if( ss != 'GreenLED'):
                        print(ss)
                        column_names[ss], df[ss] = self.get_DataFromLogIDandSensor( s['data_id'], ss )

        return column_names, df

    def get_DataFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )

    def get_DataFromLogIDandSensor( self, log_id, sensor ):
        sensor_cond = 'sensor = \'' + str(sensor) + '\''
        #sensor_cond = 'sensor = ' + str(sensor)
        print(sensor_cond)
        return self.opera.get_DataFromLogIDext( self.tables, log_id, sensor_cond )

    def get_TimeStamp( self, df ):
        JST = timezone(timedelta(hours=+9), 'JST')
        start_datetime = pd.Timestamp(df[self.StartTime][0].replace(tzinfo=JST))
        sTimeStamp = start_datetime.timestamp()
        sSerialTime = df[self.ElpTime][0]

        return (df[self.ElpTime] - sSerialTime)/1000 + sTimeStamp

    def decode_AxisValue( self, data ):
        ax = []
        ay = []
        az = []
        for d in data['value']:
            v = d.split(',')
            ax.append( v[0] )
            ay.append( v[1] )
            az.append( v[2] )

        return ax, ay, az

class TtdcGlassHr:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.sensor_name = 'piglass'
        self.tables = 'glass_hrs'
        self.StartTime = 'save_time'
        self.ElpTime = 'elp_time' #sec

    def get_DataFromDataList(self, DataList):
        for s in DataList['sensor']:
            if( s['name'] == self.sensor_name ):
                print(s)
                column_names, df = self.get_DataFromLogID( s['data_id'])

        return column_names, df

    def get_TimeStamp( self, df ):
        JST = timezone(timedelta(hours=+9), 'JST')
        start_datetime = pd.Timestamp(df[self.StartTime][0].replace(tzinfo=JST))
        sTimeStamp = start_datetime.timestamp()
        sElpTime = df[self.ElpTime][0]

        return( (df[self.ElpTime]*2 - sElpTime + sTimeStamp) ) # 2倍は暫定, 確認中

    def get_DataFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )
