import pandas as pd
import math
from operaDB2 import OperaDB2
from datetime import datetime, timezone, timedelta

class OmronEye:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'omrons'
        self.TimeIndex = 'save_time' #msec
        self.fps = 60

    def get_TimeStamp( self, df ):
        ts = df[self.TimeIndex]/1000 # msec to sec
        if( math.isnan(ts[0]) ):
            print('!WARNING: timestamp is NAN, return timestamp estimated from record time and fps')

            rec_date = df['rec_start_time'][0]
            print('rec_start_time : ' + str(rec_date))
            print('fps : ' + str(self.fps))

            JST = timezone(timedelta(hours=+9), 'JST')
            start_datetime = pd.Timestamp(rec_date.replace(tzinfo=JST))
            ts = df['frame_id'] - df['start_frame_id']
            ts = ts/self.fps
            ts = start_datetime.timestamp() + ts

        return ts

    def get_DataFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )

#    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
#        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )

    def get_DataFrameFromDuration(self, tstart, duration, id_list ):
        return self.opera.get_DataFromDuration( self.tables, self.TimeIndex, tstart*1000, duration*1000, id_list )
