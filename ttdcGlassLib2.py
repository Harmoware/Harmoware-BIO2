import pandas as pd
from operaDB2 import OperaDB2
from datetime import datetime, timezone, timedelta

class TtdcGlass:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'piglasses'
        self.StartTime = 'rec_time'
        self.ElpTime = 'serialtime'

    def get_DataFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )

    def get_TimeStamp( self, df ):
        JST = timezone(timedelta(hours=+9), 'JST')
        start_datetime = pd.Timestamp(df[self.StartTime][0].replace(tzinfo=JST))
        sTimeStamp = start_datetime.timestamp()
        sSerialTime = df[self.ElpTime][0]

        return( (df[self.ElpTime] - sSerialTime + sTimeStamp)*1000 )

class TtdcGlassHr:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'glass_hrs'

    def get_TimeStampFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )
