import pandas as pd
from operaDB2 import OperaDB2

class GPS:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'gpses'

    def get_TimeStampFromLogID( self, log_id ):
        return self.opera.get_DataFromLogID( self.tables, log_id )

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )
