import pandas as pd
import cv2
from operaDB2 import OperaDB2
from datetime import datetime, timezone, timedelta

class OperaImage:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_videofile'
        self.start_datetime = 0
        self.frontfile = 'picamera2'
        self.eyefile = 'picamera1'

    def get_VideoFromTripList(self, DataList):
        df = {}
        for s in DataList['sensor']:
            JST = timezone(timedelta(hours=+9), 'JST')
            if( s['name'] == self.frontfile ):
                df['front'] = cv2.VideoCapture(s['file_path'])

            if( s['name'] == self.eyefile ):
                df['eye'] = cv2.VideoCapture(s['file_path'])

        return df

    def get_VideoLists(self, df_tripLists, l):
        #Get video file name
        #print(df_tripLists['file_path'][l])
        #Set log id
        return df_tripLists['file_path'][l]

    def get_VideoCapture(self, df_tripLists, l):
        log_date = self.opera.get_TripTime(df_tripLists, l)
        JST = timezone(timedelta(hours=+9), 'JST')
        self.start_datetime = pd.Timestamp(log_date.replace(tzinfo=JST))
        videoLists = self.get_VideoLists(df_tripLists, l)
        print(videoLists)
        print('log date: ' + str(self.start_datetime))

        videoCap = cv2.VideoCapture(videoLists)
        print('open video file : ' + str(videoCap.isOpened() ) )
        return videoCap

    def read(self, cap):
        ret, frame = cap.read()
        if( ret == True ):
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return ret, frame

    def get_TimeStamp(self, cap):
        sTimeStamp = (self.start_datetime).timestamp()
        eTimeStamp = (sTimeStamp + self.get_SizeFrameCount( cap )/self.get_FPS( cap ))
        cTimeStamp = (sTimeStamp + self.get_FrameCount( cap )/self.get_FPS(cap))
        #print(sTimeStamp )
        #print( self.get_FrameCount( cap )/self.get_FPS(cap)*1000*1000 )
        return sTimeStamp, cTimeStamp, eTimeStamp

    def get_Width(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))

    def get_Height(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    def get_FPS(self, cap):
        return float(cap.get(cv2.CAP_PROP_FPS))

    def get_SizeFrameCount(self, cap):
        return int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    def get_FrameCount(self, cap):
        return int(cap.get(cv2.CAP_PROP_POS_FRAMES))

    def set_FrameCount(self, cap, c):
        return int(cap.set(cv2.CAP_PROP_POS_FRAMES,c))

    #未実装
    #cv2.CAP_PROP_POS_AVI_RATIO 現在のフレームの相対的な位置 #値がおかしいので実装してない
    #cv2.CAP_PROP_FOURCC コーデックを表す4文字コード
    #cv2.CAP_PROP_FORMAT retrieve() によって返されるMat オブジェクトのフォーマット
    #cv2.CAP_PROP_MODE 現在のキャプチャモードを表す、バックエンド固有の値
    #cv2.CAP_PROP_BRIGHTNESS 画像の明るさ（カメラの場合のみ）
    #cv2.CAP_PROP_CONTRAST 画像のコントラスト（カメラの場合のみ）
    #cv2.CAP_PROP_SATURATION 画像の彩度（カメラの場合のみ）
    #cv2.CAP_PROP_HUE 画像の色相（カメラの場合のみ）
    #cv2.CAP_PROP_GAIN 画像のゲイン（カメラの場合のみ）
    #cv2.CAP_PROP_EXPOSURE 露出（カメラの場合のみ）
    #cv2.CAP_PROP_CONVERT_RGB 画像がRGBに変換されるか否かを表す、ブール値のフラグ
