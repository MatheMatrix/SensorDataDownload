import pyodbc
import struct
import datetime
import os

# import thread

class DataKernel():
    """load Acceleration data from remote data server"""

    def __init__(self, server, db, uid, pwd, paras, path):
        '''Init some argvs
        '''

        self.server = server
        self.db = db
        self.uid = uid
        self.pwd = pwd
        self.paras = paras
        self.path = path

        self.tableType = {'Acce':'Acceleration', 'Obli':'Obliquitous', 'Strain':'Strain'}

        self.dtTable = {}

    def Connect(self):
        '''Build connection between server and pc
        '''

        constr = 'DRIVER={SQL Server};' + \
            'SERVER={0};DATABASE={1};UID={2};PWD={3}'.format(self.server, self.db, self.uid, self.pwd)
        conn = pyodbc.connect(constr)

        return conn

    def GetData(self, chStart, chEnd):
        '''Get datas with particular datetimes and channels

        dtStart, dtEnd should be standard datetime objects
        return data:
        data 's datastruct just like this:
        {   '20130312': {   'ch1':  [6401, 4353, 3841, ... ] ,
                            'ch2':  [6401, 4353, 3841, ... ] ,
                            ...
                        }
            '20130313': {   'ch1':  [6401, 4353, 3841, ... ] ,
                            'ch2':  [6401, 4353, 3841, ... ] ,
                            ...
                        }
            ........
        }
        '''


        exists = self.FindExist()

        for i in range(len(exists)):
            exists[i] = exists[i][0].encode('utf-8')

        data = {}
        for i in range(len(exists)):
            data[exists[i][-8:]] = self.GetChdata(chStart, chEnd, exists[i])
            self.Write(data[exists[i][-8:]], exists[i][-8:])
            del data[exists[i][-8:]]

        return 'OK'


    def FindExist(self):
        '''Find exist tables with particular dates

        Start, End: DateTime objects like datetime.datetime(2013, 3, 17, 20, 8, 51, 800)
        return like [Acceleration20130415, Acceleration20130416]
        '''

        print 'Find tables which exists in SQL server...'

        start = self.dtStart.strftime('%Y%m%d')
        end = self.dtEnd.strftime('%Y%m%d')

        conn = self.Connect()
        conn.timeout = 120
        cursor = conn.cursor()

        cmd = "select [name] from sys.tables where name between " + \
            "'{0}' and '{1}'".format(self.tableType[self.sensType] + start, self.tableType[self.sensType] + end)
        try:
            cursor.execute(cmd)
        except pyodbc.OperationalError:
            print 'SQL Server quary timeout, the sql server may have some problems'

        row = cursor.fetchone()
        result = []
        while row:
            result.append(row)
            row = cursor.fetchone()

        conn.close()
        
        return result

    def DTInit(self, start, end):
        '''Init start and end datetime into standard datetime object

        Input like : '2013, 2, 17, 5, 46, 82, 400'
        return a list like this: [start_datetime, end_datetime]
        which each element is standard datetime type'''

        format = '%Y, %m, %d, %H, %M, %S, %f'
        start = datetime.datetime.strptime(start, format)
        end = datetime.datetime.strptime(end, format)

        self.dtStart = start
        self.dtEnd = end

        return [start, end]

    def TableTime(self, table, ch):
        '''find Table's start and end time in the quary's period

        table should be table's name
        return's datastruct: [start, end] the start and end are datetime objects
        '''

        conn = self.Connect()
        cursor = conn.cursor()

        dtStart = self.dtStart.strftime('%Y-%m-%d %H:%M:%S.%f')[0:-6] + self.dtStart.strftime('%Y-%m-%d %H:%M:%S.%f')[-3:]
        dtEnd = self.dtEnd.strftime('%Y-%m-%d %H:%M:%S.%f')[0:-6] + self.dtEnd.strftime('%Y-%m-%d %H:%M:%S.%f')[-3:]

        self.dtStartSQL = dtStart
        self.dtEndSQL = dtEnd

        cmd = "select TOP 1 [DateTime] from [RiverBai].[dbo].[{0}]".format(table) + \
            " where [DateTime] between ".format(ch) + \
            "'{0}' and '{1}' order by [ID] asc".format(dtStart, dtEnd)
        cursor.execute(cmd)
        row = cursor.fetchall()
        if row == []:
            dtTableStart = table[-8:] + '000000000'
            dtTableStart = datetime.datetime.strptime(dtTableStart, '%Y%m%d%H%M%S%f')
        else:
            dtTableStart = row[0][0]

        cmd = "select TOP 1 [DateTime] from [RiverBai].[dbo].[{0}]".format(table) + \
            " where [DateTime] between ".format(ch) + \
            "'{0}' and '{1}' order by [ID] desc".format(dtStart, dtEnd)
        cursor.execute(cmd)
        row = cursor.fetchall()
        if row == []:
            dtTableEnd = table[-8:] + '235959999'
            dtTableEnd = datetime.datetime.strptime(dtTableEnd, '%Y%m%d%H%M%S%f')
        else:
            dtTableEnd = row[0][0]
        
        self.dtTable[table] = [dtTableStart, dtTableEnd]

        conn.close()

        return [dtTableStart, dtTableEnd]

    def Write(self, data, date):
        '''Write data to disk

        data 's datastruct just like this:
        {   'ch1':  [6401, 4353, 3841, ... ] ,
            'ch2':  [6401, 4353, 3841, ... ] ,
             ....
        }
        date should be like: 20130321
        '''

        chs = tuple(data.viewkeys())

        if os.path.exists(self.path):
            pass
        else:
            os.mkdir(self.path)

        for ch in chs:
            table = self.tableType[self.sensType] + date
            
            print 'Now Writing data to ' + self.path + self.sensType + '_' \
                + ch + self.dtTable[table][0].strftime('_%Y.%m.%d.%H.%M.%S') + self.dtTable[table][1].strftime('_%Y.%m.%d.%H.%M.%S.txt') + ' ...'
            
            # thread.start_new_thread(self.WriteCh, (data, path, key, ch, table))
            with open(self.path + self.sensType + '_' + ch + \
                self.dtTable[table][0].strftime('_%Y.%m.%d.%H.%M.%S') + \
                self.dtTable[table][1].strftime('_%Y.%m.%d.%H.%M.%S.txt'), 'w') as f:

                data[ch] = [str(i) + '\n' for i in data[ch]]
                f.writelines(data[ch])