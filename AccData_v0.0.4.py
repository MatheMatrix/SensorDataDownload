import pyodbc
import struct
import datetime
import os
import time

# import thread

class AcceData():
    """load Acceleration data from remote data server"""

    def __init__(self, server, db, uid, pwd, paras,path):
        '''Init some argvs
        '''

        self.server = server
        self.db = db
        self.uid = uid
        self.pwd = pwd
        self.paras = paras
        self.path = path

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
            "'Acceleration{0}' and 'Acceleration{1}'".format(start, end)
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

    def GetChdata(self, start, end, table):
        '''Get data from data and classify into channels

        start and end should be channel ID
        table is a table's name
        return a dict like {'ch1': 'ch2': , ... }
        '''

        conn = self.Connect()
        conn.timeout = 100
        cursor = conn.cursor()
        data = {}

        dtTableStart, dtTableEnd = self.TableTime(table, start)
        for i in range(start, end + 1): # end + 1 to execute to the end channel

            print 'Getting data of ch {0} in table: {1}'.format(str(i), table)

            data['ch' + str(i)] = []
            cmd = "select [Data] from [RiverBai].[dbo].[{0}]".format(table) + \
                " where [ChannelID] = {0} and ([DateTime] between ".format(i) + \
                "'{0}' and '{1}') order by [ID] asc".format(self.dtStartSQL, self.dtEndSQL)
            try:
                cursor.execute(cmd)
            except puodbc.OperationalError:
                print 'SQL Server quary timeout, the sql server may have some problems'

            row = cursor.fetchone()
            while row:
                data['ch' + str(i)].extend(self.Format(row[0], 'ch' + str(i)))
                row = cursor.fetchone()

        conn.close()

        return data

            
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
            " where [ChannelID] = {0} and ([DateTime] between ".format(ch) + \
            "'{0}' and '{1}') order by [ID] asc".format(dtStart, dtEnd)
        cursor.execute(cmd)
        row = cursor.fetchall()
        if row == []:
            dtTableStart = table[-8:] + '000000000'
            dtTableStart = datetime.datetime.strptime(dtTableStart, '%Y%m%d%H%M%S%f')
        else:
            dtTableStart = row[0][0]

        cmd = "select TOP 1 [DateTime] from [RiverBai].[dbo].[{0}]".format(table) + \
            " where [ChannelID] = {0} and ([DateTime] between ".format(ch) + \
            "'{0}' and '{1}') order by [ID] desc".format(dtStart, dtEnd)
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

    def Format(self, row, ch):
        '''To Format and calculate datas (from bytes to short)

        ch should be a string like: ch1
        row should be a bytearray
        '''

        row = struct.unpack('<' + 'h' * 200, buffer(row))

        row = list(row)

        for i in range(len(row)):
            row[i]= float(row[i]) * 10 / int('7fff', 16) / self.paras[ch] / 9.8

        return row

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
            table = 'Acceleration' + date
            
            print 'Now Writing data to ' + self.path + ch + self.dtTable[table][0].strftime('_%Y.%m.%d.%H.%M.%S') + self.dtTable[table][1].strftime('_%Y.%m.%d.%H.%M.%S.txt') + ' ...'
            
            # thread.start_new_thread(self.WriteCh, (data, path, key, ch, table))
            with open(self.path + ch + \
                self.dtTable[table][0].strftime('_%Y.%m.%d.%H.%M.%S') + \
                self.dtTable[table][1].strftime('_%Y.%m.%d.%H.%M.%S.txt'), 'w') as f:

                data[ch] = [str(i) + '\n' for i in data[ch]]
                f.writelines(data[ch])



    # def WriteCh(self, data, path, key, ch, table):
    #     '''to use muti-threading
    #     '''

    #     with open(path + ch + \
    #         self.dtTable[table][0].strftime('_%Y.%m.%d.%H.%M.%S') + \
    #         self.dtTable[table][1].strftime('_%Y.%m.%d.%H.%M.%S.txt'), 'w') as f:
    #         f.write(str(data[key][ch])[1:-1])

    #     thread.exit()


if __name__ == '__main__':

    from ConfigParser import SafeConfigParser
    # import cProfile

    time.clock()

    scp = SafeConfigParser()
    scp.read('Config.ini')

    server = scp.get('General', 'server')
    db = scp.get('General', 'db')
    uid = scp.get('General', 'uid')
    pwd = scp.get('General', 'pwd')
    path = scp.get('General', 'path')

    dtStart = scp.get('AcceCollect', 'StartDatetime')
    dtEnd = scp.get('AcceCollect', 'EndDatetime')
    chStart = int( scp.get('AcceCollect', 'StartChannel') )
    chEnd = int( scp.get('AcceCollect', 'EndChannel') )

    paras = {}
    chs = scp.options('AcceSensitivity')

    for ch in chs:
        paras[ch] = float(scp.get('AcceSensitivity', ch))

    t = AcceData(server, db, uid, pwd, paras, path)
    t.DTInit(dtStart, dtEnd)
    # cProfile.run('t.GetData(chStart, chEnd)')
    t.GetData(chStart, chEnd)

    print 'Finished in %6.3fs' % time.clock()
    print 'The program will exit in 5s ...'
    time.sleep(5)
