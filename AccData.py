from DataKernel import *

class AcceData(DataKernel):
    """load Acceleration data from remote data server"""

    def __init__(self, server, db, uid, pwd, paras, path, sensType):
        '''Init some argvs
        '''

        DataKernel.__init__(self, server, db, uid, pwd, paras, path)

        self.sensType = sensType

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

if __name__ == '__main__':

    from ConfigParser import SafeConfigParser
    import time
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

    t = AcceData(server, db, uid, pwd, paras, path, 'Acce')
    t.DTInit(dtStart, dtEnd)
    # cProfile.run('t.GetData(chStart, chEnd)')
    t.GetData(chStart, chEnd)

    print 'Finished in %6.3fs' % time.clock()
    print 'The program will exit in 5s ...'
    time.sleep(5)
