from DataKernel import *

class ObliData(DataKernel):
    """load Obliquitous data from remote data server"""

    def __init__(self, server, db, uid, pwd, path, sensType):
        '''Init some argvs
        '''

        DataKernel.__init__(self, server, db, uid, pwd, path)

        self.sensType = sensType

    def GetChdata(self, start, end, table):
        '''Get data from data and classify into channels

        start and end should be channel ID
        table is a table's name
        return a dict like 
        {   'ch1': [12, 23, 34, 45, ... ... ], 
            'ch2': [90. 89. 78, 67, ... ... ],
             ...
        }
        '''

        print 'Getting data in table: {0}'.format(table)

        conn = self.Connect()
        conn.timeout = 100
        cursor = conn.cursor()
        data = {}

        dtTableStart, dtTableEnd = self.TableTime(table)

        data = {}.fromkeys(['ch' + str(i) for i in range(start, end + 1)])
        for i in data:
            data[i] = []
            
        cmd = "select [Data] from [{0}}].[dbo].[{1}]".format(self.db, table) + \
            " where [DateTime] between " + \
            "'{0}' and '{1}' order by [ID] asc".format(self.dtStartSQL, self.dtEndSQL)

        try:
            cursor.execute(cmd)
        except pyodbc.OperationalError:
            print 'SQL Server quary timeout, the sql server may have some problems'

        row = cursor.fetchone()
        if row:
            row = self.Format( row[0] )

        while row:
            for i in data:
                data[i].extend( self.Divide(row)[i] )
            row = cursor.fetchone()
            if row:
                row = self.Format( row[0] )
        
        conn.close()

        return data

    def Format(self, row):
        '''To Format and calculate datas (from bytes to short)

        ch should be a string like: ch1
        row should be a bytearray
        return row likes: [6401, 4353, 3841, ... ]
        '''

        row = struct.unpack('<' + 'h' * 340, buffer(row))

        row = list(row)

        for i in range(len(row)):
            row[i]= float(row[i]) * 10 / int('7fff', 16)

        return row

    def Divide(self, row):
        '''Divide data of Obliquitous's different channels

        Input data should be a list of numbers like: 
        [1510.452, 1505.248, 1508.025, 1510.203, 1512.271, ... ]
        the return will like:
        {   'ch1': [21, 35, 74, ... ]
            'ch2': [21, 32, 43, ... ]
            ...
        }
        '''

        data = {}.fromkeys(['ch' + str(i) for i in range(1, 17 + 1)])
        for i in data:
            data[i] = []

        for i in range(len(row)):
            data['ch' + str(i % 17 + 1)].append(row[i])

        return data
