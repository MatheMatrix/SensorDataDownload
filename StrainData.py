from DataKernel import *
from ctypes import *

class StrainData(DataKernel):
    """load Acceleration data from remote data server"""

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

        print('Getting data in table: {0}'.format(table))

        conn = self.Connect()
        conn.timeout = 100
        cursor = conn.cursor()
        data = {}

        dtTableStart, dtTableEnd = self.TableTime(table)

        data = {}.fromkeys(['ch' + str(i) for i in range(start, end + 1)])
        for i in data:
            data[i] = []
            
        cmd = "select [WaveLength] from [RiverBai].[dbo].[{0}]".format(table.decode('utf-8')) + \
            " where [DateTime] between " + \
            "'{0}' and '{1}' order by [ID] asc".format(self.dtStartSQL, self.dtEndSQL)
        try:
            cursor.execute(cmd)
        except pyodbc.OperationalError:
            print('SQL Server quary timeout, the sql server may have some problems')

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

        row = struct.unpack('>192H', buffer(row))

        row = list(row)
        for i in range(len(row)):
            row[i] = float(row[i]) / 1000 + 1510

        return row

    def Divide(self, row):
        '''Divide data of Strain's different channels

        Input data should be a list of numbers like: 
        [1510.452, 1505.248, 1508.025, 1510.203, 1512.271, ... ]
        the return will like:
        {   'ch1': [21, 35, 74, ... ]
            'ch2': [21, 32, 43, ... ]
            ...
        }
        '''

        # DivideTable = {1: 'ch1', 2: 'ch2', 3: 'ch3', 4: 'ch4', 5: 'ch5', 6: 'ch6', 7: 'ch7', 8: 'ch8', 9: 'ch9', 10: 'ch10', 11: 'ch11', 12: 'ch12', 13: 'ch13', 14: 'ch14', 15: 'ch15', 16: 'ch16', 17: 'ch1', 18: 'ch2', 19: 'ch3', 20: 'ch4', 21: 'ch5', 22: 'ch6', 23: 'ch7', 24: 'ch8', 25: 'ch9', 26: 'ch10', 27: 'ch11', 28: 'ch12', 29: 'ch17', 30: 'ch18', 31: 'ch19', 32: 'ch20', 33: 'ch1', 34: 'ch2', 35: 'ch3', 36: 'ch4', 37: 'ch5', 38: 'ch6', 39: 'ch7', 40: 'ch8', 41: 'ch9', 42: 'ch10', 43: 'ch11', 44: 'ch12', 45: 'ch21', 46: 'ch22', 47: 'ch23', 48: 'ch24', 49: 'ch1', 50: 'ch2', 51: 'ch3', 52: 'ch4', 53: 'ch5', 54: 'ch6', 55: 'ch7', 56: 'ch8', 57: 'ch9', 58: 'ch10', 59: 'ch11', 60: 'ch12', 61: 'ch25', 62: 'ch26', 63: 'ch27', 64: 'ch28', 65: 'ch1', 66: 'ch2', 67: 'ch3', 68: 'ch4', 69: 'ch5', 70: 'ch6', 71: 'ch7', 72: 'ch8', 73: 'ch9', 74: 'ch10', 75: 'ch11', 76: 'ch12', 77: 'ch29', 78: 'ch30', 79: 'ch31', 80: 'ch32', 81: 'ch1', 82: 'ch2', 83: 'ch3', 84: 'ch4', 85: 'ch5', 86: 'ch6', 87: 'ch7', 88: 'ch8', 89: 'ch9', 90: 'ch10', 91: 'ch11', 92: 'ch12', 93: 'ch33', 94: 'ch34', 95: 'ch35', 96: 'ch36', 97: 'ch1', 98: 'ch2', 99: 'ch3', 100: 'ch4', 101: 'ch5', 102: 'ch6', 103: 'ch7', 104: 'ch8', 105: 'ch9', 106: 'ch10', 107: 'ch11', 108: 'ch12', 109: 'ch37', 110: 'ch38', 111: 'ch39', 112: 'ch40', 113: 'ch1', 114: 'ch2', 115: 'ch3', 116: 'ch4', 117: 'ch5', 118: 'ch6', 119: 'ch7', 120: 'ch8', 121: 'ch9', 122: 'ch10', 123: 'ch11', 124: 'ch12', 125: 'ch41', 126: 'ch42', 127: 'ch43', 128: 'ch44', 129: 'ch1', 130: 'ch2', 131: 'ch3', 132: 'ch4', 133: 'ch5', 134: 'ch6', 135: 'ch7', 136: 'ch8', 137: 'ch9', 138: 'ch10', 139: 'ch11', 140: 'ch12', 141: 'ch45', 142: 'ch46', 143: 'ch47', 144: 'ch48', 145: 'ch1', 146: 'ch2', 147: 'ch3', 148: 'ch4', 149: 'ch5', 150: 'ch6', 151: 'ch7', 152: 'ch8', 153: 'ch9', 154: 'ch10', 155: 'ch11', 156: 'ch12', 157: 'ch49', 158: 'ch50', 159: 'ch51', 160: 'ch52', 161: 'ch1', 162: 'ch2', 163: 'ch3', 164: 'ch4', 165: 'ch5', 166: 'ch6', 167: 'ch7', 168: 'ch8', 169: 'ch9', 170: 'ch10', 171: 'ch11', 172: 'ch12', 173: 'ch53', 174: 'ch54', 175: 'ch55', 176: 'ch56', 177: 'ch1', 178: 'ch2', 179: 'ch3', 180: 'ch4', 181: 'ch5', 182: 'ch6', 183: 'ch7', 184: 'ch8', 185: 'ch9', 186: 'ch10', 187: 'ch11', 188: 'ch12', 189: 'ch57', 190: 'ch58', 191: 'ch59', 192: 'ch60'}
        DivideTable = {1: 'ch1', 2: 'ch2', 3: 'ch3', 4: 'ch4', 5: 'ch5', 6: 'ch6', 7: 'ch7', 8: 'ch8', 9: 'ch9', 10: 'ch10', 11: 'ch11', 12: 'ch12', 13: 'ch13', 14: 'ch25', 15: 'ch37', 16: 'ch49', 17: 'ch1', 18: 'ch2', 19: 'ch3', 20: 'ch4', 21: 'ch5', 22: 'ch6', 23: 'ch7', 24: 'ch8', 25: 'ch9', 26: 'ch10', 27: 'ch11', 28: 'ch12', 29: 'ch14', 30: 'ch26', 31: 'ch38', 32: 'ch50', 33: 'ch1', 34: 'ch2', 35: 'ch3', 36: 'ch4', 37: 'ch5', 38: 'ch6', 39: 'ch7', 40: 'ch8', 41: 'ch9', 42: 'ch10', 43: 'ch11', 44: 'ch12', 45: 'ch15', 46: 'ch27', 47: 'ch39', 48: 'ch51', 49: 'ch1', 50: 'ch2', 51: 'ch3', 52: 'ch4', 53: 'ch5', 54: 'ch6', 55: 'ch7', 56: 'ch8', 57: 'ch9', 58: 'ch10', 59: 'ch11', 60: 'ch12', 61: 'ch16', 62: 'ch28', 63: 'ch40', 64: 'ch52', 65: 'ch1', 66: 'ch2', 67: 'ch3', 68: 'ch4', 69: 'ch5', 70: 'ch6', 71: 'ch7', 72: 'ch8', 73: 'ch9', 74: 'ch10', 75: 'ch11', 76: 'ch12', 77: 'ch17', 78: 'ch29', 79: 'ch41', 80: 'ch53', 81: 'ch1', 82: 'ch2', 83: 'ch3', 84: 'ch4', 85: 'ch5', 86: 'ch6', 87: 'ch7', 88: 'ch8', 89: 'ch9', 90: 'ch10', 91: 'ch11', 92: 'ch12', 93: 'ch18', 94: 'ch30', 95: 'ch42', 96: 'ch54', 97: 'ch1', 98: 'ch2', 99: 'ch3', 100: 'ch4', 101: 'ch5', 102: 'ch6', 103: 'ch7', 104: 'ch8', 105: 'ch9', 106: 'ch10', 107: 'ch11', 108: 'ch12', 109: 'ch19', 110: 'ch31', 111: 'ch43', 112: 'ch55', 113: 'ch1', 114: 'ch2', 115: 'ch3', 116: 'ch4', 117: 'ch5', 118: 'ch6', 119: 'ch7', 120: 'ch8', 121: 'ch9', 122: 'ch10', 123: 'ch11', 124: 'ch12', 125: 'ch20', 126: 'ch32', 127: 'ch44', 128: 'ch56', 129: 'ch1', 130: 'ch2', 131: 'ch3', 132: 'ch4', 133: 'ch5', 134: 'ch6', 135: 'ch7', 136: 'ch8', 137: 'ch9', 138: 'ch10', 139: 'ch11', 140: 'ch12', 141: 'ch21', 142: 'ch33', 143: 'ch45', 144: 'ch57', 145: 'ch1', 146: 'ch2', 147: 'ch3', 148: 'ch4', 149: 'ch5', 150: 'ch6', 151: 'ch7', 152: 'ch8', 153: 'ch9', 154: 'ch10', 155: 'ch11', 156: 'ch12', 157: 'ch22', 158: 'ch34', 159: 'ch46', 160: 'ch58', 161: 'ch1', 162: 'ch2', 163: 'ch3', 164: 'ch4', 165: 'ch5', 166: 'ch6', 167: 'ch7', 168: 'ch8', 169: 'ch9', 170: 'ch10', 171: 'ch11', 172: 'ch12', 173: 'ch23', 174: 'ch35', 175: 'ch47', 176: 'ch59', 177: 'ch1', 178: 'ch2', 179: 'ch3', 180: 'ch4', 181: 'ch5', 182: 'ch6', 183: 'ch7', 184: 'ch8', 185: 'ch9', 186: 'ch10', 187: 'ch11', 188: 'ch12', 189: 'ch24', 190: 'ch36', 191: 'ch48', 192: 'ch60'}

        data = {}.fromkeys(['ch' + str(i) for i in range(1, 60 + 1)])
        for i in data:
            data[i] = []
        for i in range(len(row)):
            data[DivideTable[i + 1]].append(row[i])

        return data
