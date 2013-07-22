from AccData import *

from ConfigParser import SafeConfigParser
import time

# import cProfil

def AccDataDownloader():
    '''Download Acceleration data to disk
    '''

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
