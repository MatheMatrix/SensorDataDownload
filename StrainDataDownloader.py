from StrainData import *

from ConfigParser import SafeConfigParser
import time

# import cProfil

def StrainDataDownloader():
    '''Download Strain data to disk
    '''

    time.clock()

    scp = SafeConfigParser()
    scp.read('Config.ini')

    server = scp.get('General', 'server')
    db = scp.get('General', 'db')
    uid = scp.get('General', 'uid')
    pwd = scp.get('General', 'pwd')
    path = scp.get('General', 'path')

    dtStart = scp.get('StrainCollect', 'StartDatetime')
    dtEnd = scp.get('StrainCollect', 'EndDatetime')
    chStart = int( scp.get('StrainCollect', 'StartChannel') )
    chEnd = int( scp.get('StrainCollect', 'EndChannel') )

    t = StrainData(server, db, uid, pwd, path, 'Strain')
    t.DTInit(dtStart, dtEnd)
    # cProfile.run('t.GetData(chStart, chEnd)')
    t.GetData(chStart, chEnd)

    print('Finished in %6.3fs' % time.clock())