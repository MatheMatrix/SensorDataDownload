from ObliData import *

from ConfigParser import SafeConfigParser
import time

# import cProfil

if __name__ == '__main__':

    time.clock()

    scp = SafeConfigParser()
    scp.read('Config.ini')

    server = scp.get('General', 'server')
    db = scp.get('General', 'db')
    uid = scp.get('General', 'uid')
    pwd = scp.get('General', 'pwd')
    path = scp.get('General', 'path')

    dtStart = scp.get('InclinometerCollect', 'StartDatetime')
    dtEnd = scp.get('InclinometerCollect', 'EndDatetime')
    chStart = int( scp.get('InclinometerCollect', 'StartChannel') )
    chEnd = int( scp.get('InclinometerCollect', 'EndChannel') )

    t = ObliData(server, db, uid, pwd, path, 'Obli')
    t.DTInit(dtStart, dtEnd)
    # cProfile.run('t.GetData(chStart, chEnd)')
    t.GetData(chStart, chEnd)

    print 'Finished in %6.3fs' % time.clock()
    # print 'The program will exit in 5s ...'
    # time.sleep(5)
