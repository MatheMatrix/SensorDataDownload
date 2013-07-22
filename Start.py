# -*- coding: gbk -*-

from AccDataDownloader import *
from ObliDataDownloader import *
from StrainDataDownloader import *

import cmd
import sys

class DataDownloader(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = 'DataDownloader>'
        self.intro = '''DataDownloader ʹ������˵��:

 help    ��ѯ����
 Acc     �����ݿ����ؼ��ٶ�����
 Obli    �����ݿ��������������
 Starin  �����ݿ����ع�դ��������
 Exit    �˳�����

 ע:�������ص����ݵ����䡢ͨ�������Config.ini����
            '''

    def help_Exit(self):
        print 'Quit this program'
    def do_Exit(self, line):
        sys.exit()

    def help_Acc(self):
        print 'download Acceleration data from data server'
        print "please config the parameters in 'Config.ini' "
    def do_Acc(self, para):
        AccDataDownloader()

    def help_Obli(self):
        print 'download Obliquitous data from data server'
        print "please config the parameters in 'Config.ini' "
    def do_Obli(self, para):
        ObliDataDownloader()

    def help_Strain(self):
        print 'download Strain data from data server'
        print "please config the parameters in 'Config.ini' "
    def do_Strain(self, para):
        StrainDataDownloader()

if __name__ == '__main__':
    downloader = DataDownloader()
    downloader.cmdloop()
