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
        self.intro = '''DataDownloader 使用命令说明:

 help    查询函数
 Acc     从数据库下载加速度数据
 Obli    从数据库下载倾角仪数据
 Starin  从数据库下载光栅光纤数据
 Exit    退出程序

 注:配置下载的数据的区间、通道等请打开Config.ini配置
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
