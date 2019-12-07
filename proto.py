#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' O codigo que puxava os arquivos ficou um pouco bagunçado
entao resolvi refazer como um arquivo separado
'''

import ftplib as ftp

SINASC = 'dissemin/publicos/SINASC'
SIM = 'dissemin/publicos/SIM'
SIHSUS = 'dissemin/publicos/SIHSUS'
SINAN = 'dissemin/publicos/SINAN'

class DataSus:

    def __init__(self, PAGE = 'ftp.datasus.gov.br'):
        self.page = ftp.FTP(PAGE); self.page.login()
        self.log = []

    ''' Esse loop verifica a os elementos iterados. É uma forma
    de puxar apenas os arquivos de database
    '''
    def verify(self, arg):
        for i in arg:
            if i.endswith(('.dbc', '.DBC', '.DBF', '.dbf')) == False:
                try:
                    self.page.cwd(i)
                    self.verify(self.page.nlst())
                    self.page.cwd('..')
                except:
                     pass
            elif i.endswith(('.dbc', '.DBC', '.DBF', '.dbf')):
                self.log.append('{},{}{},{}\n'.format(self.page.size(i),self.page.host,self.page.pwd(),i))

            else:
                print ('Se chegou aqui é pq deu muito errado.')

    ''' Aqui eu escolhi criar uma função pra cada banco que foi mencionado,
    porem a funcao "verify" é capaz de pegar todos os arquivos database
    dentro da raiz '/' desde que seja alterado o timeout, visto que o loop
    fecharia a conexao no momento em que copia pra self.log
    '''
    def write_sinasc(self):
        self.sinasc = open('sinasc.csv','w+')
        self.page.cwd(SINASC)
        self.verify(self.page.nlst())
        for x in self.log:
            self.sinasc.write(x)
        self.sinasc.close()

    def write_sihsus(self):
        self.sihsus = open('sihsus.csv','w+')
        self.page.cwd(SIHSUS)
        self.verify(self.page.nlst())
        for x in self.log:
            self.sihsus.write(x)
        self.sihsus.close()

    def write_sim(self):
        self.sim = open('sim.csv','w+')
        self.page.cwd(SIM)
        self.verify(self.page.nlst())
        for x in self.log:
            self.sim.write(x)
        self.sim.close()

    def write_sinan(self):
        self.sinan = open('sinan.csv','w+')
        self.page.cwd(SINAN)
        self.verify(self.page.nlst())
        for x in self.log:
            self.sinan.write(x)
        self.sinan.close()

# os testes realizados foram bem sucedidos

#a = DataSus()
#a.write_sihsus()

#b = DataSus()
#b.write_sim()

#t = DataSus()
#t.write_sinasc()

#c = DataSus()
#c.write_sinan()
