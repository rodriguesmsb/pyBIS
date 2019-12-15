#!/usr/bin/env python3
# -*- coding:utf8 -*-

# carrega nossa biblioteca fantastica para ftp
import ftplib as ftp

# cria a classe para ser instanciada
class Datasus:

    # aqui é montado o esqueleto da classe
    def __init__(self, banco, PAGINA = 'ftp.datasus.gov.br',\
            PUBLICO = '/dissemin/publicos'):

        self.log = {}
        self.log['Data'], self.log['Horario'], self.log['Tamanho'],\
        self.log['Nome'], self.log['Endereco'] = [], [], [], [], []

        '''
        usei 2 underlines caso a funcao seja usando no shell python
        pra nao ficar feio quando bater tab mais de uma vez e ficar
        um monte de coisa a mostra
        '''
        self.__log = []
        self.__pagina = ftp.FTP(PAGINA)
        self.__pagina.login()
        self.__pagina.cwd(PUBLICO)
        self.__banco = banco

    '''
    essa funcao *TENTA* carregar qualquer banco, afinal esse codigo
    espera apenas o nome do banco valido dentro do 'publicos'
    '''
    def carrega(self):
        try:

            self.__pagina.cwd(self.__banco)
            self.__pagina.dir(self.__log.append)
            self.__magica(self.__log)

        except ftp.error_perm:
            print ('diretorio invalido')

    '''
    essa função faz a magica buscando os bancos. diferente da forma
    anterior, essa funcao carrega tudo e depois espera ser chamado...
    a forma anterior era muito ruim
    '''

    def __magica(self, lista):

        for i in lista:
            if i.split()[3].endswith(('.dbc','.DBC','.DBF','.dbf')):
                self.log['Data'].append(i.split()[0]),\
                self.log['Horario'].append(i.split()[1]),\
                self.log['Tamanho'].append(i.split()[2]),\
                self.log['Nome'].append(i.split()[3]),\
                self.log['Endereco'].append(self.__pagina.pwd())

            elif i.split()[3].endswith(('.dbc','.DBC','.DBF','.dbf')) == False:
                try:
                    self.__log = []
                    self.__pagina.cwd(i.split()[3])
                    self.__pagina.dir(self.__log.append)
                    self.__magica(self.__log)
                    self.__pagina.cwd('..')
                except:
                    pass
            else:
                break

    # essa funcao grava em txt com o nome que o usuario escolher
    def grava(self, nome):
        if len(a.log['Data']) == 0:
            print ('Precisa carregar um banco')
        else:
            grava = open('{}.txt'.format(nome), 'w+')
            for x,i in self.log.items():
                grava.write('{},{}'.format(x, i))
            grava.close()
