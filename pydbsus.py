#!/usr/bin/env python3
# -*- coding:utf8 -*-

#Load necessary libraries for the Datasus class
import ftplib as ftp
import re, time, sys
import pandas as pd
from tqdm import tqdm
from PyQt5 import QtCore, QtGui, QtWidgets
import _thread, threading

class Datasus:
    
    #Define the main structure of the class

    def __init__(self, banco=None, PAGINA = 'ftp.datasus.gov.br', \
            PUBLICO = '/dissemin/publicos'):

        """
        To start the class user must provide at least one valid name 
        present in the datasus (e.g to append others repo we just need
        to add a new PAGINA)
        """

        self.log = {}
        self.log['Data'], self.log['Horario'], self.log['Tamanho'], \
        self.log['Nome'], self.log['Ano'], \
        self.log['Endereco'] = [], [], [], [], [], []
        
        self.__log = []
        try:
            self.__pagina = ftp.FTP(PAGINA)
            self.__pagina.login()
            self.__pagina.cwd(PUBLICO)
            self.__banco = banco
        except:
            print ('Verificar conexão')

    def load_files(self, p_bar=False):
        self.p_bar = p_bar

        """
        This function load files present in the current directory.
        Right now this took many time to run since it will 'read' all 
        files in a given repository
        """
        try:
            self.__pagina.cwd(self.__banco)
            self.__pagina.dir(self.__log.append)
            self.__list_data(self.__log)

        except ftp.error_perm:
            print ('diretorio invalido')


    def __list_data(self, lista):

        """
        Structure the information avaliable in the repo.
        """
        if self.p_bar == True:
            for i in tqdm(lista):
                if i.split()[3].endswith(('.dbc','.DBC','.DBF','.dbf')):
                    self.log['Data'].append(i.split()[0]),
                    self.log['Horario'].append(i.split()[1]),
                    self.log['Tamanho'].append(i.split()[2]),
                    self.log['Nome'].append(i.split()[3]),
                    self.log['Endereco'].append(self.__pagina.pwd())
                    if re.search(r"\d+",i.split()[3]):
                        self.log['Ano'].append(re.findall(r"\d+",\
                                i.split()[3])[0])
                    else:
                        self.log["Ano"].append(None)

                elif i.split()[3].endswith(('.dbc','.DBC','.DBF', \
                        '.dbf')) == False:
                    try:
                        self.__log = []
                        self.__pagina.cwd(i.split()[3])
                        self.__pagina.dir(self.__log.append)
                        self.__list_data(self.__log)
                        self.__pagina.cwd('..')
                    except:
                        pass
                else:
                    break

        elif self.p_bar == False:
            for i in lista:
                if i.split()[3].endswith(('.dbc','.DBC','.DBF','.dbf')):
                    self.log['Data'].append(i.split()[0]),
                    self.log['Horario'].append(i.split()[1]),
                    self.log['Tamanho'].append(i.split()[2]),
                    self.log['Nome'].append(i.split()[3]),
                    self.log['Endereco'].append(self.__pagina.pwd())
                    if re.search(r"\d+",i.split()[3]):
                        self.log['Ano'].append(re.findall(r"\d+", \
                                i.split()[3])[0])
                    else:
                        self.log["Ano"].append(None)

                elif i.split()[3].endswith(('.dbc','.DBC','.DBF', \
                        '.dbf')) == False:

                    try:
                        self.__log = []
                        self.__pagina.cwd(i.split()[3])
                        self.__pagina.dir(self.__log.append)
                        self.__list_data(self.__log)
                        self.__pagina.cwd('..')
                    except:
                        pass
                else:
                    break

    def write_file(self, path):

        """
        A function to write 
        """

        try:
            file_csv = pd.DataFrame.from_dict(self.log)
            file_csv.to_csv(path + ".csv", index = False)
        except:
            print("No file to write")
