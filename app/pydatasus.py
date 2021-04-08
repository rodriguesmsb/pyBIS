#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import sys
import platform
import re
import pathlib
import time
from os import path, mkdir, remove, system
import ftplib as ftp
from PyQt5.QtCore import QObject, pyqtSignal

from convert_dbf_to_csv import ReadDbf


class PyDatasus(QObject):

    download_signal = pyqtSignal(int)
    lcd_signal = pyqtSignal(int)
    label_signal = pyqtSignal(str)
    finished = pyqtSignal(int)

    def __init__(self):
        super().__init__()
        self.__none = None
        self.__page = ftp.FTP('ftp.datasus.gov.br')
        self.__page.login()
        self.__page.cwd('/dissemin/publicos/')
        if platform.system().lower() == "linux":
            self.__blast = path.join(path.dirname(__file__), './blast-dbf')
        elif platform.system().lower() == "windows":
            self.__blast = path.join(path.dirname(__file__),
                                     './blast-dbf.exe')
        self.__path_table = path.expanduser('~/datasus_tabelas/')
        self.__path_dbc = path.expanduser('~/datasus_dbc/')
        self.label_signal.emit("Iniciando conexão")


    def get_table_csv(self, database: str, base: [str, list],
            state: [str, list], date: [str, list]):

        self.label_signal.emit("Criando tabela")
        date = self.__adjust_date(database, date)
        pattern = self.__generate_pattern(database, base, state, date)
        self.__create_folder(database, base, table_or_dbc='table')
        self.__table = open(f'{self.__path_table}{database}.csv', 'w+')
        self.__table.write('Endereço,Nome,Tamanho,Data\n')
        self.__get_data_table(database, pattern)
        self.__table.close()


    def get_file_dbc(self, database, base, state, date):
        date = self.__adjust_date(database, date)
        patterns = self.__generate_pattern(database, base, state, date)

        if isinstance(patterns, list):
            for pattern in patterns:
                self.__create_folder(database, pattern.split('/')[0],
                                     table_or_dbc='dbc')
                self.__get_data_dbc(database)

        elif isinstance(patterns, str):
            self.__create_folder(database, patterns, table_or_dbc='dbc')
            self.__get_data_dbc(patterns)


    def get_data(self, database, base, state, date):
        self.get_table_csv(database, base, state, date)
        self.get_file_dbc(database, base, state, date)


    def __convert_dbc(self, db):
        self.label_signal.emit("Convertendo banco")
        if db.endswith('.csv'):
            pass

        elif db.endswith('.dbf'):
            pass

        else:
            after_ = db[:-3] + 'dbf'
            system(f'{self.__blast} {db} {after_}')
            remove(path.expanduser(db))
            ReadDbf({after_}, convert='convert')
            remove(after_)
            after_ = None
            db = None

    def __adjust_date(self, database, dates):
        if database == 'SINAN':
            if isinstance(dates, str):
                return dates[2:4]
            elif isinstance(dates, list):
                return [date[2:4] for date in dates]
        elif database == 'SIHSUS':
            if isinstance(dates, str):
                return dates[2:4] + r'\d{2}'
            elif isinstance(dates, list):
                return [date[2:4] + r'\d{2}' for date in dates]
        else:
            return dates


    def __generate_pattern(self, database, base, states, dates):
        if isinstance(states, list) and isinstance(dates, list):
            return [ base + state + date + r'\.[dDc][bBs][cCv]'
                    for state in states for date in dates ]
        elif isinstance(states, list) and isinstance(dates, str):
            return [ base + state + dates + r'\.[dDc][bBs][cCv]'
                     for state in states ]
        elif isinstance(states, str) and isinstance(dates, str):
            return [ base + states + dates + r'\.[dDc][bBs][cCv]' ]
        elif isinstance(states, str) and isinstance(dates, list):
            return [ base + states + date + r'\.[dDc][bBs][cCv]'
                     for date in dates ]

    def __create_folder(self, database, pattern, table_or_dbc):
        pathlib.Path(self.__path_table).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.__path_dbc).mkdir(parents=True, exist_ok=True)
        try:
            mkdir(path.expanduser(self.__path_dbc + database + '/'))
        except FileExistsError:
            pass


    def __get_data_table(self, database, pattern):
        self.label_signal.emit("Buscando em {}".format(database))
        branch = []

        self.__page.cwd(database)
        self.__page.dir(branch.append)

        r = re.compile('|'.join(pattern), re.IGNORECASE)

        for node in branch:
            if 'DIR' in node:
                self.__get_data_table(node.split()[3], pattern)
            elif re.match(r, node.split()[3]):
                self.__table.write(
                    '{},{},{} KB,{}\n'.format(
                    self.__page.pwd(), node.split()[3],
                    int(node.split()[2]) / 1000, node.split()[0])
                )
            else:
                pass

        self.__page.cwd('..')

    def __create_file_write(self, base, select):
        self.__file_base = open(
            path.abspath(self.__path_dbc + "/" + base
                + "/" + select), "wb"
        )
        self.__file_base_size = self.__page.size(select)

    def __write(self, base):
        self.__file_base.write(base)
        self.__pbar += len(base)
        ratio = round((float(self.__pbar / self.__file_base_size)
                       * 100 - 6), 1)
        percentage = round(100 * ratio / (100 - 6), 1)
        self.download_signal.emit(int(percentage))

    def __get_data_dbc(self, database):
        count = 0 
        if path.isfile(path.expanduser(
            self.__path_table + database + '.csv')):
            with open(path.expanduser(
                self.__path_table + database + '.csv')) as table:
                lines = table.readlines()
            for line in lines[1:]:
                count += 1
                self.__page.cwd(line.split(',')[0])
                if not path.isfile(path.expanduser(
                    self.__path_dbc + database + '/'
                    + line.split(',')[1].split('.')[0] + '.csv')):

                    self.__pbar = 0
                    self.download_signal.emit(self.__pbar)
                    self.__create_file_write(
                        database, line.split(',')[1]
                    )

                    self.label_signal.emit(
                        "Baixando {}".format(line.split(',')[1])
                    )
                    self.lcd_signal.emit(count)
                    self.__page.retrbinary('RETR ' + line.split(',')[1],
                                           self.__write)

                    self.label_signal.emit(
                        "Convertendo: {}".format(line.split(',')[1])
                    )
                    self.__convert_dbc(
                        path.expanduser(
                            self.__path_dbc + database + '/'
                            + line.split(',')[1])
                    )
                    self.label_signal.emit(
                        "{} convertido com sucesso!".format(
                            line.split(',')[1]
                        )
                    )
                else:
                    pass

            self.label_signal.emit("Concluido com sucesso")
            self.download_signal.emit(100)
            self.finished.emit(1)


if __name__ == '__main__':
    datasus = PyDatasus()
    datasus.get_data('SIM', 'DO', 'AC', ['2010', '2011', '2012'])
