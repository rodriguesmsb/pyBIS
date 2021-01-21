from sys import argv
import pathlib
from os import path, mkdir, remove
from os import system as sys_exec
import ftplib as ftp
import re
from PyQt5.QtWidgets import QApplication, QProgressBar

from convert_dbf_to_csv import ReadDbf


class PyDatasus:

    def __init__(self, PAGE='ftp.datasus.gov.br'):
        """Define o método inicial da classe, sendo a constante PAGE a
        página ftp do datasus e o primeiro acesso ao banco publico dentro
        da pasta dissemin.
        """

        # self.__app = QApplication(argv)
        # self.__pbar = QProgressBar()
        # self.__count = 0
        # self.__pbar.setValue(self.__count)
        # self.__pbar.show()

        self.path_files_csv = ''
        self.path_files_db = ''
        self.blast = path.join(path.dirname(__file__), 'blast-dbf')

        self.__page = ftp.FTP(PAGE)
        self.__page.login()
        self.__page.cwd('/dissemin/publicos/')

    def get_csv(self, system, database, states, dates):
        """Gera um arquivo no formato csv contendo link, nome, tamanho e
        data do datadatabase.
        """

        regex = ''

        if system == 'SINAN':
            if isinstance(dates, str):
                dates = dates[2:4]

            elif isinstance(dates, list):
                dates = [date[2:4] for date in dates]

        elif system == 'SIHSUS':
            if isinstance(dates, str):
                dates = dates[2:4] + r'\d{2}'

            elif isinstance(dates, list):
                dates = [date[2:4] + r'\d{2}' for date in dates]

        if isinstance(states, list) and isinstance(dates, list):
            regex = [
                database + state + date + r'\.[dDc][bBs][cCv]'
                for state in states for date in dates
            ]
        elif isinstance(states, list) and isinstance(dates, str):
            regex = [
                database + state + dates + r'\.[dDc][bBs][cCv]'
                for state in states
            ]

        elif isinstance(states, str) and isinstance(dates, str):
            regex = [
                database + states + dates + r'\.[dDc][bBs][cCv]'
            ]

        elif isinstance(states, str) and isinstance(dates, list):
            regex = [
                database + states + date + r'\.[dDc][bBs][cCv]'
                for date in dates
            ]

        self.__create_folders(system)
        self.f = open('{}{}.csv'.format(self.path_files_csv, system), 'w+')
        self.f.write('Endereco,Nome,Tamanho,Data\n')
        self.__sep_csv(system, regex)
        self.f.close()

    def convert_dbc(self, db):

        if db.endswith('.csv'):
            pass

        elif db.endswith('.dbf'):
            pass


        else:
            convertido = db[:-3] + 'dbf'
            sys_exec(f'{self.blast} {db} {convertido}')
            remove(db)
            ReadDbf({convertido}, convert='convert')
            remove(convertido)
            convertido = None
            db = None

    def get_csv_db_complete(self, system, database, states, dates):
        self.get_csv(system, database, states, dates)
        self.get_db_complete(system)

    def get_db_complete(self, *args):
        """Baixa os arquivos db* a partir da memória.
        Uma idéia para o futuro. Ta bem facil de implementar.
        """
        for i in args:
            if i != 'SELECIONAR SISTEMA':
                self.__create_folders(i)
                self.__sep_db_complete(i, i)

    def get_db_partial(self, system, base):
        """Baixa os arquivos db* a partir de um csv existente."""

        self.__create_folders(system)
        self.__sep_db_partial(system, base)

    def __create_folders(self, banco):

        self.path_files_csv = path.expanduser('~/Documentos/files_csv/')
        self.path_files_db = path.expanduser('~/Documentos/files_db/')

        pathlib.Path(self.path_files_csv).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.path_files_db).mkdir(parents=True, exist_ok=True)
        try:
            mkdir(self.path_files_db + banco + '/')
        except FileExistsError:
            pass

    def __sep_csv(self, banco, regex_1=None):
        """Faz uma separação da string na variavel e depois escreve cada
        pedaço no arquivo csv.
        """
        branch = []

        self.__page.cwd(banco)
        self.__page.dir(branch.append)

        r = re.compile('|'.join(regex_1), re.IGNORECASE)

        for x in branch:
            if 'DIR' in x:
                self.__sep_csv(x.split()[3], regex_1)

            elif re.match(r, x.split()[3]):
                '''
            elif (x.split()[3][0:2] in self.__SIM
                  and (x.split()[3][-8:][0:4] in self.__DATE_1)
                  or (x.split()[3][0:4] in self.__SIM)
                  and (x.split()[3][-8:][2:4] in self.__DATE_2)
                  or (x.split()[3][0:4] in self.__SINAN)
                  and (x.split()[3][-8:][2:4] in self.__DATE_2)
                  or (x.split()[3][0:2] in self.__SINASC)
                  and (x.split()[-8:][0:4] in self.__DATE_2)
                  or (x.split()[3][0:3] in self.__SINASC)
                  and (x.split()[3][-8:][0:4] in self.__DATE_1)):
                      '''

                self.f.write(
                    '{},{},{} KB,{}\n'.format(
                        self.__page.pwd(),
                        x.split()[3],
                        int(x.split()[2]) / 1000, x.split()[0]))

            else:
                pass

        self.__page.cwd('..')

    def __sep_db_complete(self, banco, d):
        if path.isfile(self.path_files_csv + banco + '.csv'):
            with open(self.path_files_csv + banco + '.csv') as f:
                data = f.readlines()
            for x in data[1:]:
                self.__page.cwd(x.split(',')[0])
                data = x.split(',')[1][:-4]
                if not path.isfile(self.path_files_db + banco + '/' + data
                                   + '.csv'):
                    self.__page.retrbinary(
                        'RETR ' + x.split(',')[1],
                        open(self.path_files_db + banco + '/'
                             + x.split(',')[1], 'wb').write)

                    self.convert_dbc(self.path_files_db + banco + '/'
                                     + x.split(',')[1])
                else:
                    pass


if __name__ == '__main__':
    PyDatasus().get_csv_db_complete('SIM', 'DO', 'AC', '10')
