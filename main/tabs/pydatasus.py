import pathlib
from os import path, mkdir, listdir
from os.path import expanduser
import ftplib as ftp
from os import system as sys_exec
import re

from convert_dbf_to_csv import ReadDbf


class PyDatasus:

    def __init__(self, PAGE='ftp.datasus.gov.br'):
        """Define o método inicial da classe, sendo a constante PAGE a
        página ftp do datasus e o primeiro acesso ao banco publico dentro
        da pasta dissemin.
        """
        self.path_files_csv = ''
        self.path_files_db = ''

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

        else:
            ...

        if isinstance(states, list) and isinstance(dates, list):
            regex = [
                database + state + date + r'\.[dD][bB][cC]'
                for state in states for date in dates
            ]
        elif isinstance(states, list) and isinstance(dates, str):
            regex = [
                database + state + dates + r'\.[dD][bB][cC]'
                for state in states
            ]

        elif isinstance(states, str) and isinstance(dates, str):
            regex = [
                database + states + dates + r'\.[dD][bB][cC]'
            ]

        elif isinstance(states, str) and isinstance(dates, list):
            regex = [
                database + state + dates + r'\.[dD][bB][cC]'
                for state in states
            ]

        self.__create_folders(system)
        self.f = open('{}{}.csv'.format(self.path_files_csv, system), 'w+')
        self.f.write(f'Endereco,Nome,Tamanho,Data\n')
        self.__sep_csv(system, regex)
        self.f.close()

    def convert_dbc(self, db):

        blast = path.join(path.dirname(__file__), 'blast-dbf')
        if db.endswith('.csv'):
            ...

        elif db.endswith('.dbf'):
            ...

        else:
            convertido = db[:-3] + 'dbf'
            sys_exec(f'{blast} {db} {convertido}')

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
        try:
            pathlib.Path(self.path_files_csv).mkdir(parents=True,
                                                    exist_ok=True)
        except:
            pass

        try:
            pathlib.Path(self.path_files_db).mkdir(parents=True,
                                                   exist_ok=True)
        except:
            pass

        try:
            mkdir(self.path_files_db + banco + '/')
        except:
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
                # print(regex_1)
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
                ...

        self.__page.cwd('..')

    def __sep_db_complete(self, banco, d):

        if path.isfile(self.path_files_csv + banco + '.csv'):
            with open(self.path_files_csv + banco + '.csv') as f:
                data = f.readlines()
            for x in data[1::]:
                self.__page.cwd(x.split(',')[0])
                data = x.split(',')[1][:-4]
                if path.isfile(self.path_files_db + banco + '/'
                               + data + '.dbc'):
                    self.convert_dbc(self.path_files_db + banco + '/'
                                     + data + '.dbc')

                    ReadDbf(self.path_files_db + banco + '/' + data + 'dbf')

                elif path.isfile(self.path_files_db + banco + '/'
                                 + data + '.DBC'):
                    self.convert_dbc(self.path_files_db + banco + '/'
                                     + data + '.DBC')

                    ReadDbf(self.path_files_db + banco + '/' + data + 'dbf')

                else:
                    self.__page.retrbinary(
                        'RETR ' + x.split(',')[1],
                        open(self.path_files_db + banco + '/'
                             + x.split(',')[1], 'wb').write)
                    self.convert_dbc(self.path_files_db + banco + '/'
                                     + x.split(',')[1])

                    ReadDbf(self.path_files_db + banco + '/'
                            + x.split(',')[1][:-3] + 'dbf')

    def __sep_db_partial(self, banco, regex):

        if path.isfile(self.path_files_csv + banco + '.csv'):
            with open(self.path_files_csv + banco + '.csv') as f:
                data = f.readlines()

            for x in data[1::]:
                if (x.split()[1][0:2] in regex
                        and (x.split()[1][-8:][0:4] in self.__DATE_1)
                    or (x.split()[1][0:4] in regex)
                        and (x.split()[1][-8:][2:4] in self.__DATE_2)
                    or (x.split()[1][0:4] in regex)
                        and (x.split()[1][-8:][2:4] in self.__DATE_2)
                    or (x.split()[1][0:2] in regex)
                        and (x.split()[-8:][0:4] in self.__DATE_2)
                    or (x.split()[1][0:3] in regex)
                        and (x.split()[1][-8:][0:4] in self.__DATE_1)):

                        self.__page.cwd(x.split(',')[0])
                        if path.isfile(self.path_files_db + banco + '/'
                                       + x.split(',')[1]):
                            ...

                        else:
                            self.__page.retrbinary(
                                'RETR ' + x.split(',')[1],
                                open(self.path_files_db + banco + '/'
                                     + x.split(',')[1], 'wb').write)


if __name__ == '__main__':
    PyDatasus().get_csv('SINASC')
    # PyDatasus().get_db_complete('SIM')
