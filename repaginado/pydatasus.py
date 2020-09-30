#!/usr/bin/env python3

from os import path, mkdir
from os.path import expanduser
import ftplib as ftp
from platform import system
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
        self.__SIM = ['DO', 'DOFE']
        self.__SINAN = ['ANIM', 'BOTU', 'CHAG', 'COLE', 'COQU', 'DIFT', 'ESQU',
                        'FMAC', 'FTIF', 'HANS', 'LEPT', 'MENI', 'RAIV', 'TETA',
                        'TUBE']
        # self.__SINAN.extend['PFAN','MALA','IEXO','HANT','PEST', 'TETN']
        self.__SINASC = ['DN', 'DNP', 'DNV']
        self.__DATE_1 = [str(i) for i in range(2010, 2019 + 1)]
        self.__DATE_2 = [str(i) for i in range(10, 19 + 1)]

        self.__DICT_DISEASE = {
            'Óbito': 'DO', 'Óbito Fetal': 'DOFE',
            'Animais Pençonhentos': 'ANIM', 'Botulismo': 'BOTU',
            'Chagas': 'CHAG', 'Cólera': 'COLE',
            'Coqueluche': 'COQU', 'Difteria': 'DIFT',
            'Esquistossomose': 'ESQU', 'Febre Maculosa': 'FMAC',
            'Febre Tifóide': 'FTIF', 'Hanseníase': 'HANS',
            'Leptospirose': 'LEPT', 'Meningite': 'MENI',
            'Raiva': 'RAIV', 'Tétano': 'TETA',
            'Tuberculose': 'TUBE'
        }

        self.__page = ftp.FTP(PAGE)
        self.__page.login()
        self.__page.cwd('/dissemin/publicos/')

    def create_regex_type_select(self, prefix, states, dates, dict_states):
        if states is not None:
            if isinstance(dates, list) and isinstance(states, list):
                regex_1 = [self.__DICT_DISEASE.get(prefix) + state + str(date)
                           for state in states
                           for date in dates]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + state
                                + str(date)[2:4]
                                for state in states
                                for date in dates])
                return regex_1

            elif isinstance(dates, list) and isinstance(states, str):
                regex_1 = [self.__DICT_DISEASE.get(prefix) + states + str(date)
                           for date in dates]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + states
                                + str(date)[2:4]
                                for date in dates])

                return regex_1

            elif isinstance(dates, int) and isinstance(states, list):
                regex_1 = [self.__DICT_DISEASE.get(prefix) + state + str(dates)
                           for state in states]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + state
                                + str(dates)[2:4]
                                for state in states])
                return regex_1

            elif isinstance(dates, int) and isinstance(states, str):
                regex_1 = [self.__DICT_DISEASE.get(prefix) + states
                           + str(dates)]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + states
                                + str(dates)[2:4]])

                return regex_1

        elif states is None:
            if isinstance(dates, list):

                regex_1 = [self.__DICT_DISEASE.get(prefix) + state[1]
                           + str(date)
                           for state in list(dict_states.values())
                           for date in dates]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + state[1]
                                + str(date)[2:4]
                                for state in list(dict_states.values())
                                for date in dates])

                return regex_1

#           elif isinstance(dates, int):
            else:
                regex_1 = [self.__DICT_DISEASE.get(prefix) + state[1]
                           + str(dates)
                           for state in list(dict_states.values())]

                regex_1.extend([self.__DICT_DISEASE.get(prefix) + state[1]
                                + str(dates)[2:4]
                                for state in list(dict_states.values())])

                return regex_1

    def create_regex_all(self, prefix, states, dates, dict_states):
        if states is not None:
            if isinstance(dates, list) and isinstance(states, list):
                regex_1 = [pre + state + str(date)
                           for pre in prefix
                           for state in states
                           for date in dates]

                regex_1.extend([pre + state + str(date)[2:4]
                                for pre in prefix
                                for state in states
                                for date in dates])

                return regex_1

            elif isinstance(dates, list) and isinstance(states, str):
                regex_1 = [pre + states + str(date)
                           for pre in prefix
                           for date in dates]

                regex_1.extend([pre + states + str(date)[2:4]
                                for pre in prefix
                                for date in dates])

                return regex_1

            elif isinstance(dates, int) and isinstance(states, list):
                regex_1 = [pre + state + str(dates)
                           for pre in prefix
                           for state in states]

                regex_1.extend([pre + state + str(dates)[2:4]
                                for pre in prefix
                                for state in states])

                return regex_1

            elif isinstance(dates, int) and isinstance(states, str):
                regex_1 = [pre + states + str(dates)
                           for pre in prefix]

                regex_1.extend([pre + states + str(dates)[2:4]
                                for pre in prefix])

                return regex_1

        elif states is None:
            if isinstance(dates, list):
                regex_1 = [pre + state[1] + str(date)
                           for pre in prefix
                           for state in list(dict_states.values())
                           for date in dates]

                regex_1.extend([pre + state[1] + str(date)[2:4]
                                for pre in prefix
                                for state in list(dict_states.values())
                                for date in dates])

                return regex_1

            else:
                regex_1 = [pre + state[1] + str(dates)
                           for pre in prefix
                           for state in list(dict_states.values())]

                regex_1.extend([pre + state[1] + str(dates)[2:4]
                                for pre in prefix
                                for state in list(dict_states.values())])

                return regex_1

    def get_csv(self, system, database=None, states=None, dates=None,
                dict_states=None):

        # self.create_regex(system, database, states, dates, dict_states)
        """Gera um arquivo no formato csv contendo link, nome, tamanho e
        data do datadatabase.
        """

        regex_1 = ''
        prefix = ''

        if database == 'Todos':
            if system == 'SIM':
                prefix = self.__SIM
            elif system == 'SINAN':
                prefix = self.__SINAN
            elif system == 'SINASC':
                prefix = self.__SINASC

            regex_1 = self.create_regex_all(prefix, states, dates, dict_states)

        elif database == 'Nascidos Vivos':
            if system == 'SIM':
                prefix = self.__SIM
            elif system == 'SINAN':
                prefix = self.__SINAN
            elif system == 'SINASC':
                prefix = self.__SINASC

            regex_1 = self.create_regex_all(prefix, states, dates, dict_states)

        else:
            regex_1 = self.create_regex_type_select(
                database, states, dates, dict_states)

        self.__platform(system)
        self.f = open('{}{}.csv'.format(self.path_files_csv, system), 'w+')
        # self.f = open(f'{self.path_files_csv}{system}.csv', 'w+')
        # self.f = open('teste.csv', 'w+')
        self.f.write(f'Endereco,Nome,Tamanho,Data\n')
        self.__sep_csv(system, regex_1)
        self.f.close()

    def get_db_complete(self, *args):
        """Baixa os arquivos db* a partir da memória.
        Uma idéia para o futuro. Ta bem facil de implementar.
        """
        for i in args:
            if i != 'SELECIONAR SISTEMA':
                self.__platform(i)
                self.__sep_db_complete(i, i)

    def get_db_partial(self, system, base):
        """Baixa os arquivos db* a partir de um csv existente."""

        self.__platform(system)
        self.__sep_db_partial(system, base)

    def __platform(self, banco):
        """Confere a plataforma do sistema para criar os diretórios para
        download dos arquivos csv e db*.
        """

        if system().lower() == 'linux':
            self.__if_linux(banco)

        elif system().lower == 'windows':
            self.__if_windows(banco)

    def __if_linux(self, banco):
        """Executa caso o sistema seja linux."""
        self.path_files_csv = path.expanduser('~/Documentos/files_csv/')
        self.path_files_db = path.expanduser('~/Documentos/files_db/')
        try:
            mkdir(self.path_files_csv)
        except:
            pass

        try:
            mkdir(self.path_files_db)
        except:
            pass

        try:
            mkdir(self.path_files_db + banco + '/')
        except:
            pass

    def __if_windows(self, banco):
        """Executa caso o sistema seja windows."""
        self.path_files_csv = path.expanduser('~/Documentos/files_csv/')
        self.path_files_db = path.expanduser('~/Documentos/files_db/')
        try:
            mkdir(self.path_files_csv)
        except:
            pass

        try:
            mkdir(elf.path_files_db)
        except:
            pass

        try:
            mkdir(self.path_files_db + banco + '\\')
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
                data = x.split(',')[1][:-4] + '.csv'
                if system().lower() == 'linux':
                    if path.isfile(self.path_files_db + banco + '/'
                                   + data):
                        pass

                    else:
                        self.__page.retrbinary(
                            'RETR ' + x.split(',')[1],
                            open(self.path_files_db + banco + '/'
                                 + x.split(',')[1], 'wb').write)
                elif system().lower() == 'windows':
                    if path.isfile(
                            expanduser(
                                f'{self.path_files_db} + {banco} + /  + data')):
                        pass

                    else:
                        self.__page.retrbinary(
                            'RETR ' + x.split(',')[1],
                            open(expanduser(self.path_files_db + banco + '/' + x.split(',')[1]), 'wb').write)
#                           open(self.path_files_db + banco + '\\'
#                                + x.split(',')[1], 'wb').write)

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
