#!/usr/bin/env python3

from os import path, mkdir
import ftplib as ftp
from platform import system
import re


class PyDatasus:
    """Biblioteca que realiza download dos arquivos de database dos
     sistemas SIM, SINAN e SINASC da plataforma ftp.datasus.gov.br

    Os arquivos gerados/baixados durante execução deste script serão
    armazenados em "Documentos/files_csv" e "Documentos/files_db".

    Antes de tentar baixar os arquivos database dos sistemas (dbc, dbf),
    é preciso gerar um arquivo no formato csv contendo o caminho dos arquivos
    database.

    Um exemplo muito breve:

        # importando a biblioteca
        from pydatasus import PyDatasus

        # criando uma instancia da biblioteca importada.
        data = PyDatasus()

        # utilizando o método get_csv() com o argumento 'sim' para gerar o csv
        # com as informções sobre os database no sistema 'sim'.
        data.get_csv('sim')

        # utilizando o método get_db_from_file() com o argumento 'sim' para
        # procurar se existe um arquivo 'SIM.csv', caso exista, fará download
        # das bases de dados disponíveis.

        data.get_db_from_file('sim')

    """

    def __init__(self, PAGE='ftp.datasus.gov.br'):
        """Define o método inicial da classe, sendo a constante PAGE a
        página ftp do datasus e o primeiro acesso ao banco publico dentro
        da pasta dissemin.
        """
        self.__page = ftp.FTP(PAGE)
        self.__page.login()
        self.__page.cwd('/dissemin/publicos/')

    def get_csv(self, *args: str):
        """Gera um arquivo no formato csv contendo link, nome, tamanho e
        data do database.
        """

        for i in args:
            self.__platform(i)
            self.f = open(f'{self.path_files_csv}{i}.csv', 'w+')
            self.f.write(f'Endereco,Nome,Tamanho,Data\n')
            self.__sep_csv(i)

    def get_db_from_memory(self, *args: str):
        """Baixa os arquivos db* a partir da memória.
        Uma idéia para o futuro. Ta bem facil de implementar.
        """

        for i in args:
            self.__platform(i)
            self.__sep_db_memory(i, i)

    def get_db_from_file(self, *args):
        """Baixa os arquivos db* a partir de um csv existente."""

        for i in args:
            self.__platform(i)
            self.__sep_db_file(i)

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
        self.path_files_csv = path.expanduser('~\\Documentos\\files_csv\\')
        self.path_files_db = path.expanduser('~\\Documentos\\files_db\\')
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

    def __sep_csv(self, banco):
        """Faz uma separação da string na variavel e depois escreve cada
        pedaço no arquivo csv.
        """

        branch = []

        self.__page.cwd(banco)
        self.__page.dir(branch.append)

        for x in branch:
            if 'DIR' in x:
                self.__sep_csv(x.split()[3])
            elif x.split()[3].endswith(('.DBC', '.DBF', '.DBR', '.dbc',
                                        '.dbf', '.dbr')):
                self.f.write(
                    '{},{},{} KB,{}\n'.format(
                        self.__page.pwd(),
                        x.split()[3],
                        int(x.split()[2]) / 1000, x.split()[0]))
            else:
                pass

        self.__page.cwd('..')

    def __sep_db_memory(self, banco, d):
        """Faz uma separação da variavel e baixa os arquivos que forem
        identificados como db*.
        """
        branch = []
        self.__page.cwd(banco)
        self.__page.dir(branch.append)
        for x in branch:
            if 'DIR' in x:
                self.__sep_db_memory(x.split()[3], d)
            elif x.split()[3].endswith(('.DBC', '.DBF', '.dbc', '.dbf')):
                if path.isfile(self.path_files_db + d + '/' + x.split()[3]):
                    pass
                else:
                    self.__page.retrbinary(
                        'RETR ' + x.split()[3], open(
                            self.path_files_db + d + '/' + x.split()[3],
                            'wb').write)
        self.__page.cwd('..')

    def __sep_db_file(self, banco):
        """Confere se o arquivo db* ja foi baixado e caso ja exista não
        repete o download.
        """
        if path.isfile(self.path_files_csv + banco + '.csv'):
            with open(self.path_files_csv + banco + '.csv') as f:
                data = f.readlines()

                ls = []

                for x in data[1::]:
                    if bool(re.findall(r'BR[0-9]{4}\.*', x.split(',')[1])):
                        ls.append(x)

                if len(ls) != 0:
                    for x in ls:
                        if ''.join(re.findall(r'BR[0-9]{4}\.*', x)):
                            self.__page.cwd(x.split(',')[0])
                            if path.isfile(self.path_files_db + banco + '/'
                                           + x.split(',')[1]):
                                ...

                            else:
                                self.__page.retrbinary(
                                    'RETR ' + x.split(',')[1],
                                    open(self.path_files_db + banco + '/'
                                         + x.split(',')[1], 'wb').write)

                else:
                    for x in data[1::]:
                        self.__page.cwd(x.split(',')[0])
                        if path.isfile(
                           self.path_files_db + banco + '/'
                           + x.split(',')[1]):

                            pass
                        else:
                            self.__page.retrbinary(
                                'RETR ' + x.split(',')[1],
                                open(self.path_files_db + banco + '/'
                                     + x.split(',')[1], 'wb').write)


if __name__ == '__main__':
    # testando
    PyDatasus().get_csv('SIM', 'SINAN', 'SINASC')
