import sys
from time import sleep
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QPushButton,
                             QProgressBar, QComboBox, QGroupBox, QGridLayout,
                             QLabel, QSpinBox, QTableWidget, QTableWidgetItem)
from PyQt5.QtCore import (Qt, pyqtSlot, pyqtSignal, QThread, QTimer,
                          QCoreApplication)

from pydatasus import PyDatasus


class _Loop(QThread):
    sinal = pyqtSignal(int)

    def __init__(self, thread, cls):
        super().__init__()
        self.thread = thread
        self.cls = cls

    def run(self):
        n = 0
        [self.cls.setEnabled(False) for botao in self.cls.lista_botoes]
        while self.thread.isRunning():
            n += 1
            if n == 100:
                n = 0

            sleep(0.3)
            QCoreApplication.processEvents()
            self.sinal.emit(n)
        self.sinal.emit(100)
        [self.cls.setEnabled(True) for botao in self.cls.lista_botoes]


class _Thread(QThread):

    def __init__(self, fn, *arg, **kw):
        super().__init__()
        self.fn = fn
        self.arg = arg
        self.kw = kw

    @pyqtSlot()
    def run(self):
        self.fn(*self.arg, **self.kw)


class Download(QWidget):

    def __init__(self):
        super().__init__()
        self.setWindowTitle('Iniciar')
        screen = QApplication.primaryScreen()
        screen = screen.size()
        self.setGeometry(0, 0, screen.width() - 100, screen.height() - 100)
        self.estados = {
            'Acre': {'AC': 'Norte'}, 'Amapá': {'AP': 'Norte'},
            'Amazonas': {'AM': 'Norte'}, 'Pará': {'PA': 'Norte'},
            'Rondônia': {'RO': 'Norte'}, 'Roraima': {'RR': 'Norte'},
            'Tocantins': {'TO': 'Norte'},

            'Alagoas': {'AL': 'Nordeste'}, 'Bahia': {'BA': 'Nordeste'},
            'Ceará': {'CE': 'Nordeste'}, 'Maranhão': {'MA': 'Nordeste'},
            'Paraíba': {'PB': 'Nordeste'}, 'Pernambuco': {'PE': 'Nordeste'},
            'Piauí': {'PI': 'Nordeste'},
            'Rio Grande do Norte': {'RN': 'Nordeste'},
            'Sergipe': {'SE': 'Nordeste'},

            'Distrito Federal': {'DF': 'Centro-Oeste'},
            'Goiás': {'GO': 'Centro-Oeste'},
            'Mato Grosso': {'MT': 'Centro-Oeste'},
            'Mato Grosso do Sul': {'MS': 'Centro-Oeste'},

            'Espírito Santo': {'ES': 'Sudeste'},
            'Minas Gerais': {'MG': 'Sudeste'},
            'Rio de Janeiro': {'RJ': 'Sudeste'},
            'São Paulo': {'SP': 'Sudeste'},

            'Paraná': {'PR': 'Sul'}, 'Rio Grande do Sul': {'RS': 'Sul'},
            'Santa Catarina': {'SC': 'Sul'},
        }
        self.regioes = {
            'Norte': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO'],
            'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN',
                         'SE'],
            'Centro-Oeste': ['DF', 'GO', 'MT', 'MS'],
            'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
            'Sul': ['PR', 'RS', 'SC']
        }
        self.bases_de_dados = {
            'SIM': {
                'Óbito': 'DO', 'Óbito Fetal': 'DOFE'
            },
            'SINAN': {
                'Animais Peçonhentos': 'ANIM', 'Botulismo': 'BOTU',
                'Chagas': 'CHAG',  'Cólera': 'COLE', 'Coqueluche': 'COQU',
                'Difteria': 'DIFT', 'Esquistossomose': 'ESQU',
                'Febre Maculosa': 'FMAC', 'Febre Tifóide': 'FTIF',
                'Hanseníase': 'HANS', 'Leptospirose': 'LEPT',
                'Meningite': 'MENI', 'Raiva': 'RAIV', 'Tétano': 'TETA',
                'Tuberculose': 'TUBE'
            },
            'SINASC': {
                'Nascidos Vivos': 'DN'
            }
        }

        self.group_system = QGroupBox('Sistemas')
        self.grid_sys = QGridLayout()
        self.grid_sys.setSpacing(50)

        self.sistema = QComboBox()
        self.sistema.addItems(['SELECIONAR SISTEMA',
                               'SIH', 'SIM', 'SINAN', 'SINASC'])
        self.sistema.currentTextChanged.connect(self.escolhe_sistema)
        self.bases = QComboBox()
        self.bases.setEditable(True)
        self.bases.setEnabled(False)
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.locais = QComboBox()
        self.locais.addItems(['SELECIONAR TODOS/ESTADO/REGIÃO',
                              'TODOS', 'ESTADO', 'REGIÃO'])
        self.locais.currentTextChanged.connect(self.escolhe_estado_ou_regiao)
        self.estados_regioes = QComboBox()
        self.estados_regioes.setEditable(True)
        self.estados_regioes.setEnabled(False)

        self.ano_inicial_label = QLabel('ANO INICIAL:')
        self.ano_final_label = QLabel('ANO FINAL:')
        self.ano_inicial = QSpinBox()
        self.ano_inicial.setRange(2010, 2019)
        self.ano_final = QSpinBox()
        self.ano_final.setRange(2010, 2019)
        self.spin_cores_label = QLabel('SETAR CORES:')
        self.spin_cores = QSpinBox()
        self.spin_memoria_label = QLabel('SETAR MEMORIA:')
        self.spin_memoria = QSpinBox()
        self.carregar_banco = QPushButton('CARREGAR BANCO')
        self.carregar_banco.clicked.connect(self.carregar_dados)
        self.visualizar_banco = QPushButton('VISUALIZAR BANCO')
        self.visualizar_banco.clicked.connect(self.visualizar_dados)

        self.lista_botoes = [
            self.sistema, self.bases, self.locais, self.estados_regioes,
            self.ano_inicial, self.ano_final, self.spin_cores,
            self.spin_memoria, self.carregar_banco, self.visualizar_banco
        ]

        self.grid_sys.addWidget(self.sistema, 0, 0)
        self.grid_sys.addWidget(self.bases, 1, 0)
        self.grid_sys.addWidget(self.progress_bar, 2, 0, 2, 2)
        self.grid_sys.addWidget(self.locais, 0, 1)
        self.grid_sys.addWidget(self.estados_regioes, 1, 1)

        self.group_system.setLayout(self.grid_sys)

        self.group_funct = QGroupBox('Opções')
        self.grid_funct = QGridLayout()

        self.grid_funct.addWidget(self.ano_inicial_label, 0, 0)
        self.grid_funct.addWidget(self.ano_final_label, 1, 0)
        self.grid_funct.addWidget(self.ano_inicial, 0, 1, Qt.AlignLeft)
        self.grid_funct.addWidget(self.ano_final, 1, 1, Qt.AlignLeft)

        self.grid_funct.addWidget(self.spin_cores_label, 0, 2)
        self.grid_funct.addWidget(self.spin_cores, 0, 3, Qt.AlignLeft)
        self.grid_funct.addWidget(self.spin_memoria_label, 1, 2)
        self.grid_funct.addWidget(self.spin_memoria, 1, 3, Qt.AlignLeft)

        self.group_funct.setLayout(self.grid_funct)

        self.group_botoes = QGroupBox()
        self.grid_buttons = QGridLayout()

        self.grid_buttons.addWidget(self.carregar_banco, 0, 0)
        self.grid_buttons.addWidget(self.visualizar_banco, 0, 1)

        self.group_botoes.setLayout(self.grid_buttons)

        self.tabela = QTableWidget(10, 150)

        self.group_table = QGroupBox('Tabela')
        self.grid_table = QGridLayout()
        self.grid_table.addWidget(self.tabela)
        self.group_table.setLayout(self.grid_table)

        self.main_layout = QGridLayout()
        self.main_layout.setSpacing(10)

        self.main_layout.addWidget(self.group_system, 0, 0)
        self.main_layout.addWidget(self.group_funct, 0, 1)
        self.main_layout.addWidget(self.group_botoes, 1, 1)
        self.main_layout.addWidget(self.group_table, 2, 0, 1, 0)

        self.setLayout(self.main_layout)

        self.show()

    @pyqtSlot(str)
    def escolhe_estado_ou_regiao(self, text):
        self.estados_regioes.clear()
        self.estados_regioes.setEnabled(True)

        if text == 'ESTADO':
            estados = list(self.estados.keys())
            estados.sort()

            self.estados_regioes.addItems(estados)

        elif text == 'REGIÃO':
            regioes = list(self.regioes.keys())
            regioes.sort()

            self.estados_regioes.addItems(regioes)

        elif text == 'TODOS':
            self.estados_regioes.addItem('TODOS')

        else:
            self.estados_regioes.setEnabled(False)

    @pyqtSlot(str)
    def escolhe_sistema(self, text):
        self.bases.clear()
        self.bases.setEnabled(True)

        if text == 'SIH':
            ...

        elif text == 'SELECIONAR SISTEMA':
            self.bases.setEnabled(False)

        else:
            index = list(self.bases_de_dados.keys()).index(text)
            sistema = list(self.bases_de_dados.keys())[index]
            bases = list(self.bases_de_dados.get(sistema).keys())
            bases.sort()
            self.bases.addItems(bases)

    def carregar_dados(self):
        self.sistema_chave = self.sistema.currentText()
        condicao = ''
        try:
            self.base_chave = self.bases_de_dados.get(
                self.sistema_chave).get(self.bases.currentText())
            self.data = self.carrega_datas()
            self.local_selecionado = ''
            if self.locais.currentText() == 'ESTADO':
                self.local_selecionado = list(
                    self.estados.get(
                        self.estados_regioes.currentText()).keys())

            elif self.locais.currentText() == 'REGIÃO':
                self.local_selecionado = self.regioes.get(
                    self.estados_regioes.currentText())

            elif self.locais.currentText() == 'TODOS':
                self.local_selecionado = [
                    val for key in self.regioes.keys()
                    for val in self.regioes.get(key)
                ]

            cond = [self.sistema_chave, self.base_chave,
                    self.local_selecionado, self.data]

            condicao = [
                self.sistem_chave != 'SELECIONAR SISTEMA',
                self.local_selecionado != 'SELECIONAR TODOS/ESTADO/REGiÃO'
            ]
        except AttributeError:
            ...

        if all(condicao):
            self.thread_csv = _Thread(PyDatasus().get_csv_db_complete,
                                      *cond)
            self.thread_csv.start()

            self.loop = _Loop(self.thread_csv, self)
            self.loop.sinal.connect(self.atualiza_barra)
            self.loop.start()

    @pyqtSlot(int)
    def atualiza_barra(self, val):
        self.progress_bar.setValue(val)

    def carrega_datas(self):
        if self.ano_inicial.value() < self.ano_final.value():
            return [str(data) for data in range(self.ano_inicial.value(),
                                                self.ano_final.value() + 1)]

        elif self.ano_inicial.value() > self.ano_final.value():
            return [
                str(data) for data in range(self.ano_final.value(),
                                            self.ano_inicial.value() + 1)
            ]

        else:
            return str(self.ano_inicial.value())

    def visualizar_dados(self):
        print(self.sistema_chave, self.base_chave, self.local_selecionado,
              self.data)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    download = Download()
    sys.exit(app.exec_())
