import os
import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QComboBox, QGroupBox,
                             QVBoxLayout, QHBoxLayout, QSpinBox,
                             QPushButton, QColorDialog, QFormLayout)
from PyQt5.QtGui import QIcon, QFontDatabase, QColor
from PyQt5.QtCore import Qt, pyqtSignal, pyqtSlot


sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

# layout = os.path.join(os.path.dirname(__file__), 'layouts/')
icos = os.path.join(os.path.dirname(__file__), 'imgs/')


class QColorButton(QPushButton):
    '''
    Custom Qt Widget to show a chosen color.
    Left-clicking the button shows the color-chooser, while
    right-clicking resets the color to None (no-color).
    '''

    colorChanged = pyqtSignal()

    def __init__(self, *args, **kwargs):
        super(QColorButton, self).__init__(*args, **kwargs)

        self._color = None
        self.setMaximumWidth(32)
        self.pressed.connect(self.onColorPicker)

    def setColor(self, color):
        if color != self._color:
            self._color = color
            self.colorChanged.emit()

        if self._color:
            self.setStyleSheet("background-color: %s;" % self._color)
        else:
            self.setStyleSheet("")

    def color(self):
        return self._color

    def onColorPicker(self):
        '''
        Show color-picker dialog to select color.
        Qt will use the native dialog by default.
        '''
        dlg = QColorDialog(self)
        if self._color:
            dlg.setCurrentColor(QColor(self._color))

        if dlg.exec_():
            self.setColor(dlg.currentColor().name())

    def mousePressEvent(self, e):
        if e.button() == Qt.RightButton:
            self.setColor(None)

        return super(QColorButton, self).mousePressEvent(e)


class Config(QWidget):
    def __init__(self):
        super().__init__()

        main_layout = QVBoxLayout()
        group_theme = QGroupBox('Tema')
        group_font = QGroupBox('Fonte')
        group_color = QGroupBox('Cores')
        form_color = QFormLayout()
        vbox = QVBoxLayout()
        hbox = QHBoxLayout()
        self.setWindowIcon(QIcon(icos + 'config.png'))
        self.setWindowTitle('Configuração')
        self.select_layout = QComboBox()
        self.select_layout.addItems([
            'Fusion', 'Windows', 'Dark', 'DarkGray'
        ])

        allfonts = QFontDatabase().families()
        stringlist = list(allfonts)

        self.select_font = QComboBox()
        self.select_font.setEditable(True)
        self.select_font.addItems(stringlist)
        self.select_size_font = QSpinBox()
        self.select_size_font.setMinimum(10)
        self.select_size_font.setMaximum(100)

        self.select_color = QColorButton()

        vbox.addWidget(self.select_layout)
        group_theme.setLayout(vbox)

        hbox.addWidget(self.select_font)
        hbox.addWidget(self.select_size_font)
        group_font.setLayout(hbox)

        self.text_text = 'Texto', QColorButton()
        self.button_text = 'Botão', QColorButton()
        self.combo_text = 'ComboBox', QColorButton()
        self.spin_text = 'SpinBox', QColorButton()

        widgets = [self.text_text, self.button_text, self.combo_text]
        for widget in widgets:
            form_color.addRow(*widget)
        group_color.setLayout(form_color)

        main_layout.addWidget(group_theme)
        main_layout.addWidget(group_font)
        main_layout.addWidget(group_color)
        main_layout.addStretch()
        main_layout.setAlignment(Qt.AlignLeft)

        self.setLayout(main_layout)

    def setStyle(self, text):
        if text == 'Fusion':
            app.setStyleSheet('')
            app.setStyle(QStyleFactory.create('Fusion'))
            config.select_font.setEnabled(True)
            config.select_size_font.setEnabled(True)
            config.select_color.setEnabled(True)
        elif text == 'Windows':
            app.setStyleSheet('')
            app.setStyle(QStyleFactory.create('Windows'))
            config.select_font.setEnabled(True)
            config.select_size_font.setEnabled(True)
            config.select_color.setEnabled(True)
        elif text == 'Dark':
            app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyqt5'))
            config.select_font.setEnabled(False)
            config.select_size_font.setEnabled(False)
            config.select_color.setEnabled(False)
        elif text == 'DarkGray':
            app.setStyleSheet(qdarkgraystyle.load_stylesheet())
            config.select_font.setEnabled(False)
            config.select_size_font.setEnabled(False)
            config.select_color.setEnabled(False)


    @pyqtSlot(int)
    def setFont(self, val):
        app.setFont(QFont('', val))


    def setFontFamily(self, text):
        app.setFont(QFont(text))


    @pyqtSlot(str)
    def setFontColor(self, color):
        palette.setColor(QPalette.Text, QColor(color))
        palette.setColor(QPalette.ButtonText, QColor(color))
        palette.setColor(QPalette.QProgressBar, QColor(color))
        app.setPalette(palette)
