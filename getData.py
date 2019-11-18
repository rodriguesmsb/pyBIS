import ftplib as ftp
import glob as gb

class dataSus:

    #Start the class with main directory of the databases
    def __init__(self, page = "ftp.datasus.gov.br"):
        self.data_sus = ftp.FTP(page)
        self.data_sus.login()
        self.data_sus.cwd("/dissemin/publicos")


    #Crete a method to choose specific data set
    def select_data(self, data):
        data = data.upper()


        #Point to the correct directory for every dataset
        #only the most update repo will be avaliable from dowload
        if data == "SINASC":
            self.data_sus.cwd(data + "/1996_/Dados/DNRES")
        elif data == "SIHSUS":
            self.data_sus.cwd(data + "/200801_/Dados/")
        elif data == "SIM":
            self.data_sus.cwd(data + "/1996_/Dados/DNRES")


    #Show atributes
    def attributes(self):
        print(self.data_sus.dir())

   #Atribbutes to pandas
        


     
        
 #Example
new_data = dataSus()
new_data.select_data("SINASC")
new_data.attributes()
