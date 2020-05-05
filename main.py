import zipfile
import os
import sys
import glob
import shutil
import argparse

from extract import Extract
from db import DBWrapper

parser = argparse.ArgumentParser(description='Обновление данных в рабочей книге')

parser.add_argument('--filename', '-f', required=True, help='Название книги Tableau')
parser.add_argument('--date', '-d', required=True, help='Дата обновления в формате ГГГГ-ММ-ДД')
args = parser.parse_args()
filename = args.filename
date = args.date

folder_name = os.path.splitext(filename)[0]
currentDirectory = os.getcwd()

with zipfile.ZipFile(filename,"r") as zip_ref:
    zip_ref.extractall(folder_name)

path_to_extract = glob.glob(currentDirectory + '/' + folder_name + '/**/*.hyper', recursive=True)[0]

data = DBWrapper().get_new_data(date)
extract = Extract(path_to_extract)
extract.delete_data(date)
extract.insert_data(data)
del extract

archive_name = shutil.make_archive(folder_name, 'zip', folder_name)
os.rename(filename, filename + ".backup")
os.rename(folder_name + ".zip", folder_name + ".twbx")