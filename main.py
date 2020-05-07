import os
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

# Файл twbx - архив zip, который можно распаковать с помощью любого архиватора
shutil.unpack_archive(filename, extract_dir=folder_name, format='zip')

path_to_extract = glob.glob(currentDirectory + '/' + folder_name + '/**/*.hyper', recursive=True)[0]

# получаем данные после нужной нам даты
data = DBWrapper().get_new_data(date)

# подключаемся к экстракту, удаляем старые данные и добавляем новые
extract = Extract(path_to_extract)
extract.delete_data(date)
extract.insert_data(data)
del extract

# После того, как данные в экстракте обновлены, его нужно снова запаковать и поменять расширение с .zip на .twbx
archive_name = shutil.make_archive(folder_name, 'zip', folder_name)
os.rename(filename, filename + ".backup") # далаем бэкап предыдущей версии отчета
os.rename(folder_name + ".zip", folder_name + ".twbx")