import csv, datetime as dt

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db import *

db = create_engine('sqlite:///data/mke_wibrs_db.db', echo = False)
Session = sessionmaker(bind = db)
session = Session()

def get_data(start, end):
    with open('data/wibrs.csv') as data:
        read = csv.reader(data, delimiter = ',')
        record = [i for i in read]
        return record[start:end]

def tr_data(start, end):
    record = get_data(start, end)
    for i in record:
        date_data = i[1]
        f_date = '%Y-%m-%d %H:%M:%S'
        date = datetime.strptime(date_data, f_date)
        update_time = dt.datetime.now()
        insert = mke_wibrs_db(i[0], date, i[2], i[9], None, None, None, i[4], i[6],
                              i[8], i[3], i[12], i[13], i[14], i[15], i[16], i[17],
                              i[18], i[19], i[20], i[21], update_time)
        session.add(insert)
        session.commit()
        print(i[0], ' successfully entered.')

def exe(start, end):
    tr_data(start, end)

if __name__ == '__main__':
    exe(1, 650673)
