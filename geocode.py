import _access, datetime, googlemaps, sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db import *

sys.setrecursionlimit(100000)

db = create_engine('sqlite:///data/mke_wibrs_db.db', echo = False)
Session = sessionmaker(bind = db)
session = Session()

access = _access.google_api_token()
gm = googlemaps.Client(key = access[0])

def get_data():
    query = session.query(mke_wibrs_db.incident_number).filter(mke_wibrs_db.x_lon == None,
                                                               mke_wibrs_db.addr != None,
                                                               mke_wibrs_db.zip_code != None)
    raw = query.all()
    data = [int(i[0]) for i in raw]
    return data

def get_geo(data):
    if type(data) is list:
        for num in data:
            get_geo(num)
    else:
        query = session.query(mke_wibrs_db.addr, mke_wibrs_db.zip_code).filter(mke_wibrs_db.incident_number == data)
        raw = query.first()
        addr = (raw.addr, 'Milwaukee, WI')
        f_addr = ' '.join(addr)

        try:
            gc_data = gm.geocode(f_addr)
            x_lon = gc_data[0]['geometry']['location']['lng']
            y_lat = gc_data[0]['geometry']['location']['lat']
            formatted_addr = gc_data[0]['formatted_address']
            update_time = datetime.datetime.now()
            session.query(mke_wibrs_db).filter(mke_wibrs_db.incident_number == data).update({\
            'x_lon':x_lon, 'y_lat':y_lat, 'formatted_addr':formatted_addr, 'update_time':update_time})
            session.commit()
            print(data, ' updated.')
            print(f_addr)
            print(formatted_addr)

        except:
            print(data, ' was not updated.')
            pass

def exe(n):
    data = get_data()
    get_geo(data[:n])

if __name__ == '__main__':
    exe(4700)
