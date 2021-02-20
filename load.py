#Import Library
from requests.models import LocationParseError
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import * 
from kafka import KafkaConsumer
import re

def subscribe_and_load():
    #Instansiasi Kafka Consumer
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],api_version=(0, 10, 1))
    consumer.subscribe('project_de')
    tweets = []

    #Memasukkan Data ke dalam Database setelah Batas 'Max' Terlampaui
    max = 0
    for i in consumer:
        if max < 50:
            tweets.append(i)
        else:
            break
        max += 1
        print(max)

    #Menghilangkan Suatu Character Tertentu dan Memisahkan Setiap Atribut dari Tweet yang Didapat
    cleanse = [re.sub("b'", "", str(j.value)) for j in tweets]
    split = [k.split(";") for k in cleanse]

    #Instansiasi MySQL Connector
    engine = create_engine('mysql+mysqlconnector://root:@localhost/tweetstream')
    Base = declarative_base()

    #Pembuatan Schema Tabel Tweet
    class Tweet(Base):
        __tablename__ = 'streamprojectde'
        index = Column(Integer, primary_key = True)
        user_id = Column(String(30))
        timestamp = Column(String(25))
        text = Column(String(200))

    #Membuat Table pada Database MySQL, dengan Pengecekan Terlebih Dahulu
    Tweet.__table__.create(bind=engine, checkfirst=True)

    #Memasukkan Data dalam List dan Array
    tweets = []
    idx = 0
    for i in split:
        data = {}
        data['index'] = idx
        data['user_id'] = i[0]
        data['timestamp'] = i[1]
        data['text'] = i[2]
        tweets.append(data)

    #Memasukkan Tweet yang Tersimpan dalam Array Tweets ke Dalam DB dengan Session
    Session = sessionmaker(bind=engine)
    session = Session()
    for i in tweets:
        data = Tweet(**i)
        session.add(data)

    session.commit()
    session.close()

#Function yang Dipanggil untuk Memulai Kode Program dan Loop tanpa Henti
def periodic_work():
    while True:
        subscribe_and_load()

periodic_work()