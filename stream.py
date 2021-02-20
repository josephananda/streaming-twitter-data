#Import Library
import tweepy
from kafka import KafkaProducer
from datetime import datetime, timedelta

#Twitter API Setup
consumer_key = "ip0XSBcJnsTz2THQgELyCPDF3"
consumer_secret = "5W1Fcbgi9cEQvU9QOp8eV8IUTwavpAwyTAdamHUi8k2lHuawC1"
access_token = "879466375932137472-5sRBD8h5pi6ELOUY5XIzK8IPJ8alw6S"
access_token_secret = "g872IGwsxsHoNyKG0mvJDat7Ivjl7Jr5uzucfmyyhXh3x"

#Setup Autentikasi
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

#Instansiasi API
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#Menyesuaikan waktu dengan waktu lokal (GMT +7 / UTC +7)
def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=7)
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))

#Instansiasi Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0, 10, 1))

#Inisialisasi Topik, Kata kunci, serta Batas Maksimum Query dan Tweet
topic_name = 'project_de'
search_key = "indihome"
maxId = -1
maxTweets = 30000
tweetCount = 0
tweetsPerQuery = 200

#Perulangan untuk mendapatkan Tweet dengan API Twitter hingga Limit yang Ditentukan
while tweetCount < maxTweets:
    #Mengambil Data Tweet Pertama Kali
    if maxId <= 0 :
        newTweets = api.search(q=search_key, lang="id", count=tweetsPerQuery, result_type="recent", tweet_mode = "extended")
    
    #Mengambil Data Tweet Kedua dan Seterusnya
    newTweets = api.search(q=search_key, lang="id", count=tweetsPerQuery, result_type="recent", tweet_mode = "extended", max_id=str(maxId-1))
    
    #Mengambil Atribut Tertentu dari Suatu Tweet
    for i in newTweets:
        record = str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.full_text.encode('utf-8'))
        record += ';'
        print(str.encode(record))
        producer.send(topic_name, str.encode(record))

    #Menambah Jumlah TweetCount dan MaxId
    tweetCount += len(newTweets)	
    maxId = newTweets[-1].id