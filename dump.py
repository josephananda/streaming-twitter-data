#Import Library
import pandas as pd
import mysql.connector as mysql

#Menghubungkan dengan Database
db = mysql.connect(host='localhost',user='root',passwd='',database='tweetstream')

#Penentuan dan Eksekusi Query
query = "SELECT * FROM streamprojectde"
cursor = db.cursor()
cursor.execute(query)

#Menyimpan Tweet yang Didapat dari Database dalam Bentuk Pandas Dataframe
save = cursor.fetchall()
df = pd.DataFrame(save)

#Menyimpan Dataframe ke dalam Bentuk CSV 
df.to_csv("dump.csv", header=['index','user_id','timestamp','text'], index=False)