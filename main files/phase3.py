from kafka import KafkaConsumer
import json
import random
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from flask import Flask
from flask import render_template
from flask_wtf.csrf import CSRFProtect

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topics = ['topic1', 'topic2', 'topic3', 'topic4', 'topic5']

# Create Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Read from the beginning of the topic
    enable_auto_commit=True,       # Automatically commit offsets
    group_id='my_consumer_group'   # Consumer group ID
)
# Subscribe to topics
consumer.subscribe(topics)


app = Flask(__name__)
csrf = CSRFProtect(app)
# Set a secret key for your Flask app
app.config['SECRET_KEY'] = 'pailab'
username1={"bilal":"pass"}


dict_topic1={}
dict_topic2={}
dict_topic3={}
dict_topic4={}
dict_topic5={}
count=0
random_records1=[]
random_records2=[]
random_records3=[]
random_records4=[]
random_records5=[]
flagsample1=0
flagsample2=0
flagsample3=0
flagsample4=0
flagsample5=0


def save_home_pagetrack1():
     random_records1 = random.sample(dict_topic1.items(), 3)
     return random_records1

def save_home_pagetrack2():
     random_records2 = random.sample(dict_topic2.items(), 3)
     return random_records2
def save_home_pagetrack3():
     random_records3 = random.sample(dict_topic3.items(), 3)
     return random_records3
def save_home_pagetrack4():
     random_records4 = random.sample(dict_topic4.items(), 3)
     return random_records4
def save_home_pagetrack5():
     random_records5 = random.sample(dict_topic5.items(), 3)
     return random_records5

def sorttopic1(random_records1,dict_topic1):
    for key, value in dict_topic1.items():
         print("number of fav", value[2])
    genres = [record[-1] for record in random_records1] 
    tempdict={}
    score=0
    count=0
    with open("dict1.txt", "w") as file:
        for genre in genres:
            count=count+1
            for key, value in dict_topic1.items():

                # print("genre check ",value, value[3],genre, genre[3] )
                if value[3]==genre[3]:
                    score=score+10
                else:
                    score=score+6
                if value[2]>10:
                    score=score+5
                elif value[2]>5:
                    score=score+3     
                # modified=value+ [score]
                tempdict[key]=value+[score]
                score=0
            sorted_tempdict = sorted(tempdict.items(), key=lambda x: x[1][-1], reverse=True)
            for key, value in sorted_tempdict[:5]:
                file.write(f"{count}:{key}: {value}\n")

            print("Top 5 items:")
            for key, value in sorted_tempdict[:5]:
                print(f"{key}: {value}")
                print(tempdict[key])


def sorttopic2(random_records1,dict_topic1):
    for key, value in dict_topic1.items():
         print("number of fav", value[2])
    genres = [record[-1] for record in random_records1] 
    tempdict={}
    score=0
    count=0
    with open("dict2.txt", "w") as file:
        for genre in genres:
            count=count+1
            for key, value in dict_topic1.items():

                # print("genre check ",value, value[3],genre, genre[3] )
                if value[3]==genre[3]:
                    score=score+10
                else:
                    score=score+6
                if value[2]>10:
                    score=score+5
                elif value[2]>5:
                    score=score+3     
                # modified=value+ [score]
                tempdict[key]=value+[score]
                score=0
            sorted_tempdict = sorted(tempdict.items(), key=lambda x: x[1][-1], reverse=True)
            for key, value in sorted_tempdict[:5]:
                file.write(f"{count}:{key}: {value}\n")

            print("Top 5 items:")
            for key, value in sorted_tempdict[:5]:
                print(f"{key}: {value}")
                print(tempdict[key])


def sorttopic3(random_records1,dict_topic1):
    for key, value in dict_topic1.items():
         print("number of fav", value[2])
    genres = [record[-1] for record in random_records1] 
    tempdict={}
    score=0
    count=0
    with open("dict3.txt", "w") as file:
        for genre in genres:
            count=count+1
            for key, value in dict_topic1.items():

                # print("genre check ",value, value[3],genre, genre[3] )
                if value[3]==genre[3]:
                    score=score+10
                else:
                    score=score+6
                if value[2]>10:
                    score=score+5
                elif value[2]>5:
                    score=score+3     
                # modified=value+ [score]
                tempdict[key]=value+[score]
                score=0
            sorted_tempdict = sorted(tempdict.items(), key=lambda x: x[1][-1], reverse=True)
            for key, value in sorted_tempdict[:5]:
                file.write(f"{count}:{key}: {value}\n")

            print("Top 5 items:")
            for key, value in sorted_tempdict[:5]:
                print(f"{key}: {value}")
                score=0
                print(tempdict[key])


def sorttopic4(random_records1,dict_topic1):
    for key, value in dict_topic1.items():
         print("number of fav", value[2])
    genres = [record[-1] for record in random_records1] 
    tempdict={}
    score=0
    count=0
    with open("dict4.txt", "w") as file:
        for genre in genres:
            count=count+1
            for key, value in dict_topic1.items():

                # print("genre check ",value, value[3],genre, genre[3] )
                if value[3]==genre[3]:
                    score=score+10
                else:
                    score=score+6
                if value[2]>10:
                    score=score+5
                elif value[2]>5:
                    score=score+3     
                # modified=value+ [score]
                tempdict[key]=value+[score]
                score=0
            sorted_tempdict = sorted(tempdict.items(), key=lambda x: x[1][-1], reverse=True)
            for key, value in sorted_tempdict[:5]:
                file.write(f"{count}:{key}: {value}\n")

            print("Top 5 items:")
            for key, value in sorted_tempdict[:5]:
                print(f"{key}: {value}")
                score=0
                print(tempdict[key])

def sorttopic5(random_records1,dict_topic1):
    for key, value in dict_topic1.items():
         print("number of fav", value[2])
    genres = [record[-1] for record in random_records1] 
    tempdict={}
    score=0
    count=0
    with open("dict5.txt", "w") as file:
        for genre in genres:
            count=count+1
            for key, value in dict_topic1.items():

                # print("genre check ",value, value[3],genre, genre[3] )
                if value[3]==genre[3]:
                    score=score+10
                else:
                    score=score+6
                if value[2]>10:
                    score=score+5
                elif value[2]>5:
                    score=score+3     
                # modified=value+ [score]
                tempdict[key]=value+[score]
                score=0
            sorted_tempdict = sorted(tempdict.items(), key=lambda x: x[1][-1], reverse=True)
            for key, value in sorted_tempdict[:5]:
                file.write(f"{count}:{key}: {value}\n")

            print("Top 5 items:")
            for key, value in sorted_tempdict[:5]:
                print(f"{key}: {value}")
                score=0
                print(tempdict[key])



def printrecords(random_records1,random_records2,random_records3,random_records4,random_records5):
    with open("sample.txt", "w") as file:
        file.write("Random Records 1:\n")
        file.write("\n".join(map(lambda x: ' '.join(map(str, x)), random_records1)))
        file.write("\n\n")

        file.write("Random Records 2:\n")
        file.write("\n".join(map(lambda x: ' '.join(map(str, x)), random_records2)))
        file.write("\n\n")

        file.write("Random Records 3:\n")
        file.write("\n".join(map(lambda x: ' '.join(map(str, x)), random_records3)))
        file.write("\n\n")

        file.write("Random Records 4:\n")
        file.write("\n".join(map(lambda x: ' '.join(map(str, x)), random_records4)))
        file.write("\n\n")

        file.write("Random Records 5:\n")
        file.write("\n".join(map(lambda x: ' '.join(map(str, x)), random_records5)))

def printdictionary(dict_topic1,dict_topic2,dict_topic3,dict_topic4,dict_topic5):
    with open("dict.txt", "w") as file:
        file.write("Dictionary Entries:\n\n")

        file.write("Entries for Dictionary Topic 1:\n")
        for key, value in list(dict_topic1.items())[:5]:
            file.write(f"{key}: {str(value)}\n")  # Convert value to string
        file.write("\n")

        file.write("Entries for Dictionary Topic 2:\n")
        for key, value in list(dict_topic2.items())[:5]:
            file.write(f"{key}: {str(value)}\n")  # Convert value to string
        file.write("\n")

        file.write("Entries for Dictionary Topic 3:\n")
        for key, value in list(dict_topic3.items())[:5]:
            file.write(f"{key}: {str(value)}\n")  # Convert value to string
        file.write("\n")

        file.write("Entries for Dictionary Topic 4:\n")
        for key, value in list(dict_topic4.items())[:5]:
            file.write(f"{key}: {str(value)}\n")  # Convert value to string
        file.write("\n")

        file.write("Entries for Dictionary Topic 5:\n")
        for key, value in list(dict_topic5.items())[:5]:
            file.write(f"{key}: {str(value)}\n")  # Convert value to string
        file.write("\n")

     


try:
    # Continuously consume messages
    for message in consumer:

        # Extract topic number from topic name
        topic_number = int(message.topic[-1])
        
        # Decode bytes to string and remove leading/trailing quotes
        received_message_str = message.value.decode('utf-8').strip('"')
        
        # Remove one layer of escaping
        received_message_str = received_message_str.replace('\\"', '"')

        # Parse JSON content
        try:
            track_info = json.loads(received_message_str)
            print("Track information:")
            print(track_info)
            
            # Extract specific fields
            track_id = track_info.get('track_id')
            track_title = track_info.get('track_title')
            artist_name = track_info.get('artist_name')
            track_favorites = track_info.get('track_favorites')
            track_genres = track_info.get('track_genres')
            cleaned_string = track_genres.strip('[]')
            cleaned_string= cleaned_string.strip('{}')
            print(cleaned_string)
            pairs = cleaned_string.split(", ")
            try:
                for pair in pairs:
                    key, value = pair.split(": ")
                    if key.strip() == "'genre_title'":
                        track_genres = value.strip().strip("'")
                        break
            except:
                 continue
            if topic_number==1:
                    print("TOPIC1")
                    dict_topic1[track_id]=[track_title,artist_name,track_favorites,track_genres]
                    # print('wos')
                    # print(dict_topic1[track_id])

                    abc=dict_topic1[track_id]
                    print(abc[3])
            if topic_number==2:
                    print("TOPIC2")
                    dict_topic2[track_id]=[track_title,artist_name,track_favorites,track_genres]
                    # print(dict_topic2)
                    # abc=dict_topic2[track_id]
                    # print(abc[3])
            if topic_number==3:
                    print("TOPIC3")
                    dict_topic3[track_id]=[track_title,artist_name,track_favorites,track_genres]
                    # print(dict_topic3)
                    # print(dict_topic3[track_id])

                    abc=dict_topic3[track_id]
                    print(abc[3])
            if topic_number==4:
                    print("TOPIC4")
                    dict_topic4[track_id]=[track_title,artist_name,track_favorites,track_genres]
                    # print(dict_topic4)
                    # print(dict_topic4[track_id])

            if topic_number==5:
                    print("TOPIC5")
                    dict_topic5[track_id]=[track_title,artist_name,track_favorites,track_genres]
                    # print(dict_topic5)
            # if count == 10:
            #     save_home_pagetrack1()
            print(len(dict_topic1))
            print(len(dict_topic2))
            print(len(dict_topic3))
            print(len(dict_topic4))
            print(len(dict_topic5))

            if len(dict_topic1) >= 4 and not flagsample1:
                random_records1=save_home_pagetrack1()
                flagsample1=1
            if len(dict_topic2) >= 4 and not flagsample2:
                random_records2=save_home_pagetrack2()
                flagsample2=1
            if len(dict_topic3) >= 4 and not flagsample3:
                 random_records3=save_home_pagetrack3()
                 flagsample3=1
            if len(dict_topic4) >= 4 and not flagsample4:
                 random_records4=save_home_pagetrack4()
                 flagsample4=1
            if len(dict_topic5) == 4 and not flagsample5:
                 random_records5=save_home_pagetrack5()
                 flagsample5=1
            
            if (len(dict_topic1)>=10 and len(dict_topic2)>=10 and len(dict_topic3)>=10 and len(dict_topic4)>=10 and len(dict_topic5)>=10):
                 break
            # if len(dict_topic4)>=40:
            #      break
            # print("Topic:", topic_number)
            # print("Track ID:", track_id)
            # print("Track Title:", track_title)
            # print("Artist Name:", artist_name)
            # print("Track Favorites:", track_favorites)
            # print("Track Genres:", track_genres)
            print(random_records1)
            print(random_records2)
            print(random_records3)
            print(random_records4)
            print(random_records5)
        except json.JSONDecodeError as e:
            print("Error decoding JSON:", e)
            continue  # Skip to the next message
    # flask(random_records1,random_records2,random_records3,random_records4,random_records5,dict_topic1,dict_topic2,dict_topic3,dict_topic4,dict_topic5)
    sorttopic1(random_records1,dict_topic1)
    sorttopic2(random_records2,dict_topic2)
    sorttopic3(random_records3,dict_topic3)
    sorttopic4(random_records4,dict_topic4)
    sorttopic5(random_records5,dict_topic5)
    printrecords(random_records1,random_records2,random_records3,random_records4,random_records5)
    # printdictionary(dict_topic1,dict_topic2,dict_topic3,dict_topic4,dict_topic5)

except KeyboardInterrupt:
    # Close the consumer on interrupt (Ctrl+C)
    consumer.close()

