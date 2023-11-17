from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
import googleapiclient.discovery 
import pymongo
import psycopg2

def api_connect():
    api = 'AIzaSyBBoG4JuREmMcoZsw18Jj9cj2solBheWwE'
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api)
    return youtube
youtube = api_connect()

def get_channel_details(channel_id):
    request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(channel_name = i['snippet']['title'],
                    channel_Id = i['id'],
                    subscriber = i['statistics']['subscriberCount'],
                    viewCount = i['statistics']['viewCount'],
                    Total_videoCount = i['statistics']['videoCount'],
                    channel_Description = i['snippet']['description'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads']           

    )
    return data

# Getting Video Ids

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
                part = 'ContentDetails',
                id=channel_id
                ).execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response_1 = youtube.playlistItems().list(
                        part='snippet',
                        maxResults=50,
                        playlistId=playlist_id,
                        pageToken =next_page_token).execute()

        for i in range(len(response_1['items'])):
            video_ids.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response_1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# get video_list

def get_video_info(video_details):
    video_data = []
    for video_id in video_details:
        request = youtube.videos().list(
                    part = "snippet,ContentDetails,statistics",
                    id = video_id )
        response = request.execute()
        for item in response['items']:
            data = dict( channel_name = item['snippet']['channelTitle'],
                       channel_id = item['snippet']['channelId'],
                       Video_id = item['id'],
                       video_title = item['snippet']['title'],
                       Tags = item['snippet'].get('tags'),
                       Thumbnails = item['snippet']['thumbnails']['default']['url'],
                       Description= item['snippet'].get('description'),
                       published_date = item['snippet']['publishedAt'],
                       Duration = item['contentDetails']['duration'],
                       Views = item['statistics'].get('viewCount'),
                       Comments = item['statistics'].get('commentCount'),
                       Likes = item['statistics'].get('likeCount'),
                       favorite_count = item['statistics']['favoriteCount'],
                       Definition = item['contentDetails']['definition'],
                       caption_status = item['contentDetails']['caption']
                      )
            video_data.append(data)
            
    return video_data  
        

def get_comment_info(videos_id):
    comment_data=[]
    try:
        for video_id in videos_id:
            request = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults=10
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published_date = item['snippet']['topLevelComment']['snippet']['publishedAt']

                )
                comment_data.append(data)
    except:
        pass
    return comment_data

# Get Playlist details 
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken = next_page_token
        )
        response = request.execute()


        for item in response['items']:
            data = dict (
                playlist_id = item['id'],
                title = item['snippet']['title'],
                channel_id = item['snippet']['channelId'],
                channel_name = item['snippet']['channelTitle'],
                published_at = item['snippet']['publishedAt'],
                video_count = item['contentDetails']['itemCount']
            )
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

# Upload mongo DB

mongodb_url = "mongodb://localhost:27017"
client = pymongo.MongoClient("mongodb://localhost:27017")
db_name = "Youtube_Data"
db = client[db_name]

def channel_det (channel_id):
    ch_details = get_channel_details(channel_id)
    play_details = get_playlist_details(channel_id)
    vid_idss = get_video_ids(channel_id)
    vid_details = get_video_info(vid_idss)
    com_details = get_comment_info(vid_idss)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":play_details,
                       "video_information":vid_details,
                        "comment_information": com_details})
    return "uploaded Successfully"

# Table Creation for channels 
def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Nithyas",
                            database = "Youtube_Data",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
                                                            channel_Id varchar(80) primary key,
                                                            subscriber bigint,
                                                                viewCount bigint,
                                                                Total_videoCount int,
                                                                channel_Description text,
                                                                playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("channel table already created")


    ch_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query = '''insert into channels(channel_name,
                                                channel_Id,
                                                subscriber,
                                                viewCount,
                                                Total_videoCount,
                                                channel_Description,
                                                playlist_id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_Id'],
                row['subscriber'],
                row['viewCount'],
                row['Total_videoCount'],
                row['channel_Description'],
                row['playlist_id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("Channels values are already inserted")
    

# Table creation for playlist 


def playlist_table():
    mydb = psycopg2.connect(host = "localhost",
                                            user = "postgres",
                                            password = "Nithyas",
                                            database = "Youtube_Data",
                                            port = "5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists(playlist_id varchar(100) primary key,
                                                                title varchar(100) ,
                                                                channel_id varchar(100),
                                                                channel_name varchar(100),
                                                                published_at timestamp,
                                                                video_count int
                                                                )'''
    cursor.execute(create_query)
    mydb.commit()

    play_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])

    df1 = pd.DataFrame(play_list)

    for index,row in df1.iterrows():
            insert_query = '''insert into playlists(playlist_id,
                                                    title,
                                                    channel_id,
                                                    channel_name,
                                                    published_at,
                                                    video_count
                                                    )
                                                    
                                                    values(%s,%s,%s,%s,%s,%s)'''
            values=(row['playlist_id'],
                    row['title'],
                    row['channel_id'],
                    row['channel_name'],
                    row['published_at'],
                    row['video_count']
                    )


            cursor.execute(insert_query,values)
            mydb.commit()


# Table for Videos 

def vi_tables():

    mydb = psycopg2.connect(host = "localhost",
                                            user = "postgres",
                                            password = "Nithyas",
                                            database = "Youtube_Data",
                                            port = "5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists videos(channel_name varchar(100),
                                                        channel_id varchar(100) ,
                                                        Video_id varchar(100) primary key,
                                                        video_title varchar(180),
                                                        Tags text,
                                                        Thumbnails varchar(200),
                                                        Description text,
                                                        published_date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Comments int,
                                                        Likes bigint,
                                                        favorite_count int,
                                                        Definition varchar(30),
                                                        caption_status varchar (50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
                insert_query = '''insert into videos(channel_name,
                                                    channel_id,
                                                    Video_id,
                                                    video_title,
                                                    Tags,
                                                    Thumbnails,
                                                    Description,
                                                    published_date,
                                                    Duration,
                                                    Views,
                                                    Comments,
                                                    Likes,
                                                    favorite_count,
                                                    Definition ,
                                                    caption_status  
                                                    )
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['channel_name'],
                        row['channel_id'],
                        row['Video_id'],
                        row['video_title'],
                        row['Tags'],
                        row['Thumbnails'],
                        row['Description'],
                        row['published_date'],
                        row['Duration'],
                        row['Views'],
                        row['Comments'],
                        row['Likes'],
                        row['favorite_count'],
                        row['Definition'],
                        row['caption_status']
                        )


                cursor.execute(insert_query,values)
                mydb.commit()


# Table for Comments 

def comments_table():

    mydb = psycopg2.connect(host = "localhost",
                                            user = "postgres",
                                            password = "Nithyas",
                                            database = "Youtube_Data",
                                            port = "5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        video_Id varchar(50),
                                                        Comment_text text,
                                                        Comment_author varchar(200),
                                                        Comment_Published_date timestamp
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                    video_Id,
                                                    Comment_text,
                                                    Comment_author,
                                                    Comment_Published_date
                                                    )
                                                            
                                                    values(%s,%s,%s,%s,%s)'''
            values=(row['Comment_Id'],
                    row['video_Id'],
                    row['Comment_text'],
                    row['Comment_author'],
                    row['Comment_Published_date']
                    )


            cursor.execute(insert_query,values)
            mydb.commit()


def tables():
    channels_table()
    playlist_table()
    vi_tables()
    comments_table()

    return "Tables created successfully" 

def show_channels_tables():
    ch_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    play_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for play_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])

    df1 = st.dataframe(play_list)

    return df1

def show_videos_table():
    vi_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list= []
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = st.dataframe(com_list)

    return df3

# Streamlit 

st.set_page_config(layout="wide")
st.title(":blue[YOUTUBE DATA HARVESTING & WAREHOUSING]")
st.markdown(" ")


with st.sidebar:
    st.header("Learnings")
    st.caption("Data scarping")
    st.caption("Data Collection")
    st.caption("MongoDB Compass")
    st.caption("API Integrated")
    st.caption("Data using MongoDB & SQL")

channel_id = st.text_input("Enter your Channel id")

if st.button("Submit"): 
    ch_ids=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])

    if channel_id in ch_ids:
        st.success("Given channel details already exists")
    else:
        insert = channel_det(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)

show_table = st.radio("select to view table",("Channels","Playlists","Videos","Comments"))

if show_table == "Channels":
    show_channels_tables()

elif show_table == "Playlists":
    show_playlists_table()

elif show_table == "Videos":
    show_videos_table()

elif show_table == "Comments":
    show_comments_table()



# SQL Connection

mydb = psycopg2.connect(host = "localhost",
                                        user = "postgres",
                                        password = "Nithyas",
                                        database = "Youtube_Data",
                                        port = "5432")
cursor = mydb.cursor()

Question =st.selectbox("Select your question",("1.Names of all the videos and their corresponding channel",
                                                "2. channel with Most No.of.videos & videos available",
                                                "3. 10 Most Viewed videos",
                                                "4. comments in each videos ",
                                                "5. videos with highest likes",
                                                "6. Likes and Dislikes of videos",
                                                "7. Views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10. videos with highest number of comments"))

if Question=="1.Names of all the videos and their corresponding channel":

    query1='''select video_title as videos, channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns=["video title","channelname"])
    st.write(df)

elif Question=="2. channel with Most No.of.videos & videos available":

    query2='''select channel_name as channelname,Total_videoCount as no_of_videos from channels
                order by Total_videoCount desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["channel name","no_of_videos"])
    st.write(df2)

elif Question=="3. 10 Most Viewed videos":
    query3='''select Views as views,channel_name as channelname , video_title as videotitle from videos
                where Views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns = ["views","channelname","videotitle"])
    st.write(df3)

elif Question=="4. comments in each videos ":
    query4='''select Comments as no_of_comment , video_title as videotitle from videos
                    where Comments is not null order by views desc limit 10'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns = ["No ofcomment","videotitle"])
    st.write(df4)

elif Question=="5. videos with highest likes":
    query5='''select video_title as videotitle ,channel_name as channelname, Likes as Likecount from videos
                        where Likes is not null order by Likes desc '''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns = ["videotitle","channelname","Likecount"])
    st.write(df5)

elif Question=="6. Likes and Dislikes of videos":
    query6='''select Likes as Likecount,video_title as videotitle from videos
                        where Likes is not null order by Likes desc '''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns = ["Likecount","videotitle"])
    st.write(df6)

elif Question=="7. Views of each channel":
    query7='''select channel_name as channelname, viewCount as totalviews from channels '''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns = ["channelname","totalviews"])
    st.write(df7)

elif Question=="8. videos published in the year of 2022":
    query8='''select video_title as videotitle,published_date as video_release , channel_name as channelname from videos
                    where extract(year from published_date)=2022 '''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns = ["videotitle","video_release","channelname"])
    st.write(df8)

elif Question=="9. Average duration of all videos in each channel":

    query9='''select channel_name as channelname, AVG(Duration) as averageduration from videos
                    group by channel_name '''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns = ["channelname","average_duration"])

    T9 = []
    for index,row in df9.iterrows():
        channel_title = row["channelname"]
        Average_duration= row["average_duration"]
        average_duration_str = str(Average_duration)
        T9.append(dict(channeltitle = channel_title, Avg_duration = average_duration_str))
    df1 = pd.DataFrame(T9)
    st.write(df1)

elif Question=="10. videos with highest number of comments":
    query10='''select video_title as videotitle, channel_name as channelname, Comments as comments from videos
                    where Comments is not null order by Comments desc '''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns = ["videotitle","channelname","comments"])
    st.write(df10)
