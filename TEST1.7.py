from chat_downloader import ChatDownloader
from api_keys import *
import numpy as np
import math
import pandas as pd
import requests
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from dateutil import parser
import seaborn as sns
from scipy import stats
CONVERSION_RATES = {"USD":1,"AED":3.6725,"AFN":67.5832,"ALL":90.3602,"AMD":387.1848,"ANG":1.7900,"AOA":913.6107,"ARS":979.4200,"AUD":1.4857,"AWG":1.7900,"AZN":1.6999,"BAM":1.7895,"BBD":2.0000,"BDT":119.5158,"BGN":1.7895,"BHD":0.3760,"BIF":2910.5749,"BMD":1.0000, "BND":1.3066, "BOB":6.9232,"BRL":5.6019,"BSD":1.0000,"BTN":84.1756,"BWP":13.2510,"BYN":3.2747,"BZD":2.0000,"CAD":1.3780,"CDF":2855.2500,"CHF":0.8579,"CLP":928.9125,"CNY":7.0850,"COP":4198.9862,"CRC":517.7377,"CUP":24.0000,"CVE":100.8886,"CZK":23.1686,"DJF":177.7210,"DKK":6.8263,"DOP":60.2102,"DZD":133.1360,"EGP":48.5653,"ERN":15.0000,"ETB":117.4351,"EUR":0.9151,"FJD":2.2224,"FKP":0.7660,"FOK":6.8261,"GBP":0.7661,"GEL":2.7265,"GGP":0.7660,"GHS":15.9449,"GIP":0.7660,"GMD":70.5683,"GNF":8715.2710,"GTQ":7.7343, "GYD":209.1944,"HKD":7.7713,"HNL":24.9591,"HRK":6.8938,"HTG":131.7028,"HUF":367.1316,"IDR":15590.5301,"ILS":3.7629,"IMP":0.7660,"INR":84.1638,"IQD":1307.2191,"IRR":42069.7977,"ISK":136.4796,"JEP":0.7660,"JMD":158.4402,"JOD":0.7090,"JPY":149.2442,"KES":129.0049,"KGS":85.1773,"KHR":4076.7905,"KID":1.4853,"KMF":450.1330,"KRW":1349.2144,"KWD":0.3063,"KYD":0.8333,"KZT":484.1603,"LAK":21907.9033,"LBP":89500.0000,"LKR":292.6606,"LRD":193.0175,"LSL":17.4429,"LYD":4.7916,"MAD":9.8092,"MDL":17.5829,"MGA":4567.1018,"MKD":56.2049,"MMK":2096.3693,"MNT":3379.7236,"MOP":8.0038,"MRU":39.7586,"MUR":46.0454,"MVR":15.4385,"MWK":1739.3886,"MXN":19.3270,"MYR":4.2876,"MZN":63.8775,"NAD":17.4429,"NGN":1620.6519,"NIO":36.8139,"NOK":10.7321,"NPR":134.6810,"NZD":1.6413,"OMR":0.3845,"PAB":1.0000,"PEN":3.7288,"PGK":3.9299,"PHP":57.2356,"PKR":277.8468,"PLN":3.9292,"PYG":7823.6184,"QAR":3.6400,"RON":4.5483,"RSD":106.9944,"RUB":95.8397,"RWF":1354.6476,"SAR":3.7500,"SBD":8.5052,"SCR":14.4395,"SDG":458.7205,"SEK":10.3903,"SGD":1.3070,"SHP":0.7660,"SLE":23.1089,"SLL":23108.8851,"SOS":571.6493,"SRD":32.0109,"SSP":3210.0893,"STN":22.4166,"SYP":12871.7051,"SZL":17.4429,"THB":33.2400,"TJS":10.6742,"TMT":3.4996,"TND":3.0720,"TOP":2.3270,"TRY":34.2962,"TTD":6.7810,"TVD":1.4853,"TWD":32.1563,"TZS":2714.7846, "UAH":41.2146,"UGX":3662.3142,"UYU":41.6690,"UZS":12750.7805,"VES":38.8857,"₫":24822.1094,"VUV":117.9734,"WST":2.7084,"XAF":600.1773,"XCD":2.7000, "XDR":0.7464, "XOF":600.1773,"XPF":109.1845,"YER":250.3202,"ZAR":17.4443,"ZMW":26.4406,"ZWL":6.3309}
CONVERSION_RATES["₱"] = CONVERSION_RATES["PHP"]
MEMBERSHIP_VALUE = 4.99 #value taken on oct 15 2024

def get_superchats(url):
    print(url)
    '''string url -> tuple (# of superchats, $ value of superchats, superchat_timestamps)'''
    chat = ChatDownloader().get_chat(url,message_groups=['superchat'])       # create a generator
    total_superchat_earnings_usd = 0.0
    total_num_chats = 0
    original_value = []
    currency = []
    usd_value = []
    timestamps = []

    for message in chat:                        # iterate over messages
        if message['message_type'] == 'paid_message' or message['message_type'] == 'paid_sticker':
            # superchat
            message_money = message['money']['amount']
            message_currency = message['money']['currency']
            usd_msg = message_money / CONVERSION_RATES[message_currency]
            #print("message got %f %s = %f USD"%(message_money, message_currency, usd_msg))
            total_superchat_earnings_usd += usd_msg
            timestamps.append(message["timestamp"])
            original_value.append(message_money)
            currency.append(message_currency)
            usd_value.append(usd_msg)

        elif message['message_type'] == 'membership_item':
            MEMBERSHIP_VALUE = 4.99
            total_superchat_earnings_usd += MEMBERSHIP_VALUE
            timestamps.append(message["timestamp"]) 
            original_value.append(MEMBERSHIP_VALUE)
            currency.append('USD')
            usd_value.append(MEMBERSHIP_VALUE)

        else:
            print("unknown message type: ", message['message_type'])
            print(message)
        total_num_chats += 1
    return (total_num_chats, total_superchat_earnings_usd, timestamps, original_value, currency, usd_value)

def get_last_50_videos(channel_id):
    search_api_str = "https://youtube.googleapis.com/youtube/v3/search?part=id&channelId={0}&type=video&eventType=completed&maxResults=50&key={1}"
    search_deet = search_api_str.format(channel_id, YOUTUBE_API_KEY)
    response = requests.get(search_deet).json()
    
    video_ids = [item['id']['videoId'] for item in response['items']]
    return video_ids

def get_video_details(vid_id):
    vid_details_template = "https://youtube.googleapis.com/youtube/v3/videos?part=liveStreamingDetails%2Cstatistics%2Cstatus%2CtopicDetails%2Clocalizations%2Csnippet%2CcontentDetails&id={0}&key={1}"
    deet3 = vid_details_template.format(vid_id, YOUTUBE_API_KEY)
    video_details = requests.get(deet3).json()
    return video_details

def get_all_vids_details(channel_videos):
    out = []
    for vid_id in channel_videos:
        a = get_video_details(vid_id)
        if 'liveStreamingDetails' in a['items'][0]:  # Ensures it's a livestream with potential chat replay
            out.append(a)
    return out

def main():

    c_names = pd.read_csv("7.csv", names=["vtuber_name", "affiliation", "channel_id","gender","language"])
    df = pd.DataFrame(columns = ['channel_name', 'channel_id', 'video_name', 'video_id', 'description', 'published_at',
                                'video_start_time', 'video_end_time', 'video_length', 'num_superchats', 'val_superchats',
                                'locale', 'viewcount', 'tags', 'timestamps','original_value', 'currency', 'usd_value','total_views', 'subscriber_count', 'gender', 'language', 'affiliation'])

    c_names.head()
    gender = c_names['gender'].tolist()
    language = c_names['language'].tolist()
    affiliation = c_names['affiliation'].tolist()
    videos_processed = 0
    for index, row in c_names.iterrows():

        print(row['vtuber_name'], row['channel_id'])

        #channel's stats, total views and subcount
        total_views, subscriber_count = get_channel_stats(row['channel_id'])

        # get their videos
        last_50_vids = get_last_50_videos(row['channel_id'])
        all_vids_details = get_all_vids_details(last_50_vids)

        for vid in all_vids_details:

            item = vid["items"][0]
            try:
                # Extract video start and end times
                video_start_time = item['liveStreamingDetails']['actualStartTime']
                video_end_time = item['liveStreamingDetails']['actualEndTime']

                # Convert them to datetime objects
                start_time_dt = parser.parse(video_start_time)
                end_time_dt = parser.parse(video_end_time)

                # Calculate the video length (in seconds)
                video_length_seconds = (end_time_dt - start_time_dt).total_seconds()
                video_length = video_length_seconds / 60

                data_we_want = [row['vtuber_name'], row['channel_id'],
                                item['snippet']['title'],
                                item['id'],
                                item['snippet']['description'],
                                item['snippet']['publishedAt'],
                                item['liveStreamingDetails']['actualStartTime'],
                                item['liveStreamingDetails']['actualEndTime'],
                                video_length, # video length in minutes
                                0, # num_superchats, to be computed next
                                0.0, # val_superchats, to be computed next
                                'ja', # language
                                item['statistics']['viewCount'],
                                [], # topic
                                [], # superchat timestamps - to be computed next
                                [], # original value of superchat in native currency
                                [], # type of native currency
                                [],# value of the superchat converted to usd
                                total_views,
                                subscriber_count,  # Channel's subscriber count (added here)
                                gender[index],  # Gender
                                language[index],  # Language
                                affiliation[index]
                            ]

                if 'defaultAudioLanguage' in item['snippet']:
                    data_we_want[11] = item['snippet']['defaultAudioLanguage']

                if 'topicDetails' in item and 'topicCategories' in item['topicDetails']:
                    data_we_want[13] = [i.replace('https://en.wikipedia.org/wiki/','') for i in item['topicDetails']['topicCategories']]
                yt_url = 'https://www.youtube.com/watch?v='+item['id']
                total_num_chats, total_superchat_earnings_usd, timestamps, original_value, currency, usd_value = get_superchats(yt_url)

                data_we_want[9] = total_num_chats
                data_we_want[10] = total_superchat_earnings_usd
                data_we_want[14] = timestamps
                data_we_want[15] = original_value
                data_we_want[16] = currency
                data_we_want[17] = usd_value

                df.loc[len(df)] = data_we_want
                videos_processed += 1
                print(videos_processed, data_we_want)

            except Exception as e:
                print(f"Exception Type: {type(e).__name__}")
                print(f"Exception Message: {e}")
            continue
        print(data_we_want)
        df.to_csv('./data_csvs1/data_'+str(index)+'.csv', index=False)

main()
