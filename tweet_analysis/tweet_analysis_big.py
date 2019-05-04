import json
import urllib
from urllib.request import urlopen

import ijson
import numpy
from sqlalchemy.dialects.mssql.information_schema import columns

def analyze_avg(list,key):

    avg=numpy.average(list)


    print(key+' avg:',avg)
    # print('sd:',sd)
    return avg

def analyze_std(list,key):

    sd=numpy.std(list)

    # print('avg:',avg)
    print(key+' sd:',sd)
    return sd




#
id=[]
id_str=[]
urls=[]
created_at=[]
text=[]
truncated=[]
is_quote_status=[]
retweet_count=[]
favorite_count=[]
favorited=[]
retweeted=[]
lang=[]
source=[]
in_reply_to_status_id=[]
in_reply_to_status_id_str=[]
in_reply_to_user_id=[]
in_reply_to_user_id_str=[]
in_reply_to_screen_name=[]
is_quote_status=[]
hashtags=[]
symbols=[]
user_mentions=[]
iso_language_code=[]
result_type=[]
geo=[]
coordinates=[]
place=[]


count=0
object= ijson.items(urlopen('https://00e9e64bac7df15211e0daf81a68c58883e0b841a776389ccd-apidata.googleusercontent.com/download/storage/v1/b/kvs-tweet-data/o/bag.json?qk=AD5uMEuPtT1qfvlr9jZvs115aqJQ6xrLSSxZu5gdq_RUKzXXdgXp894VolB6u7vONaXbIEoHX0cqm4TA9HNMsgIu1T05l8BFYI4ZFPwe61MD6XkiDbGl2i8x94EJ9zTOhhm7--iDkKnoVoJ-AYnHA7oyUDDawOyvrBfjOHsZw-eTQBbXHG2xraWE-8CzxgA7Qz_Hz6wLW7yCli0j69RoY3wLjx5Mi19lz_5QgJXTUt5IQEl-d0wru8iq_64LDTBiAKRhqCCYbY-JNVk3el76zZcDSm5C3n8tGYreMD2dx_cOaKq8tBwM_XiTCyXHTXLDk3KeJPmA-U_fWMiKx0of41fT5qNUEnqh3qgGe7LZpvPZKYLsXaATYIVJet4Qwh3P00KWlKrIjWA8LszBhwAllF7cNVJqb0Cg8EKbxevHY5CNqeZkoigYCndKl_CqPKoleceq2YzwG9hcDGRn9O5sWW7kFSPMhhrE4WZWkVtMffK7_pdh6TprXMagIK_tQDmwU_xjfgFK8vjJsfSBaueN8oyeNA6BUtMblg0XdYTNUq3PcZbqScUHh4y30qOvfptXu0oW6itgp4wNkJpA7K3tMU68rIwvPpcdpoKqOlJcE9by21LSl97YDMb9P4XdX85EyuKn8Er-VRUmLEr70zsINLn37JjoLtWrjlynjNlVZmoIoJzVRR3v6GguECMGPX2LQfjZIxWUabLUxByeYvX8fPAQvlyCIdf8rpGmuHGvIlgIUW5R849NHg8AQUtm4kKpvS-GYuIscSR8'),"item")
#data_file=urlopen('https://00e9e64bac5fdb04da38645cbb8327d70cac0c75c9d397467b-apidata.googleusercontent.com/download/storage/v1/b/kvs-tweet-data/o/here2.json?qk=AD5uMEtoRqMWDrAqeSA-tYEgI8Tif6sL32-vN1XmdBIrZxvSbb60zaBHB1_cfZAlIq2v9XfcNr-1i8B_6vusGde_eBtBqhLIBgksveFcPe2TetsFHowH4ntumBXHOYEQeQ1JT4yMWC65zSunoE2jO5paPOGLm5cVuU_DS8UupP8BfWUlKKiixOmOKKSemlSbiw6EbninjieaOdiYgygtDVddpsxH2VzLEMRTaTfMmM1l4AOsefAs0J5I2ezphK3ZvMqN_hzL4wUFwmfYmErdAC7J7R2PwrSvNFr0yUN4mwmVaFRyej3wk-Wv_t8hqVQj_nNSpGy4yBWJoZYhVyNCisBXRlKTiFEhCMJSpchUnosnBVmHFcGrSMofIFCM_Bdlui8KjmyRBRrp_jJuGWyHV7nV4pkjX_ZIzO6fX9deGwBXApfsZtTu3fihuyKJKag9p8q2TnXTjP-nMWUL18s7NHYYwT_eMnddcNe9KnC5AsXOAdyL3sRkXstT_ebUgJOUhbUusT1yBFfWX_L0Gp8HXn28qtj4s4T7RaYJUYoGJJVOExKvYX89lMoXS4QiL2H_k0k9OTTw4BBBI5iE6xOnSYwr3eyRfbdI-FYemMbkGL1k-2eu8Ea3swsIIvN7yOEqscRTy-37aSZxVut0nqMxc4Qybsvyqu7TiWrsCI8wl2a7pluA2ZUdP-Qj-0UO6Qb_lfkDAB1D2OYRcPoY62Iiu0J9-JWoC15XBoKB6DEvgM_z3-yizTOe2hN4nqkyT8otb0ZYKhiCIBYI')


# for i in object:
#     print(type(i))
key_cadidate = ['id', 'id_str', 'urls']

value = ['created_at', 'text', 'truncated', 'is_quote_status', 'retweet_count',
         'favorite_count', 'favorited', 'retweeted', 'lang', 'source', 'in_reply_to_status_id',
         'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str',
         'in_reply_to_screen_name', 'is_quote_status', 'hashtags', 'symbols', 'user_mentions',
         'iso_language_code', 'result_type', 'geo', 'coordinates', 'place']


while True:
    try:
        count=count+1
        print(count)
        x = object.__next__()
        print(x['id'])
        id.append(len(str(x[key_cadidate[0]])))
        id_str.append(len(str(x[key_cadidate[1]])))
        urls.append(len(str(x[key_cadidate[2]])))

        created_at.append(len(str(x[value[0]])))
        text.append(len(str(x[value[1]])))
        truncated.append(len(str(x[value[2]])))
        is_quote_status.append(len(str(x[value[3]])))
        retweet_count.append(len(str(x[value[4]])))
        favorite_count.append(len(str(x[value[5]])))
        favorited.append(len(str(x[value[6]])))
        retweeted.append(len(str(x[value[7]])))
        lang.append(len(str(x[value[8]])))
        source.append(len(str(x[value[9]])))
        in_reply_to_status_id.append(len(str(x[value[10]])))
        in_reply_to_status_id_str.append(len(str(x[value[11]])))
        in_reply_to_user_id.append(len(str(x[value[12]])))
        in_reply_to_user_id_str.append(len(str(x[value[13]])))
        in_reply_to_screen_name.append(len(str(x[value[14]])))
        is_quote_status.append(len(str(x[value[15]])))
        hashtags.append(len(str(x[value[16]])))
        symbols.append(len(str(x[value[17]])))
        user_mentions.append(len(str(x[value[18]])))
        iso_language_code.append(len(str(x[value[19]])))
        result_type.append(len(str(x[value[20]])))
        geo.append(len(str(x[value[21]])))
        coordinates.append(len(str(x[value[22]])))
        place.append(len(str(x[value[23]])))

    except:
        print('read Finish')
        break

print('*****result about key avg********\n\n\n')
analyze_avg(id,'id')
analyze_avg(id_str,'id_str')
analyze_avg(urls,'urls')
print('*****result about key std********\n\n\n')

analyze_std(id,'id')
analyze_std(id_str,'id_str')
analyze_std(urls,'urls')

print('\n\n\n*****result about value avg********')
analyze_avg(created_at,'created_at')
analyze_avg(text,'text')
analyze_avg(truncated,'truncated')
analyze_avg(is_quote_status,'is_quote_status')
analyze_avg(retweet_count,'retweet_count')
analyze_avg(lang,'lang')
analyze_avg(source,'source')
analyze_avg(in_reply_to_status_id,'in_reply_to_status_id')
analyze_avg(in_reply_to_status_id_str,'in_reply_to_status_id_str')
analyze_avg(in_reply_to_user_id,'in_reply_to_user_id')
analyze_avg(in_reply_to_user_id_str,'in_reply_to_user_id_str')
analyze_avg(in_reply_to_screen_name,'in_reply_to_screen_name')
analyze_avg(is_quote_status,'is_quote_status')
analyze_avg(hashtags,'hashtags')
analyze_avg(symbols,'symbols')
analyze_avg(user_mentions,'user_mentions')
analyze_avg(iso_language_code,'iso_language_code')
analyze_avg(result_type,'result_type')
analyze_avg(geo,'geo')
analyze_avg(coordinates,'coordinates')
analyze_avg(place,'place')
analyze_avg(favorite_count,'favorite_count')
analyze_avg(favorited,'favorited')
analyze_avg(retweeted,'retweeted')

print('\n\n\n*****result about value std********')

analyze_std(created_at,'created_at')
analyze_std(text,'text')
analyze_std(truncated,'truncated')
analyze_std(is_quote_status,'is_quote_status')
analyze_std(retweet_count,'retweet_count')
analyze_std(lang,'lang')
analyze_std(source,'source')
analyze_std(in_reply_to_status_id,'in_reply_to_status_id')
analyze_std(in_reply_to_status_id_str,'in_reply_to_status_id_str')
analyze_std(in_reply_to_user_id,'in_reply_to_user_id')
analyze_std(in_reply_to_user_id_str,'in_reply_to_user_id_str')
analyze_std(in_reply_to_screen_name,'in_reply_to_screen_name')
analyze_std(is_quote_status,'is_quote_status')
analyze_std(hashtags,'hashtags')
analyze_std(symbols,'symbols')
analyze_std(user_mentions,'user_mentions')
analyze_std(iso_language_code,'iso_language_code')
analyze_std(result_type,'result_type')
analyze_std(geo,'geo')
analyze_std(coordinates,'coordinates')
analyze_std(place,'place')
analyze_std(favorite_count,'favorite_count')
analyze_std(favorited,'favorited')
analyze_std(retweeted,'retweeted')

