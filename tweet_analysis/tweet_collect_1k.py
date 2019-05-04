import json
import urllib
from urllib.request import urlopen

import ijson
import numpy
from sqlalchemy.dialects.mssql.information_schema import columns



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
#object= ijson.items(urlopen('https://00e9e64bac4a62a4eedcc1798f9a915a44a26aa054433c9440-apidata.googleusercontent.com/download/storage/v1/b/kvs-tweet-data/o/bag.json?qk=AD5uMEsuQcbXgx0IF0-L2iVxG4HlemEndwlNrpBspogYDxoJMfCsiGzpBbH8mOmwF8VVnZfw4QhQzU_-b5o5gWvXpxt1-onbx4gKWJXmujFYFI1nKkrQR8Zh6ZshgCw-U7bHeK7ypFkrGReVsh37OjKxi_h0vxCP8hq5yigMYK6oQoEV01fjGKCTRnwwlbvhY0uIEqRK_SGxpgdPRhzEHoLSiiEPz_IqKDrLQQYDu8Z3SZGYlvDHwL9xV1s5RsPmOgciTYtLQUXnf5_-mUwJUeZ9uKUiQKqU0i2d3h7xHiQbEqiuOdgHUgOc6gWuufk753jAbeS7qr76RiU4l-v8P9mhL6XPdJAglXEOTUadCTFG44oxqk_2TA52g7pPfKS3DZhx-GY3zVeCW9C_doFc3glKAVlcAgLSX-_1Yze_BrM6od69o5oGM3vhA9KCwHl4T-tYgMGKf6cXcB65dXMRGSjz3Xulcf7Dh5Rvm6guNbhoKXjyzh0Vth8bSLcHYy8YG99NqgEbn1XvDr2EvjDDl9E0_bigMnAIXWO0ZFD8yHDU80YMiUxCB8zpXEnp9KwtsOjLSFXFn7SMn0kmIshOOjzO7Rpo-MUHfxlIzxDCPmyNi9MjJGqu3gfVffkuDB-XgqiLxfhbXyVjQjwaUz7VkDI2m0K2g2OFqSUVYyHHZemv0iBhBoNLx2jCYIXu6BYFCCGJchVqhiEdgbgRKMQU7xvH4ZQtF1Jh1MaV2QCisI4OEw5r0K2vAaK7dFmP7bbcc-C0ShL7fzpz'),"item")
object=ijson.items(open('1k1k1k.json','r',encoding='utf-8-sig'),"item")
filename1 =open('1k_avg_small.txt','w')
filename2 =open('1k_avg_medium.txt','w')
filename3 =open('1k_avg_large.txt','w')
filename4 =open('1k_std_small.txt','w')
filename5 =open('1k_std_medium.txt','w')
filename6 =open('1k_std_large.txt','w')




# for i in object:
#     print(type(i))
key = ['id_str']

avg_value = ['favorite_count', 'in_reply_to_user_id','user_mentions']
std_value=['created_at','coordinates','user_mentions']

while True:
    try:
        count=count+1
        print(count)
        x = object.__next__()
        key=x[key[0]]
        avg_value_small=x[avg_value[0]]
        avg_value_medium=x[avg_value[1]]
        avg_value_large=x[avg_value[2]]
        std_value_small=x[std_value[0]]
        std_value_medium=x[std_value[1]]
        std_value_large=x[std_value[2]]

        # print(value_std_large)
        # print(type(value_std_large))
        # print('hit')
        # #********* KEY AVG**********
        filename1.write('['+str(key)+'] '+'{'+str(avg_value_small)+'}\n')
        filename2.write('[' + str(key) + '] ' + '{' + str(avg_value_medium) + '}\n')
        filename3.write('<<'+str(key)+'>> '+'(('+str(avg_value_large)+'))\n')
        filename4.write('[' + str(key) + '] ' + '{' + str(std_value_small) + '}\n')
        filename5.write('[' + str(key) + '] ' + '{' + str(std_value_medium) + '}\n')
        filename6.write('<<' + str(key) + '>> ' + '((' + str(std_value_large) + '))\n')
        # bag_key_std_id_test.write('[' +str(key_small)+ '] ' + '{' + str(value_avg_medium) + '}\n')
        # print('hit')
        #
        # #********* KEY STD **********
        #bag_value_std_usermention_test.write('[' + str(key_large) + '] ' + '{' + str(value_std_large) + '}\n')
        #bag_value_std_usermention_test.write(str(value_std_large) + '\n')

        #********* VALUE AVG **********
        # bag_value_avg_favorite_test.write(str(value_avg_small)+'\n')
        # bag_value_avg_retweet_test.write(str(value_avg_medium) + '\n')
        # bag_value_avg_usermention_test.write(str(value_avg_large) + '\n')
        #
        # #********* VALUE STD **********
        # bag_value_std_createdat_test.write(str(value_std_small) + '\n')
        # bag_value_std_retweetcount_test.write(str(value_std_medium) + '\n')
        #filename.write(str(value_std_large) + '\n')



    except:
        print('read Finish')
        filename1.close()
        filename2.close()
        filename3.close()
        filename4.close()
        filename5.close()
        filename6.close()

        #bag_value_std_usermention_test.close()
        break
