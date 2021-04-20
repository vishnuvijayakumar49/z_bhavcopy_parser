import schedule, redis
import time, requests, redis, zipfile, io, json
from datetime import datetime

def parser():
  time_stamp = datetime.today().strftime('%d%m%y')
  header = { 
             'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
           }
  url = f'https://www.bseindia.com/download/BhavCopy/Equity/EQ{time_stamp}_CSV.ZIP'
  redis_conn = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
  file_name = f'bhav_files/EQ{time_stamp}.CSV'
  download_file(url, header)
  store_data(redis_conn, file_name)
  time.sleep(10)

def store_data(redis_conn, file):
  redis_conn.delete('bhav_copy_data')
  with open(file, "r") as bhav_file:
    headers = line_to_arr(bhav_file.readline())
    with redis_conn.pipeline() as pipe:
      for line in bhav_file:
        line_dict = filter_data(headers, line)
        pipe.hset('bhav_copy_data', line_dict["SC_NAME"], json.dumps(line_dict))
      pipe.execute()

def filter_data(headers, line):
  line_dict = dict(zip(headers, line_to_arr(line)))
  allowed_keys = ['SC_CODE','SC_NAME','OPEN','HIGH','LOW','CLOSE']
  return {key: value for key, value in line_dict.items() if key in allowed_keys}

def line_to_arr(line):
  return [column.strip() for column in line.split(',')]

def download_file(url, header):
  response = requests.get(url, stream=True, headers=header)
  z = zipfile.ZipFile(io.BytesIO(response.content))
  z.extractall('bhav_files/')

schedule.every().day.at("18:30").do(parser)
schedule.every(5).seconds.do(parser)

while 1:
  schedule.run_pending()
  print('check')
  time.sleep(1)
