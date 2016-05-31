import requests
import privateKey as pkey
import json
import time
import logging

logger = logging.getLogger('pagelog')
logger.debug('pagelog')
fh = logging.FileHandler('pagelog.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)



# init
fb_api_url = "https://graph.facebook.com/v2.6/"

def get_refresh_token(live_token):
    token = requests.get("https://graph.facebook.com/oauth/access_token", params = {
            "client_id": pkey.FB_client_id,
            "client_secret": pkey.FB_secret_id,
            "grant_type": "fb_exchange_token",
            "fb_exchange_token": live_token
        })
    if token.ok == True:
        if token.text.find("access_token=") >= 0:
            # 13까지
            if token.text.find("&expires") >= 0:
                last = token.text.find("&expires")
                return token.text[13:last]
            return token.text[13:]
    return ""
def get_fb(id, cmd, params_dic = None):
    params = { "access_token": access_token }
    if params_dic is not None: 
        params.update(params_dic)
    return requests.get(url = fb_api_url + id + "/" + cmd, params = params)
def post_table_log(json_str):
    from azure.storage.table import TableService, Entity
    table_service = TableService(account_name=pkey.azure_storage_id, account_key=pkey.azure_storage_key)
    table_service.create_table("facebooklog")

    def get_table_timestamp_key():
        import time
        current_time = time.gmtime()
        start = time.mktime(current_time)
        last = time.mktime(time.struct_time((2070,1,1,0,0,0,3,100,-1)))
        return str(int(last - start))

    task = Entity()
    task.PartitionKey = 'feedlog'
    task.RowKey = get_table_timestamp_key()
    task.json = json_str
    table_service.insert_entity('facebooklog', task)

access_token = pkey.start_token
dotface_id = "214142335609595"

# main
try:
    while True:
        access_token = get_refresh_token(access_token)
        # Todo : 100개 이상 늘어날 시에 받아오기 위한 작업 필요
        # 예외처리 작업 필요
        try:
            posts_json = get_fb(dotface_id, "posts", {"limit": 100}).json()["data"]
        except Exception as e:            
            logger.critical(e)            
            time.sleep(60 * 1)
            continue


        data = []
        unique_key = "total_video_impressions_viral_unique"
        story_key = "total_video_stories_by_action_type"
        for post_json in posts_json:
            post_id = post_json["id"][16:]
            rq = get_fb(post_id, "video_insights", {"metric": unique_key + "," + story_key})
            # 리턴 값이 없는 경우
            if rq.ok is not True:
                continue
            rq_json = rq.json()
            # 어차피 2개 밖에 없으니
            try:
                if rq_json["data"][0]["name"] == unique_key:
                    viral_json = rq_json["data"][0]
                    stories_json = rq_json["data"][1]
                else:
                    viral_json = rq_json["data"][1]
                    stories_json = rq_json["data"][0]
                    pass
                post_unique = viral_json["values"][0]["value"]
            except Exception as e:
                logger.warn(e)
                post_unique = 0

            # append story
            # 값이 없을 경우, key로 존재하지 않으므로 이를 예외처리할 함수
            def get_story_value(key, rq_json):
                if list(rq_json.keys()).count(key) > 0:
                    return rq_json[key]
                else:
                    return 0
                pass

            try:
                story_value = stories_json["values"][0]["value"]
                post_like = story_value.get("like")
                post_share = story_value.get("share")
                post_comment = story_value.get("comment")
            except Exception as e:
                logger.warn(e)
                post_like = 0
                post_share = 0
                post_comment = 0        
        
            post_msg = post_json.get("message")
            post_created_time = post_json["created_time"]

            data.append((
                post_id, post_like, post_share, post_comment, post_unique, post_msg, post_created_time
                ))
            pass

        json_dump_str = json.dumps(data, ensure_ascii=False).encode('utf8')
        #json.dump(data, open('data/data.json', 'w+'))

        post_table_log(json_dump_str.decode(encoding="utf-8"))
        # test    
        #break
        time.sleep(60 * 10)
        pass
except Exception as e:
    logger.critical(e)

