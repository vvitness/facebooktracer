import requests
import privateKey as pkey
import json
import time

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
while True:
    access_token = get_refresh_token(access_token)
    # Todo : 100개 이상 늘어날 시에 받아오기 위한 작업 필요
    posts_json = get_fb(dotface_id, "posts", {"limit": 100}).json()["data"]

    data = []
    unique_key = "total_video_impressions_viral_unique"
    story_key = "total_video_stories_by_action_type"
    for post_json in posts_json:
        post_id = post_json["id"][16:]
        rq = get_fb(post_id, "video_insights", {"metric": unique_key + "," + story_key})
        # 리턴 값이 없는 경우
        if rq.ok is not True:
            continue
        json = rq.json()
        # 어차피 2개 밖에 없으니
        if json["data"][0]["name"] == unique_key:
            viral_json = json["data"][0]
            stories_json = json["data"][1]
        else:
            viral_json = json["data"][1]
            stories_json = json["data"][0]
            pass
        # append story
        # 값이 없을 경우, key로 존재하지 않으므로 이를 예외처리할 함수
        def get_story_value(key, json):
            if list(json.keys()).count(key) > 0:
                return json[key]
            else:
                return 0
            pass
        story_value = stories_json["values"][0]["value"]

        post_like = story_value.get("like")
        post_share = story_value.get("share")
        post_comment = story_value.get("comment")
        post_unique = viral_json["values"][0]["value"]
        post_msg = post_json.get("message")
        post_created_time = post_json["created_time"]

        data.append((
            post_id, post_like, post_share, post_comment, post_unique, post_msg, post_created_time
            ))
        pass

    json_dump_str = json.dumps(data)
    #json.dump(data, open('data/data.json', 'w+'))

    post_table_log(json_dump_str)
    # test    
    time.sleep(10)
    pass
