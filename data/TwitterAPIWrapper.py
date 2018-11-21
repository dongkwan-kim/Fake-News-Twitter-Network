import threading

import twitter
import configparser

from utill import wait_second


class TwitterAPIWrapper:

    def __init__(self, config_file_path_or_list: str or list):
        self.is_single = isinstance(config_file_path_or_list, str)

        methods = ["GetFollowerIDsPaged", "GetFriendIDsPaged", "GetUser"]

        self.api, self.apis = None, None
        if self.is_single:
            self.api = self.api_twitter(config_file_path_or_list)
        else:
            self.apis = {self.api_twitter(p): {m: True for m in methods}
                         for p in config_file_path_or_list}

    def api_twitter(self, config_file_path) -> twitter.Api:
        try:
            config = configparser.ConfigParser()
            config.read(config_file_path)
            config_t = config['TWITTER']

            consumer_key = config_t['CONSUMER_KEY']
            consumer_secret = config_t['CONSUMER_SECRET']
            access_token = config_t['ACCESS_TOKEN']
            access_token_secret = config_t['ACCESS_TOKEN_SECRET']

            _api = twitter.Api(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token_key=access_token,
                access_token_secret=access_token_secret
            )
        except Exception as e:
            print('Failed to load Twitter API Configs. Do not worry, you can still use this.\n', str(e))
            _api = None

        return _api

    def _get_available_api(self, method) -> twitter.Api or None:
        for i, (api, method_to_available) in enumerate(self.apis.items()):
            if method_to_available[method]:
                return api
        return None

    def schedule_available_api(self, method, check_interval=15) -> twitter.Api:
        assert not self.is_single

        api = None
        while not api:
            api = self._get_available_api(method=method)
            if not api:
                wait_second(check_interval, with_tqdm=True)

        return api

    def block_api_for_time(self, api_to_block, method_to_block, time_in_sec):

        def toggle_available(apis, api, method):
            apis[api][method] = not apis[api][method]

        toggle_available(self.apis, api_to_block, method_to_block)
        timer = threading.Timer(time_in_sec, toggle_available, [self.apis, api_to_block, method_to_block])
        timer.start()

    def GetFollowerIDsPaged(self, user_id, cursor):
        if self.is_single:
            return self.api.GetFollowerIDsPaged(user_id=user_id, cursor=cursor)
        else:
            api = self.schedule_available_api("GetFollowerIDsPaged")
            self.block_api_for_time(api, "GetFollowerIDsPaged", 60 + 2)
            results = api.GetFollowerIDsPaged(user_id=user_id, cursor=cursor)
            return results

    def GetFriendIDsPaged(self, user_id, cursor):
        if self.is_single:
            return self.api.GetFriendIDsPaged(user_id=user_id, cursor=cursor)
        else:
            api = self.schedule_available_api("GetFriendIDsPaged")
            self.block_api_for_time(api, "GetFriendIDsPaged", 60 + 2)
            results = api.GetFriendIDsPaged(user_id=user_id, cursor=cursor)
            return results

    def GetUser(self, user_id):
        if self.is_single:
            return self.api.GetUser(user_id=user_id)
        else:
            api = self.schedule_available_api("GetUser")
            self.block_api_for_time(api, "GetUser", 1 + 1)
            results = api.GetUser(user_id=user_id)
            return results

    def VerifyCredentials(self):
        try:
            if self.is_single:
                return self.api.VerifyCredentials()
            else:
                return [a.VerifyCredentials() for a in self.apis.keys()]
        except Exception as e:
            return str(e)


if __name__ == '__main__':
    test_api = TwitterAPIWrapper(['./config/config_01.ini', './config/config_02.ini'])
    print(test_api.VerifyCredentials())

    for u in {'836322793', '318956466', '2567151784', '1337170682'}:
        print(test_api.GetFollowerIDsPaged(u, -1))
