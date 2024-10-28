import requests
import json
import argparse


def vk_request(method, params):
    # функция для запроса к api.vk
    url = f'https://api.vk.com/method/{method}'
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_user_info(user_id, token):
    # функция для сбора информации о пользователе
    params = {
        'user_ids': user_id,
        'fields': 'followers_count',
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('users.get', params)


def get_followers(user_id, token):
    # функция для сбора фолловеров

    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('friends.get', params)


def get_subscriptions(user_id, token):
    # функция для сбора подписок пользователя

    params = {
        'user_id': user_id,
        'extended': 1,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('users.getSubscriptions', params)


def get_groups(user_id, token):
    # функция для сбора групп пользователя

    params = {
        'user_id': user_id,
        'extended': 1,
        'access_token': token,
        'v': '5.131'
    }
    return vk_request('groups.get', params)


def get_numeric_user_id(username, token):
    # функция для получения id пользователя, если у него есть username
    # mr_mousemove -> 371468999

    params = {
        'user_ids': username,
        'access_token': token,
        'v': '5.131'
    }

    response = vk_request('users.get', params)
    return response['response'][0]['id']


def main(user_id, output_path, token):
    if not user_id.isdigit():
        user_id = get_numeric_user_id(user_id, token)
    data = {}
    data['user'] = get_user_info(user_id, token)
    data['followers'] = get_followers(user_id, token)
    data['subscriptions'] = get_subscriptions(user_id, token)
    data['groups'] = get_groups(user_id, token)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="I Steal Your VK Data, hehehehehe")
    parser.add_argument("--vk_token", type=str)
    parser.add_argument("--user_id", type=str, default="mr_mousemove")
    parser.add_argument("--output_path", type=str, default="vk_user_info.json")
    args = parser.parse_args()
    main(args.user_id, args.output_path, args.vk_token)