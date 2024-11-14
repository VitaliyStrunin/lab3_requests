import argparse
import asyncio
import aiohttp
import logging

import neo4j.exceptions
from neo4j import GraphDatabase

semaphore = asyncio.Semaphore(100)  # семафорчик, чтобы не возникала ошибка с переполнением буфера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# без асинка ждать приходится миллион лет
async def vk_request(method, params):
    url = f'https://api.vk.com/method/{method}'
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        await asyncio.sleep(1.5) # VK API ограничивает запросы, делаем задержку, чтобы не было ошибок от сервера
        async with session.get(url, params=params) as response:
            data = await response.json()
            if 'error' in data:
                error_code = data['error'].get('error_code')
                profile_closed_code = 30 # профиль закрыт - инфу не достать
                profile_unavailable = 18 # профиль забанен или удалён - инфу не достать
                if error_code not in (profile_closed_code, profile_unavailable): # не логгируем ошибку доступа закрытым профилям
                    logger.error(f"VK API Error: {data['error']}")
            return data

async def get_user_info(user_id, token):
    params = {
        'user_ids': user_id,
        'access_token': token,
        'v': '5.131',
        'fields': 'screen_name,first_name,last_name,sex,home_town,city'
    }
    async with semaphore:
        user_info = await vk_request('users.get', params)
        return user_info.get('response', [{}])[0]

async def get_user_followers(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131',
        'count': 1000
    }
    async with semaphore:
        followers_data = await vk_request('users.getFollowers', params)
        return followers_data.get('response', {}).get('items', [])

async def get_user_subscriptions(user_id, token):
    params = {
        'user_id': user_id,
        'access_token': token,
        'v': '5.131',
        'extended': 1,
        'fields': 'screen_name,name',
        'count': 50
    }
    async with semaphore:
        subscriptions_data = await vk_request('users.getSubscriptions', params)
        return subscriptions_data.get('response', {}).get('items', [])

def save_user_to_neo(transaction, user):
    transaction.run(
        """
        
        MERGE (u:User {id: $id})
        SET u.screen_name = $screen_name, u.name = $name, u.sex = $sex, 
            u.home_town = COALESCE($home_town, $city)
        """,
        id=user['id'], screen_name=user.get('screen_name'),
        name=f"{user['first_name']} {user['last_name']}",
        sex=user.get('sex'), home_town=user.get('home_town'),
        city=user.get('city', {}).get('title')
    )

def save_group_to_neo(transaction, group):
    transaction.run(
        """
        MERGE (g:Group {id: $id})
        SET g.name = $name, g.screen_name = $screen_name
        """,
        id=group['id'], name=group.get('name'), screen_name=group.get('screen_name')
    )

def save_follow_relation(transaction, user_id, follower_id):
    transaction.run(
        """
        MATCH (u1:User {id: $user_id})
        MATCH (u2:User {id: $follower_id})
        MERGE (u2)-[:Follow]->(u1)
        """,
        user_id=user_id, follower_id=follower_id
    )

def save_subscription_relation(transaction, user_id, subscription):
    '''
    Проблемы со связями:
    1. Друзей в фолловерах нет
    2. В профиле можно закрыть возможность добавить в друзья, оставив возможность подписки
    3. Объектов типа "Group" очень мало, большинство пабликов - публичные страницы (Page)
    '''

    sub_id = subscription['id']
    sub_name = subscription.get('name', f"{subscription.get('first_name', '')} {subscription.get('last_name', '')}".strip())
    sub_screen_name = subscription.get('screen_name')
    sub_type = subscription.get('type')
    if sub_type == 'group':
        query = """
        MERGE (g:Group {id: $sub_id})
        SET g.name = $sub_name, g.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(g)
        """
    elif sub_type == 'page':
        query = """
        MERGE (p:Page {id: $sub_id})
        SET p.name = $sub_name, p.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(p)
        """
    elif sub_type == 'profile':
        query = """
        MERGE (p:User {id: $sub_id})
        SET p.name = $sub_name, p.screen_name = $sub_screen_name
        MERGE (u:User {id: $user_id})
        MERGE (u)-[:Subscribe]->(p)
        """
    else:
        logger.warning(f"Unknown subscribe type: {sub_type}")
        return
    transaction.run(query, user_id=user_id, sub_id=sub_id, sub_name=sub_name, sub_screen_name=sub_screen_name)


async def collect_data_async(user_id, token, session, depth=2):
    if depth < 0:
        return
    user_data = await get_user_info(user_id, token)
    if not user_data:
        logger.error(f"User data collection failed: {user_id}")
        return
    session.execute_write(save_user_to_neo, user_data)
    followers = await get_user_followers(user_id, token)
    subscriptions = await get_user_subscriptions(user_id, token)
    tasks = []
    for follower_id in followers:
        session.execute_write(save_follow_relation, user_id, follower_id)
        tasks.append(collect_data_async(follower_id, token, session, depth - 1)) # рекурсивный обход по фолловерам
    for subscription in subscriptions:
        session.execute_write(save_subscription_relation, user_id, subscription)

    await asyncio.gather(*tasks)

def get_users_count(transaction):
    result = transaction.run("MATCH (u:User) RETURN count(u) AS users_count")
    return result.single()["users_count"]

def get_top_followers(transaction):
    result = transaction.run("""
        MATCH (u:User)<-[:Follow]-(f:User)
        RETURN u.id AS user_id, u.screen_name AS screen_name, u.name AS name, count(f) AS followers_count
        ORDER BY followers_count DESC
        LIMIT 5
    """)
    return [{"user_id": record["user_id"], "screen_name": record["screen_name"], "name": record["name"], "followers_count": record["followers_count"]} for record in result]

def get_top_pages(transaction):
    result = transaction.run("""
        MATCH (p:Page)<-[:Subscribe]-(u:User)
        RETURN p.id AS page_id, p.name AS name, p.screen_name AS screen_name, count(u) AS subscribers_count
        ORDER BY subscribers_count DESC
        LIMIT 5
    """)
    return [{"page_id": record["page_id"], "name": record["name"], "screen_name": record["screen_name"], "subscribers_count": record["subscribers_count"]} for record in result]

async def get_numeric_user_id(username, token):
    params = {
        'user_ids': username,
        'access_token': token,
        'v': '5.131'
    }
    response = await vk_request('users.get', params)

    if 'error' in response:
        logger.error(f"VK API Error: {response['error']}")
        return None
    try:
        return response['response'][0]['id']
    except (KeyError, IndexError):
        logger.error("Unexpected response structure: 'response' key not found.")
        return None

async def main():
    parser = argparse.ArgumentParser(description="I Steal Your VK Data, hehehehehe 2")
    parser.add_argument("--vk_token", type=str, default="")
    parser.add_argument("--user_id", type=str, default="mr_mousemove")
    parser.add_argument("--db_uri", type=str, default="neo4j://localhost:7687")
    parser.add_argument("--db_user", type=str, default="neo4j")
    parser.add_argument("--db_password", type=str, default="")
    parser.add_argument("--collect_data", type=bool, default=False)
    parser.add_argument("--request_type", type=str, default='all')
    args = parser.parse_args()
    NEO4J_URI = args.db_uri
    NEO4J_USER = args.db_user
    NEO4J_PASSWORD = args.db_password
    token = args.vk_token
    request_type = args.request_type
    collect_data = args.collect_data
    username = args.user_id
    user_id = await get_numeric_user_id(username, token)
    if user_id is None: # проверка на корректность получения user_id
        logger.error("Failed to get user ID. Check token and username.")
        return

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        try:
            session.run("RETURN 'Connection successful' AS message") # тестовый запрос для проверки подключения
        except neo4j.exceptions.AuthError:
            logger.error("Neo4j connection failed, check data and retry")
            return
    with driver.session() as session:
        if collect_data:
            await collect_data_async(user_id, token, session, depth=2)
        if request_type == 'all':
            users_count = session.execute_read(get_users_count)
            logger.info(f"Total users in the db = {users_count}")
            top_followers = session.execute_read(get_top_followers)
            logger.info(f"Total 5 users by followers:")
            for follower in top_followers:
                logger.info(f"User ID: {follower['user_id']}, name: {follower['name']}")
            top_pages = session.execute_read(get_top_pages)
            logger.info("Top 5 pages by subscribers:")
            for page in top_pages:
                logger.info(
                    f"Page ID: {page['page_id']}, Name: {page['name']}")

        elif request_type == 'users_amount':
            users_count = session.execute_read(get_users_count)
            logger.info(f"Total users in the db = {users_count}")
        elif request_type == 'top_followers':
            top_followers = session.execute_read(get_top_followers)
            logger.info(f"Total 5 users by followers:")
            for follower in top_followers:
                logger.info(f"User ID: {follower['user_id']}, name: {follower['name']}")
        elif request_type == 'top_pages':
            top_pages = session.execute_read(get_top_pages)
            logger.info("Top 5 pages by subscribers:")
            for page in top_pages:
                logger.info(
                    f"Page ID: {page['page_id']}, Name: {page['name']}")
        else:
            logger.warning(f"Unknown request: {request_type}")

if __name__ == "__main__":
    asyncio.run(main())
