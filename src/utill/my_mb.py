
import csv
import json
import os
import requests

from loguru import logger

from .my_const import HttpMethod
from .my_csv import write as csv_write
from .my_dict import AutoPopulatingDict
from .my_env import MB_FILENAME


def _decode_collection_location_to_group(collections_dict: dict, location: str):
    return ' > '.join(map(lambda x: collections_dict[x], map(int, location.strip('/').split('/'))))


def _translate_user_group_ids(user: dict) -> set:
    return set(user['group_ids']) - {1}  # Exclude 'All Users' group


class MB:
    def __init__(self, base_url: str = None) -> None:
        config = json.loads(open(MB_FILENAME, 'r').read())

        self.base_url = base_url or config['base_url']
        self.api_key = config['api_key']

        self._is_user_initialized = False
        self._is_group_initialized = False
        self._is_collection_initialized = False

        self.dict__question_id__question = AutoPopulatingDict(self._fetch_question_by_id)
        self.dict__question_url__question = AutoPopulatingDict(self._fetch_question_by_url)
        self.dict__dashboard_id__dashboard = AutoPopulatingDict(self._fetch_dashboard_by_id)
        self.dict__collection_id__collection = AutoPopulatingDict(self._fetch_collection_by_id)

        logger.info(f'✅ Initialized {self.base_url}')

    # <<----- START: Util

    def send_request(self, method: HttpMethod, endpoint: str, json_data: dict = None) -> requests.Response:
        url = f'{self.base_url}/{endpoint}'
        logger.debug(f'🚗 [{method}] {endpoint}')

        headers = {
            'x-api-key': self.api_key
        }

        if method == HttpMethod.GET:
            response = requests.get(url, headers=headers)
        elif method == HttpMethod.POST:
            response = requests.post(url, headers=headers, json=json_data)
        elif method == HttpMethod.PUT:
            response = requests.put(url, headers=headers, json=json_data)
        elif method == HttpMethod.DELETE:
            response = requests.delete(url, headers=headers, json=json_data)
        else:
            raise ValueError(f'HTTP method {method} not recognized!')

        if not (200 <= response.status_code < 300):
            raise Exception(f'HTTP error {response.status_code}: {response.text}')

        return response

    def reinit(self):
        self.__init__()

    # END: Util ----->>

    # <<----- START: User

    def _init_all_users(self):
        if not self._is_user_initialized:
            logger.debug('🕐 Initialize user data')
            response_json = self.send_request(HttpMethod.GET, 'api/user').json()['data']
            self._dict__user_id__user = {x['id']: x for x in response_json}
            self._dict__user_email__user = {x['email']: x for x in response_json}
            self._is_user_initialized = True

    @property
    def dict__user_id__user(self) -> dict:
        self._init_all_users()
        return self._dict__user_id__user

    @property
    def dict__user_email__user(self) -> dict:
        self._init_all_users()
        return self._dict__user_email__user

    def make_sure_all_email_exists(self, emails: list[str]):
        not_exists = []
        for email in emails:
            try:
                self.dict__user_email__user[email]
            except KeyError:
                not_exists.append(email)

        if not_exists:
            raise ValueError(f'Email not exists: {not_exists}')

    def create_user(self, first_name: str, last_name: str, email: str, group_ids: list):
        self.send_request(HttpMethod.POST, 'api/user', {
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'user_group_memberships': group_ids,
        }).json()
        self._is_user_initialized = False
        logger.info(f'✅ Create user {email}')

    def deactivate_user_by_email(self, email: str):
        user = self.dict__user_email__user[email]
        self.send_request(HttpMethod.DELETE, f'api/user/{user["id"]}')
        del self.dict__user_email__user[email]
        logger.info(f'✅ Deactivate user [{user["id"]}] {email}')

    def reset_password_by_email(self, email: str):
        try:
            self.dict__user_email__user[email]
        except KeyError as e:
            logger.error(f'User {email} not exists')
            raise e
        self.send_request(HttpMethod.POST, 'api/session/forgot_password', {'email': email})
        logger.info(f'✅ Reset password {email}')

    # END: User ----->>

    # <<----- START: Group

    def _init_all_groups(self):
        if not self._is_group_initialized:
            logger.debug('🕐 Initialize group data')
            response_json = self.send_request(HttpMethod.GET, 'api/permissions/group').json()
            self._dict__group_id__group = {x['id']: x for x in response_json}
            self._dict__group_name__group = {x['name']: x for x in response_json}
            self._is_group_initialized = True

    @property
    def dict__group_id__group(self) -> dict:
        self._init_all_groups()
        return self._dict__group_id__group

    @property
    def dict__group_name__group(self) -> dict:
        self._init_all_groups()
        return self._dict__group_name__group

    def create_group(self, group_name: str):
        self.send_request(HttpMethod.POST, 'api/permissions/group', {
            'name': group_name,
        })
        self._is_group_initialized = False
        logger.info(f'✅ Create group {group_name}')

    def delete_group(self, group_name: str):
        self.send_request(HttpMethod.DELETE, f'api/permissions/group/{self.dict__group_name__group[group_name]["id"]}')
        self._is_group_initialized = False
        logger.info(f'✅ Delete group {group_name}')

    # END: Group ----->>

    # <<----- START: Permission

    def grant_user_id_to_group_by_id(self, user_id: int, group_id: int) -> None:
        self.send_request(HttpMethod.POST, 'api/permissions/membership', {
            'group_id': group_id,
            'user_id': user_id,
        })

        # Update locally
        self.dict__user_id__user[user_id]['group_ids'].append(group_id)
        self.dict__user_email__user[self.dict__user_id__user[user_id]['email']]['group_ids'].append(group_id)

        logger.info(f'✅ Grant user \'{self.dict__user_id__user[user_id]["email"]}\' to group \'{self.dict__group_id__group[group_id]["name"]}\'')

    def mirror_user_permission_by_email(self, source_email: str, target_email: str) -> None:
        source_user = self.dict__user_email__user[source_email]
        target_user = self.dict__user_email__user[target_email]

        source_user_group_ids = _translate_user_group_ids(source_user)
        target_user_group_ids = _translate_user_group_ids(target_user)

        to_be_granted_group_ids = source_user_group_ids - target_user_group_ids
        existing_group_ids = source_user_group_ids - to_be_granted_group_ids
        if existing_group_ids:
            pass
        for group_id in to_be_granted_group_ids:
            self.grant_user_id_to_group_by_id(target_user['id'], group_id)

    def grant_group_id_to_collection_by_id(self, group_id: int, collection_id: int):
        # Get latest revision
        graph = self.send_request(HttpMethod.GET, 'api/collection/graph').json()
        logger.debug(f'Latest revision: {graph["revision"]}')

        group_id_str = str(group_id)
        collection_id_str = str(collection_id)

        # Test group existence
        try:
            self.dict__group_id__group[group_id]
        except KeyError as e:
            logger.error(f'Group ID {group_id} not exists')
            raise e

        # Test collection existence
        try:
            self.dict__collection_id__collection[collection_id]
        except KeyError as e:
            logger.error(f'Collection ID {collection_id} not exists')
            raise e

        if graph['groups'][group_id_str][collection_id_str] != 'none':
            logger.warning(f'Group {self.dict__group_id__group[group_id]["name"]} already has permission {graph["groups"][group_id_str][collection_id_str]} to collection {self.dict__collection_id__collection[collection_id]["name"]}')
            return
        graph['groups'][group_id_str][collection_id_str] = 'read'

        self.send_request(HttpMethod.PUT, 'api/collection/graph', {
            'revision': graph['revision'],
            'groups': {
                group_id_str: {
                    collection_id_str: 'read'
                }
            },
        })

        logger.info(f'✅ Grant group \'{self.dict__group_id__group[group_id]["name"]}\' to collection \'{self.dict__collection_id__collection[collection_id]["name"]}\'')

    def grant_user_email_to_dashboard_by_url(self, email: str, dashboard_url: str):
        # Get user
        user = self.dict__user_email__user[email]
        user_group_ids = _translate_user_group_ids(user)

        # Get dashboard
        dashboard_id = int(dashboard_url.split(f'{self.base_url}/dashboard/')[1].split('-')[0])
        dashboard = self.dict__dashboard_id__dashboard[dashboard_id]

        # Get collection
        collection_id = dashboard['collection_id']
        collection = self.dict__collection_id__collection[collection_id]

        # Get collection's group
        try:
            group = self.dict__group_name__group[collection['group_name']]
        except KeyError:
            # Create group if not exists
            self.create_group(collection['group_name'])
            group = self.dict__group_name__group[collection['group_name']]

            # Grant group to collection
            self.grant_group_id_to_collection_by_id(group['id'], collection_id)

        # Skip if user already in group
        if group['id'] in user_group_ids:
            logger.warning(f'{dashboard_url}: User {email} already in group {group["name"]}')
            return

        # Grant
        self.grant_user_id_to_group_by_id(user['id'], group['id'])

    def grant_user_email_to_collection_by_url(self, email: str, collection_url: str):
        # Get user
        user = self.dict__user_email__user[email]
        user_group_ids = _translate_user_group_ids(user)

        # Get collection
        collection_id = int(collection_url.split(f'{self.base_url}/collection/')[1].split('-')[0])
        collection = self.dict__collection_id__collection[collection_id]

        # Get collection's group
        try:
            group = self.dict__group_name__group[collection['group_name']]
        except KeyError:
            # Create group if not exists
            self.create_group(collection['group_name'])
            group = self.dict__group_name__group[collection['group_name']]

            # Grant group to collection
            self.grant_group_id_to_collection_by_id(group['id'], collection_id)

        # Skip if user already in group
        if group['id'] in user_group_ids:
            logger.warning(f'{collection_url}: User {email} already in group {group["name"]}')
            return

        # Grant
        self.grant_user_id_to_group_by_id(user['id'], group['id'])

    def grant_user_email_to_question_by_url(self, email: str, question_url: str):
        # Get user
        user = self.dict__user_email__user[email]
        user_group_ids = _translate_user_group_ids(user)

        # Get question
        question = self.dict__question_url__question[question_url]

        # Get question's collection
        collection_id = question['collection_id']
        collection = self.dict__collection_id__collection[question['collection_id']]

        # Get collection's group
        try:
            group = self.dict__group_name__group[collection['group_name']]
        except KeyError:
            # Create group if not exists
            self.create_group(collection['group_name'])
            group = self.dict__group_name__group[collection['group_name']]

            # Grant group to collection
            self.grant_group_id_to_collection_by_id(group['id'], collection_id)

        # Skip if user already in group
        if group['id'] in user_group_ids:
            logger.warning(f'{question_url}: User {email} already in group {group["name"]}')
            return

        # Grant
        self.grant_user_id_to_group_by_id(user['id'], group['id'])

    # END: Permission ----->>

    # <<----- START: Collection

    def _fetch_collection_by_id(self, collection_id: int) -> dict:
        if not self._is_collection_initialized:
            logger.debug('🕐 Initialize collection data')
            response_json = [x for x in self.send_request(HttpMethod.GET, 'api/collection').json()[1:]]  # Exclude root collection
            self.dict__collection_id__collection_name = {x['id']: x['name'] for x in response_json}
            self.dict__collection_id__collection = {x['id']: {
                **x,
                'group_name': ' > '.join([_decode_collection_location_to_group(self.dict__collection_id__collection_name, x['location']), x['name']]) if x['location'] != '/' else x['name']
            } for x in response_json if x['personal_owner_id'] is None}
            self._is_collection_initialized = True

            if collection_id in self.dict__collection_id__collection:
                return self.dict__collection_id__collection[collection_id]
        else:
            return self.send_request(HttpMethod.GET, f'api/collection/{collection_id}').json()

    # END: Collection ----->>

    # <<----- START: Dashboard

    def _fetch_dashboard_by_id(self, dashboard_id: int) -> dict:
        return self.send_request(HttpMethod.GET, f'api/dashboard/{dashboard_id}').json()

    # END: Dashboard ----->>

    # <<----- START: Question

    def _fetch_question_by_id(self, question_id: int) -> dict:
        return self.send_request(HttpMethod.GET, f'api/card/{question_id}').json()

    def _fetch_question_by_url(self, question_url: str) -> dict:
        question_id = int(question_url.split(f'{self.base_url}/question/')[1].split('-')[0])
        return self._fetch_question_by_id(question_id)

    def download_question_as_csv(self, card_id: int, dst_filename: str = None):
        dst_filename = os.path.expanduser(dst_filename)
        response = self.send_request(HttpMethod.POST, f'api/card/{card_id}/query/csv')
        content_decoded = response.content.decode()
        csvreader = csv.reader(content_decoded.splitlines(), delimiter=',')
        data = list(csvreader)

        csv_write(dst_filename, data)

    def archive_question_by_url(self, question_url: str) -> None:
        question_id = int(question_url.split(f'{self.base_url}/question/')[1].split('-')[0])
        self.send_request(HttpMethod.PUT, f'api/card/{question_id}', {
            'archived': True
        })
        logger.info(f'✅ Archive question {question_url}')

    # END: Question ----->>
