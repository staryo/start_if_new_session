import json
import ssl
import time
import uuid
from datetime import datetime, timedelta
from functools import partialmethod
from json import JSONDecodeError
from mimetypes import guess_type
from operator import ne, itemgetter
from websocket import create_connection
from time import sleep
from urllib.parse import urljoin

import pytz
import urllib3
from requests import Session
from tqdm import tqdm

__all__ = [
    'IARest',
]

from base.base import Base
from utils.list_to_dict import list_to_dict

_DATETIME_SIMPLE_FORMAT = '%Y-%m-%dT%H:%M:%S'


class IARest(Base):

    def __init__(self, login, password, base_url, ws_url,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._base_url = base_url
        self._login = login
        self._password = password
        self.ws_url = ws_url

        self._session = Session()
        self._session.verify = False

        self.cache = {}
        urllib3.disable_warnings()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.close()

    def _make_url(self, uri):
        return urljoin(self._base_url, uri)

    @staticmethod
    def _make_entity_name(filename, timestamp=datetime.now()):
        return '({}) {}'.format(
            timestamp.strftime(_DATETIME_SIMPLE_FORMAT),
            filename
        )

    def _check_status(self, uri):
        return self._session.get(
            self._make_url(uri)
        ).status_code

    def _wait_for_status(self, uri, status, operator=ne):
        while operator(self._check_status(uri), status):
            sleep(5)

    def get_from_rest_collection(self, table, filter=None):
        if table in self.cache:
            return self.cache[table]
        result = []
        self._perform_login()
        counter = 0
        step = 100000
        if table == 'specification_item':
            order_by = '&order_by=parent_id&order_by=child_id'
        elif table == 'operation_profession':
            order_by = '&order_by=operation_id&order_by=profession_id'
        elif table == 'order_entry':
            order_by = '&order_by=order_id&order_by=entity_id'
        else:
            order_by = '&order_by=id'
        pbar = tqdm(desc=f'Получение данных из таблицы {table}')
        while True:
            request_str = f'rest/collection/{table}' \
                      f'?start={counter}' \
                      f'&stop={counter + step}' \
                      f'{order_by}'
            if filter is not None:
                request_str += f'&filter={{{filter}}}'
            temp = self._perform_get(request_str)
            pbar.total = temp['meta']['count']
            counter += step
            pbar.update(min(
                step,
                temp['meta']['count'] - (counter - step)
            ))
            if table not in temp:
                break
            result += temp[table]
            if counter >= temp['meta']['count']:
                break
        if filter is None:
            self.cache[table] = result
        return result

    def _get_main_session(self):
        return self._perform_get('action/primary_simulation_session')['data']

    def _perform_json_request(self, http_method, uri, **kwargs):
        url = self._make_url(uri)
        logger = self._logger

        logger.debug('Выполнение {} запроса '
                    'по ссылке {!r}.'.format(http_method, url))

        logger.debug('Отправляемые данные: {!r}.'.format(kwargs))

        response = self._session.request(http_method,
                                         url=url,
                                         **kwargs)
        try:
            response_json = response.json()
        except JSONDecodeError:
            logger.error('Получен ответ на {} запрос по ссылке {!r}: '
                         '{!r}'.format(http_method, url, response))
            raise JSONDecodeError

        logger.debug('Получен ответ на {} запрос по ссылке {!r}: '
                     '{!r}'.format(http_method, url, response_json))
        return response_json

    _perform_get = partialmethod(_perform_json_request, 'GET')
    _perform_delete = partialmethod(_perform_json_request, 'DELETE')

    def _perform_post(self, uri, data):
        return self._perform_json_request('POST', uri, json=data)

    def _perform_put(self, uri, data):
        return self._perform_json_request('PUT', uri, json=data)

    def _perform_action(self, uri_part, **data):
        return self._perform_post(
            '/action/{}'.format(uri_part),
            data=data
        )

    def _perform_login(self):
        return self._perform_action(
            'login',
            data={
                'login': self._login,
                'password': self._password
            },
            action='login'
        )['data']

    def _perform_import_action(self, import_type, **kwargs):
        return self._perform_action(
            'import{}'.format(import_type),
            **kwargs
        ).get('data')

    def _perform_upload(self, filepath):
        url = self._make_url('/action/upload')

        self._logger.info(
            'Загружается файл {!r} по ссылке {!r}.'.format(
                filepath, url
            )
        )

        with open(filepath, 'rb') as f:
            return self._session.post(
                url=url,
                files={
                    'data': (
                        filepath,
                        f,
                        guess_type(filepath)
                    )
                }
            ).json()['data']

    def perform_plan_import(self, filepath):
        logger = self._logger

        logger.info('Импорт плана запущен.')

        return self._perform_import_action(
            '/plan',
            data={
                'plan': {
                    'type': 1,
                    'name': self._make_entity_name(filepath)
                },
                'filepath': self._perform_upload(filepath),
                'aggregate_order_entries': True,
                'time_zone': '+00:00'
            }
        )

    def _perform_state_import(self, state_type, import_config):
        logger = self._logger

        logger.info('Импорт даннных в таблицу {} запущен.'.format(state_type))

        state_import_session_id = self._perform_import_action(
            '/{}'.format(state_type),
            filepath=self._perform_upload(import_config)
        )['import_session_id']

        self._wait_for_status('/action/import/{}'.format(state_type), 400)

        self._perform_get(
            '/rest/collection/import_mismatch',
            params={
                'filter': 'import_session_id eq {}'.format(
                    state_import_session_id
                )
            }
        )

        logger.info('Импорт данных в таблицу {} завершен.'.format(state_type))

    perform_wip_import = partialmethod(
        _perform_state_import,
        'state'
    )

    perform_setup_state_import = partialmethod(
        _perform_state_import,
        'equipment_adjustment'
    )

    def start_simulation(self, plan_id, equipment_variation_id,
                         employee_variation_id, entity_batch_variation_id,
                         simulation_settings_id, equipment_amount_variation_id,
                         sim_period=None,
                         start_time=3):
        ws = create_connection(
            f'{self.ws_url}/message',
            sslopt={'cert_reqs': ssl.CERT_NONE}
        )
        user_id = self._perform_login()['id']
        allocation_check = self._perform_action(
            'state_allocation/check',
            data={
                'plan_id': plan_id,
                'allocation_types': [{'type': 1}, {'type': 0}]
            }
        )['data']
        types = []
        for row in allocation_check:
            if row['data']['allocated']:
                types.append(row['type'])
        wip_allocation_uuid = str(uuid.uuid4())
        allocation_data = self._perform_action(
            'state_allocation/allocate',
            data={
                'state_allocation_session_uuid': wip_allocation_uuid,
                'plan_id': plan_id,
                'allocation_types': [
                    {'type': each_type} for each_type in types
                ],
            }
        )
        # while True:
        #     message = json.loads(ws.recv())
        #     self._logger.info(str(message))
        #     if message['msg'] == 'STATE_ALLOCATION_COMPLETED':
        #         break
        #     if message['msg'] == 'STATE_ALLOCATION_FAILED':
        #         raise
        time.sleep(10)

        if sim_period is None:
            simulation_session_id = self._perform_post(
                'rest/simulation_session',
                data={
                    'simulation_session': {
                        'employee_variation_id': employee_variation_id,
                        'entity_batch_variation_id': entity_batch_variation_id,
                        'entity_route_variation_id': None,
                        'equipment_amount_variation_id': equipment_amount_variation_id,
                        'equipment_variation_id': equipment_variation_id,
                        'include_state': True,
                        'operation_task_variation_id': None,
                        'operation_variation_id': None,
                        'plan_id': plan_id,
                        'post_types': [24, 20, 25, 22],
                        'simulation_settings_id': simulation_settings_id,
                        'start_date': datetime.now(pytz.UTC).replace(
                            hour=start_time,
                            minute=0,
                            second=0
                        ).strftime(
                            _DATETIME_SIMPLE_FORMAT
                        ),
                        'type': 0,
                        'user_id': user_id
                    }
                }
            )['simulation_session']['id']
        else:
            simulation_session_id = self._perform_post(
                'rest/simulation_session',
                data={
                    'simulation_session': {
                        'employee_variation_id': employee_variation_id,
                        'entity_batch_variation_id': entity_batch_variation_id,
                        'entity_route_variation_id': None,
                        'equipment_amount_variation_id': None,
                        'equipment_variation_id': equipment_variation_id,
                        'include_state': True,
                        'operation_task_variation_id': None,
                        'operation_variation_id': None,
                        'plan_id': plan_id,
                        'post_types': [],
                        'simulation_settings_id': simulation_settings_id,
                        'start_date': datetime.now(pytz.UTC).replace(
                            hour=start_time,
                            minute=0,
                            second=0
                        ).strftime(
                            _DATETIME_SIMPLE_FORMAT
                        ),
                        'stop_date': (
                                datetime.now(
                                    pytz.UTC
                                ) + timedelta(
                                    days=sim_period
                                )).strftime(
                            _DATETIME_SIMPLE_FORMAT
                        ),
                        'type': 2,
                        'user_id': user_id
                    }
                }
            )['simulation_session']['id']

        self._logger.info(
            self._perform_post(
                f'action/simulation/{simulation_session_id}',
                data={'action': 'start'}
            )
        )
        return simulation_session_id

    def clean_sessions(self, number: int) -> None:
        sessions = self._perform_get('rest/simulation_session')['simulation_session']
        sessions = sorted(sessions, key=itemgetter('id'), reverse=True)
        for row in sessions[number:]:
            self.delete_simulation(row['id'])

    def delete_simulation(self, simulation_session_id: int) -> None:
        self._perform_delete(f'rest/simulation_session/{simulation_session_id}')

    def accept_simulation(
            self,
            simulation_session_id: int,
            accept=True
    ) -> None:
        ws = create_connection(
            f'{self.ws_url}/message',
            sslopt={'cert_reqs': ssl.CERT_NONE}
        )
        self._logger.info('Ожидаем завершения расчета')
        while True:
            message = json.loads(ws.recv())
            self._logger.info(str(message))
            try:
                if message['data']['simulation_session_id'] != str(simulation_session_id):
                    continue
            except TypeError:
                continue
            if message['msg'] == 'SIMULATION_SESSION_SUCCESSFULLY_FINISHED':
                self._logger.info('Расчет завершен')
                break
            if message['msg'] == 'SIMULATION_SESSION_FAILED':
                self._logger.info('Расчет завершен с ошибкой')
                break

        # while self.get_from_rest_collection(
        #         'simulation_session',
        #         filter=f'id eq {simulation_session_id}'
        # )[0]['status'] is None:
        #     self._logger.info('Ожидаем завершения расчета')
        #     sleep(60)
        result = self.get_from_rest_collection(
                'simulation_session',
                filter=f'id eq {simulation_session_id}'
        )[0]['status']
        if result == 0 and accept:
            self._perform_action(
                'primary_simulation_session',
                data={
                    'simulation_session_id': simulation_session_id,
                    'cleanup': False
                }
            )
            self._logger.info('Расчет принят')
        else:
            self._logger.info(f'Расчет завершился со статусом {result}')
        return result

    def get_trafficlight_data(self, session_id, department_id, direction):
        self._perform_login()
        if session_id is None:
            sessions = self.get_from_rest_collection(
                'static_session'
            )
            session_id = 0
            for row in sessions:
                if row['type'] == 2:
                    session_id = max(session_id, row['id'])
            tqdm.write(f"Сессия расчета {session_id}")
        self._perform_get(
            f"/data/static_result/{session_id}/consolidated/order_supply?offload=true"
        )
        sleep(2)
        departments = list_to_dict(self.get_from_rest_collection(
            'department'
        ))
        dept_id = None
        for each_id in departments:
            if department_id is None:
                break
            if departments[each_id]['identity'] == department_id:
                dept_id = each_id
                break
        while True:
            try:
                request = f"/data/static_result/{session_id}/consolidated/"\
                          f"product_supply/preprocessed?"\
                          f"with=order&with=entity&with=department&offload=true&"\
                          f"start=0&stop=1&"

                if dept_id is not None:
                    request += f"filter={{ {direction}_department_id eq {dept_id} }}"

                result = self._perform_get(
                    request
                )

                # print(result)
                if 'errors' in result:
                    self._logger.info(str(result))
                    if result['errors'][0]['name'] == \
                            'STATIC_REPORT_HAS_NOT_BEEN_PROCEED_YET':
                        for _ in tqdm(range(120), desc="waiting..."):
                            sleep(1)
                        continue
                    if result['errors'][0]['name'] == \
                            'STATIC_MODULE_DOES_NOT_HAVE_DATA':
                        for _ in tqdm(range(120), desc="waiting..."):
                            sleep(1)
                        continue
                if type(result['data']) == str:
                    for _ in tqdm(range(120), desc="waiting..."):
                        sleep(1)
                    continue
                total_rows_number = result['meta']['count']
                cur_row = 1
                step = 100000
                iter_rows = tqdm(desc='Запрашиваем отчет', total=total_rows_number)
                while cur_row < total_rows_number:
                    request = f"/data/static_result/{session_id}/consolidated/"\
                              f"product_supply/preprocessed?"\
                              f"with=order&with=entity&with=department&offload=true&"\
                              f"start={cur_row}&stop={cur_row + step}&"
                    if dept_id is not None:
                        request += f"filter={{ {direction}_department_id eq {dept_id} }}"

                    temp_result = self._perform_get(
                        request
                    )

                    for table in result:
                        result[table] = [*result[table], *temp_result[table]]

                    iter_rows.update(step)
                    cur_row += step

                break
            except TypeError:
                for _ in tqdm(range(120), desc="waiting..."):
                    sleep(1)
                continue
        return result

    @classmethod
    def from_config(cls, config):
        return cls(
            config['login'],
            config['password'],
            config['url'],
            config['ws_url'],
        )
