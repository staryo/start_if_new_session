import subprocess
from argparse import ArgumentParser
from os import getcwd
from os.path import join

import yaml
from tqdm import tqdm

from ia_rest.iarest import IARest


def read_yml(filename):
    with open(filename, 'r', encoding="utf-8") as stream:
        result = yaml.load(stream, Loader=yaml.SafeLoader)
    return result


def write_to_yml(data, filename):
    with open(filename, 'w', encoding="utf-8") as stream:
        yaml.dump(data, stream)


def read_script_from_yml(filename):
    return read_yml(filename)['scripts']


def read_session_from_yml(filename):
    return read_yml(filename)['session']


def save_session_to_yml(session, filename):
    write_to_yml({'session': session}, filename)


def read_session_from_rest(config):
    with IARest.from_config(config) as ia:
        ia._perform_login()
        session = ia._get_main_session()
    return session


def start_script(session_config, config):
    old_session = read_session_from_yml(session_config)
    new_session = read_session_from_rest(read_yml(config)['IA'])
    if old_session == new_session:
        tqdm.write('Новой сессии не появилось')
        return
    if new_session is None:
        tqdm.write('Нет принятого расчета')
        return
    tqdm.write(f"Появилась новый главный расчет номер {new_session}, "
               f"раньше главный расчет был {old_session}")
    scripts = read_script_from_yml(config)
    for script in scripts:
        tqdm.write(f"Запускаем скрипт {script}")
        subprocess.call(script, shell=True)
    tqdm.write(f"Все скрипты отработали")
    save_session_to_yml(new_session, session_config)


if __name__ == '__main__':
    parser = ArgumentParser(
        description='Запускаем скрипт, если появился новый главный расчет'
    )
    parser.add_argument('-c', '--config', required=False,
                        default=join(getcwd(), 'config.yml'))

    parser.add_argument('-s', '--session', required=False,
                        default=join(getcwd(), 'session.yml'))

    args = parser.parse_args()

    start_script(args.session, args.config)

