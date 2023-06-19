PROJECT_DIRPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run \
    --rm \
    --workdir='/usr/src/myapp' \
    -v "${PROJECT_DIRPATH}:/usr/src/myapp" \
    python:3.8 bash -c "apt update ; apt install --yes unixodbc-dev;
                               pip install -r requirements.txt;
                               pip3 install pyinstaller;
                               pyinstaller main.py \
                               --clean \
                               --name start_if_new_session \
                               --distpath=dist/linux/ \
                               --onefile -y ;
                               chown -R ${UID} dist; "