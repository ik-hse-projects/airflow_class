import requests
from pathlib import Path
import shutil
import bz2
import datetime
import xml.etree.ElementTree as ET
import re
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, task_group

# NOTE: configure this as appropriate for your airflow environment
DATA = Path('/opt/airflow/dags/files')
ARTICLES_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
INDEX_URL = "https://dumps.wikimedia.org/enwiki/latest"

with DAG(
    dag_id="wiki-links",
    catchup=False,
    start_date=datetime.datetime.now(),
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:
    @task()
    def get_urls():
        index = requests.get(INDEX_URL).text
        return [
            f'{INDEX_URL}/{m.group(1)}'
            for ln in index.split('\n')
            for m in re.finditer(r'href="(.*-pages-articles\d+\.xml.*\.bz2)"', ln)
        ]

    @task()
    def get_data(url: str = ARTICLES_URL):
        fname = url.rsplit('/', 1)[-1]
        DATA.mkdir(parents=True, exist_ok=True)
        dst_path = DATA / fname
        tmp_path = dst_path.with_suffix(dst_path.suffix + '.tmp')
        with requests.get(url, stream=True) as src:
            with open(tmp_path, 'wb') as dst:
                shutil.copyfileobj(src.raw, dst)
        tmp_path.rename(dst_path)
        return dst_path

    @task()
    def extract_data(src_path = DATA / 'articles.xml.bz2'):
        dst_path = src_path.with_suffix('')
        tmp_path = dst_path.with_suffix(dst_path.suffix + '.tmp')
        with bz2.BZ2File(src_path, 'r') as src:
            with open(tmp_xml, 'w') as dst:
                shutil.copyfileobj(src, dst)
        tmp_path.rename(dst_path)
        return dst_path

    @task()
    def process_data(src_path = DATA / 'articles.xml'):
        dst_path = src_path.with_suffix('.json')
        tmp_path = dst_path.with_suffix(dst_path.suffix + '.tmp')
        with open(src_path, 'r') as src:
            contributors = []
            title = None
            for action, elem in ET.iterparse(src, events=('start', 'end')):
                if action == 'end' and elem.tag == 'title':
                    title = elem.text
                if action == 'end' and elem.tag == 'username':
                    contributors.append((elem.text, title))
        with open(tmp_path, 'w') as f:
            json.dump(contributors, f)
        tmp_path.rename(dst_path)
        return dst_path

    @task.short_circuit()
    def check_new_version(url: str = ARTICLES_URL) -> str:
        return False

        fname = url.rsplit('/', 1)[-1]
        dst_path = DATA / fname
        if not dst_path.exists():
            return True
        resp = requests.head(url)
        size_match = dst_path.stat().st_size == int(resp.headers['content-length'])
        if size_match:
            return False
        return True

    @task.short_circuit()
    def collect_data(jsons):
        pass

    @task_group
    def process_group(url):
        x_check_version = check_new_version(url)
        x_get_data = get_data(url)
        x_extract_data = extract_data(x_get_data)
        x_process_data = process_data(x_extract_data)
        x_check_version >> x_get_data >> x_extract_data >> x_process_data

    results = process_group.expand(url=get_urls())
    collect = collect_data(results)
    results >> collect

