# ===============================================================================
# Copyright 2019 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import json
import os
from datetime import datetime

from git import Repo
import pymssql

CONNECTOR_ROOT = os.path.join(os.path.expanduser('~'), '.dvc_connector')

if not os.path.dirname(CONNECTOR_ROOT):
    os.mkdir(CONNECTOR_ROOT)

REPO_ROOT = os.path.join(CONNECTOR_ROOT, 'repositories')
if not os.path.dirname(REPO_ROOT):
    os.mkdir(REPO_ROOT)


def warning(msg):
    print('WARNING:  {}'.format(msg))


def info(msg):
    print('INFO   :  {}'.format(msg))


class LogEntry:
    def __init__(self, name, url):
        self.name = name
        self.url = url
        self.timestamp = datetime.now()

    def tolist(self):
        return self.timestamp, self.name, self.url


class Log:
    def __init__(self):
        self._items = []

    def add(self, name, url):
        e = LogEntry(name, url)
        self._items.append(e)

        self._truncate()

    def _truncate(self):
        now = datetime.now()
        threshold = 60 * 60 * 24 * 2  # 2 days
        for i, e in enumerate(reversed(self._items)):
            dt = now - e.timestamp
            if dt.total_seconds() > threshold:
                self._items = self._items[i:]
                break

    def tolist(self):
        return [e.tolist() for e in self._items]


class DVC:
    def __init__(self):
        self._log = Log()

    def handle(self, request):
        # parse request
        name, url = self._parse_request(request)

        self._log.add(name, url)
        # update repo
        repo = self._update_repo(name, url)

        # extract payload
        payload = self._format_payload(repo)

        # upload
        self._upload(*payload)

    def log_list(self):
        return self._log.tolist()

    def _parse_request(self, request):
        name = request['name']
        url = request['clone_url']

        return name, url

    def _update_repo(self, name, url):
        info('updating repository')
        repo = self._get_repo(name, url)

        repo.git.fetch('--all')
        repo.git.reset('--hard', 'origin/master')

        return repo

    def _get_repo(self, name, url):
        root = os.path.join(REPO_ROOT, name)
        if not os.path.isdir(root):
            info('cloning repository from {} to {}'.format(url, root))
            repo = Repo.clone_from(url, root)
        else:
            info('Got existing repository. {}'.format(root))
            repo = Repo(root)

        return repo

    def _format_payload(self, repo):
        columns = [('SampleNo_Orig', '%s'),
                   ('Method', '%s'),
                   ('Description', '%s'),
                   ('Lab', '%d'),

                   ('Age', '%d'),
                   ('Error', '%d'),
                   ('Sigma', '%d'),
                   ('MSWD', '%d'),

                   ('Material', '%s'),
                   ('Formation', '%s'),
                   ('Latitude', '%d'),
                   ('Longitude', '%d'),
                   ]
        ias = []

        # get interpreted ages
        rr = os.path.join(repo.working_tree_dir, 'ia')
        for r, ds, fs in os.walk(rr, topdown=True):
            if os.path.basename(r) == 'ia':
                for f in fs:
                    if not f.startswith('.'):
                        ia = self._extract_ia(os.path.join(r, f))
                        if ia is not None:
                            ias.append(ia)

        return columns, ias

    def _extract_ia(self, path):
        sigma = 1
        method = 'AA'
        lab = 6  # NMGRL
        description = '40/39 Argon-Argon'

        with open(path, 'r') as rfile:
            d = json.load(rfile)
            age = d.get('age')
            age_err = d.get('age_err')
            mswd = d.get('mswd')
            sample = d.get('sample')
            material = d.get('material')
            lat = d.get('latitude')
            lon = d.get('longitude')
            formation = d.get('formation')

        return sample, method, description, lab, \
               age, age_err, sigma, mswd, \
               material, formation, lat, lon

    def _upload(self, columns, values):
        conn = self._get_connection()
        if conn:
            cursor = conn.cursor()

            columns, formats = zip(*columns)
            columns = ','.join(columns)
            formats = ','.join(formats)

            esql = 'SELECT * FROM dbo.nm_geochronology WHERE SampleNo_Orig=%s'
            sql = '''INSERT INTO dbo.nm_geochronology ({}) VALUES ({})'''.format(columns, formats)

            for vs in values:
                cursor.execute(esql, vs[0])
                if cursor.fetchone():
                    info('Already exists {}'.format(vs))
                else:
                    cursor.execute(sql, values)

            conn.close()

    def _get_connection(self, *args, **kw):
        h, u, p, n = [os.environ.get('DVC_CONNECTOR_DB_{}'.format(key)) for key in ('HOST', 'USER', 'PWD', 'NAME')]
        try:
            conn = pymssql.connect(h, u, p, n, timeout=15, login_timeout=5, *args, **kw)
            return conn
        except (pymssql.InterfaceError, pymssql.OperationalError) as e:
            warning('Could not connect to database. Error={}, host={}, user={}, db={}'.format(e, h, u, n))

# ============= EOF =============================================
