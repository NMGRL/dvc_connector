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


class DVC:
    def handle(self, request):
        # parse request
        name, url = self._parse_request(request)

        # update repo
        repo = self._update_repo(name, url)

        # extract payload
        payload = self._format_payload(repo)

        # upload
        self._upload(*payload)

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
        columns = [('Method', '%s'),
                   ('Description', '%s'),
                   ('Age', '%d'),
                   ('Error', '%d'),
                   ('Sigma', '%d'),
                   ('MSWD', '%d'),
                   ('SampleNo_Orig', '%s'),
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
                    if not f[0] == '.':

                        ias.append(self._extract_ia(os.path.join(r, f)))

        return columns, ias

    def _extract_ia(self, path):
        sigma = 1
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

            return age, age_err, sigma, mswd, sample, material, formation, lat, lon

    def _upload(self, columns, values):
        conn = self._get_connection()
        if conn:
            cursor = conn.cursor()

            columns, formats = zip(*columns)

            sql = '''INSERT INTO dbo.Gechronology {} VALUES {}'''.format(columns, formats)
            esql = 'SELECT * FROM dbo.Geochronology WHERE= %s'
            for vs in values:
                cursor.execute(esql)
                if not cursor.fetchone():
                    cursor.execute(sql, values)
                else:
                    info('Already exists {}'.format(vs))

            conn.close()

    def _get_connection(self, *args, **kw):
        h, u, p, n = [os.environ.get('DVC_CONNECTOR_DB_{}'.format(key)) for key in ('HOST', 'USER', 'PWD', 'NAME')]
        try:
            conn = pymssql.connect(h, u, p, n, timeout=15, login_timeout=5, *args, **kw)
            return conn
        except (pymssql.InterfaceError, pymssql.OperationalError) as e:
            warning('Could not connect to database. Error={}, host={}, user={}, db={}'.format(e, h, u, n))

# ============= EOF =============================================
