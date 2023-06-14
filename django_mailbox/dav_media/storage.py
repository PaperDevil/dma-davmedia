from django_mailbox.utils import get_settings

import os
import logging
from io import BytesIO
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

from django.core.files import File
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible


logger = logging.getLogger(__name__)


class DavFunctionsMixin:
    STORAGE_URL = None  # need to be overridden
    TIMEOUT_HEAD = 3
    TIMEOUT_GET = 12
    TIMEOUT_PUT = 30
    TIMEOUT_DELETE = 5
    read_session = None
    write_session = None

    def __init__(self, location=None):
        self.location = urljoin(self.STORAGE_URL, location)

    def read_in_chunks(self, f, chunk_size=1024):
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data

    def url(self, name):
        rp = self.remote_path(name)
        filename, ext = os.path.splitext(rp)
        if ext == '.gz':
            return filename
        return rp

    def get_name_from_url(self, url):
        return url.replace(self.location, '')

    def path(self, name):
        return self.remote_path(name)

    def remote_path(self, name):
        return urljoin(self.location, name)

    def ensure_session(self, for_write=False):
        """ По умолчанию выдаём сессию для операций чтения """

        if for_write and self.write_session:
            return self.write_session
        elif not for_write and self.read_session:
            return self.read_session

        self.rotate_session()
        if for_write:
            return self.write_session
        return self.read_session

    def rotate_session(self, max_retries=None):
        """ Должно быть вызвано внешним кодом - селери-таском или вьюхой """

        DavFunctionsMixin.read_session = requests.Session()
        DavFunctionsMixin.read_session.trust_env = False
        DavFunctionsMixin.write_session = requests.Session()
        DavFunctionsMixin.write_session.trust_env = False
        if max_retries is not None:
            DavFunctionsMixin.write_session.mount(self.STORAGE_URL, HTTPAdapter(max_retries=max_retries))


@deconstructible
class DavMediaStorage(DavFunctionsMixin, Storage):
    def __init__(self, *args, **kwargs):
        settings = get_settings()
        self.STORAGE_URL = settings['dav_media_url']
        super(DavMediaStorage, self).__init__( *args, **kwargs)

    def _save(self, name, content):
        path = self.remote_path(name)
        session = self.ensure_session(for_write=True)
        if hasattr(content, 'temporary_file_path'):
            with open(content.temporary_file_path(), 'rb') as src_file:
                response = session.put(
                    path, data=self.read_in_chunks(src_file),
                    timeout=self.TIMEOUT_PUT)
        else:
            content.seek(0)
            response = session.put(path, content.read(), timeout=self.TIMEOUT_PUT)
        response.raise_for_status()
        return name

    def _open(self, name, mode='rb'):
        path = self.remote_path(name)
        session = self.ensure_session()
        file_response = session.get(path, timeout=self.TIMEOUT_GET)
        _file = File(BytesIO(file_response.content), name=name)
        return _file

    def exists(self, name):
        path = self.remote_path(name)
        session = self.ensure_session()
        response = session.head(path, timeout=self.TIMEOUT_HEAD)
        return response.status_code == 200

    def size(self, name):
        path = self.remote_path(name)
        session = self.ensure_session()
        response = session.head(path, timeout=self.TIMEOUT_HEAD)
        return int(response.headers.get('Content-Length', 1))

    def delete(self, name):
        path = self.remote_path(name)
        session = self.ensure_session(for_write=True)
        response = session.delete(path, timeout=self.TIMEOUT_DELETE)
        if response.status_code == 404:
            # уже удалено
            pass

        elif response.status_code == 405:
            error_msg = 'Забыт DELETE в dav_methods'
            logger.error(error_msg)
            return
        else:
            response.raise_for_status()
