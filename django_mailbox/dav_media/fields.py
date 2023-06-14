from django_mailbox.utils import get_settings

from django.db.models import FileField
from django.db.models.fields.files import ImageField

from django_mailbox.dav_media.storage import DavMediaStorage


class AutoChoiceStorage:
    def __init__(self, storage=None, storage_location=None, **kwargs):
        """ Работа с файлами из media или webdav в зависимости от флага в конфиге """
        settings = get_settings()

        if storage:
            _storage = storage
        elif settings['dav_media_enabled']:
            _storage = DavMediaStorage(location=storage_location)
        else:
            _storage = None
        super().__init__(storage=_storage, **kwargs)


class FileDavMedia(AutoChoiceStorage, FileField):
    pass


class ImageDavMedia(AutoChoiceStorage, ImageField):
    pass
