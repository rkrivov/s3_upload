#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
from uuid import UUID

from s3.s3functions import convert_uuid_to_string


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return convert_uuid_to_string(obj, use_curly_brackets=True)
        return json.JSONEncoder.default(self, obj)
