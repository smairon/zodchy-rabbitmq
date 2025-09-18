import uuid
import datetime
from json import JSONEncoder


class DefaultJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # Convert UUID to string
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        if isinstance(obj, datetime.time):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return obj.total_seconds()
        if isinstance(obj, datetime.timezone):
            return obj.tzname()
        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, obj)
