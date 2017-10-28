from datetime import datetime, timedelta

from .base import FormatColumn


class DateTimeColumn(FormatColumn):
    ch_type = 'DateTime'

    py_types = (datetime, )
    format = 'I'

    epoch_start = datetime(1970, 1, 1)

    def before_write_item(self, value):
        return int((value - self.epoch_start).total_seconds())

    def after_read_item(self, value):
        return self.epoch_start + timedelta(seconds=value)
