# -*- coding: utf-8 -*-

from datetime import datetime, date

__all__ = (
	'Encoder'
)


class Encoder(object):
	def __call__(self, o):
		if isinstance(o, (date, datetime)):
			return o.isoformat()
		return o
