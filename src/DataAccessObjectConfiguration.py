# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObjectConfiguration as DataAccessObjectConfigurationBase
from ssl import create_default_context

__all__ = (
	'DataAccessObjectConfiguration'
)


class DataAccessObjectConfiguration(DataAccessObjectConfigurationBase):
	"""
	Data Access Object Configuration Class for ElasticSearch
	"""

	def __init__(self, host, port, username=None, password=None, ssl=False):
		self.scheme = 'https' if ssl else 'http',
		self.host = host
		self.port = port
		self.auth = (username, password) if username and password else None
		self.ssl = ssl
		self.certs = None
		self.context = create_default_context() if ssl else None
		self.connection = None
		self.timeout = 60
		self.retries = 10
		self.retry = True
		return
