# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObject as DataAccessObjectBase
from Liquirizia.DataAccessObject.Properties.Index import Index

from Liquirizia.DataAccessObject.DataAccessObjectError import DataAccessObjectError
from Liquirizia.DataAccessObject.Errors import *

from .DataAccessObjectConfiguration import DataAccessObjectConfiguration
from .DataAccessObjectFormatEncoder import DataAccessObjectFormatEncoder
from .DataAccessObjectFormatDecoder import DataAccessObjectFormatDecoder

from Liquirizia.Util.Dictionary import Replace

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
	ConnectionError as ElasticSearchConnectionError,
	ConnectionTimeout as ElasticSearchConnectionTimeout,
	NotFoundError as ElasticSearchNotFound
)

from bson.objectid import ObjectId
from hashlib import shake_256

__all__ = (
	'DataAccessObject'
)


class DataAccessObject(DataAccessObjectBase, Index):
	"""
	Data Access Object Class for OpenSearch of AWS
	"""

	def __init__(self, conf: DataAccessObjectConfiguration, decoder=DataAccessObjectFormatDecoder(), encoder=DataAccessObjectFormatEncoder()):
		self.conf = conf
		self.decoder = decoder
		self.encoder = encoder

		if not isinstance(conf, DataAccessObjectConfiguration):
			raise RuntimeError('{} is not DataAccessConfiguration for OpenSearch')

		self.client = None
		return

	def __del__(self):
		if self.client:
			self.client.close()
		return

	def __id__(self, id):
		hash = shake_256()
		hash.update(str(id).encode('utf-8'))
		return ObjectId(hash.digest(12))

	def connect(self):
		try:
			self.client = Elasticsearch(
				scheme=self.conf.scheme,
				host=self.conf.host,
				port=self.conf.port,
				http_auth=self.conf.auth,
				use_ssl=self.conf.ssl,
				# verify_certs=self.conf.certs,
				verify_certs=False,
				ssl_context=self.conf.context,
				ssl_show_warn=False,
				connection_class=self.conf.connection,
				timeout=self.conf.timeout,
				max_retries=self.conf.retries,
				retry_on_timeout=self.conf.retry,
			)
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)
		return

	def close(self):
		self.client.close()
		self.client = None
		return

	def create(
		self,
		index,
		mappings,
		shards=1,
		totalFields=None,
		nestedFields=None,
		maxDepth=None,
		replicas=0,
		analyzer=None,
		tokenizer=None,
		normalizer=None,
		filter=None,
		cfilter=None,
		excepts=(400)
	):
		try:
			body = {
				'settings': {
					'index': {
						'number_of_shards': shards,
						'number_of_replicas': replicas,
						'mapping': {},
					},
					'analysis': {}
				},
				'mappings': mappings
			}

			if totalFields:
				body['settings']['index']['mapping']['total_fields'] = {'limit': totalFields}
			if nestedFields:
				body['settings']['index']['mapping']['nested_fields'] = {'limit': nestedFields}
			if maxDepth:
				body['settings']['index']['mapping']['depth'] = {'limit': maxDepth}
			if analyzer:
				body['settings']['analysis']['analyzer'] = analyzer
			if tokenizer:
				body['settings']['analysis']['tokenizer'] = tokenizer
			if normalizer:
				body['settings']['analysis']['normalizer'] = normalizer
			if filter:
				body['settings']['analysis']['filter'] = filter
			if cfilter:
				body['settings']['analysis']['char_filter'] = cfilter

			res = self.client.indices.create(index=index, body=body, ignore=excepts)
			if 'status' in res and 'error' in res:
				raise DataAccessObjectError(res['error']['reason'])
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)
		return

	def delete(self, index, excepts=(400, 404)):
		try:
			self.client.indices.delete(index, ignore=excepts)
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)
		return

	def total(self, index):
		try:
			return self.client.count(index)
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)

	def set(self, index, doc, type=None, id=None):
		try:
			self.client.index(
				index=index,
				id=self.__id__(id),
				doc_type=type,
				body=Replace(doc, fn=self.encoder)
			)
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)
		return

	def get(self, index, id):
		try:
			doc = self.client.get(
				index=index,
				id=self.__id__(id)
			)
			return Replace(doc['_source'], fn=self.decoder)
		except ElasticSearchNotFound as e:
			return None
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)

	def exists(self, index, id, type=None):
		try:
			return self.client.exists(
				index=index,
				id=self.__id__(id),
				doc_type=type
			)
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)

	def query(self, index, query, page=None, pos=None, size=None, aggs=None, sort=None, type=None):
		try:
			body = {'query': query}

			if size:
				body['size'] = size
			if page:
				body['from'] = (page - 1) * size
			if pos:
				body['from'] = pos
			if aggs:
				body['aggs'] = aggs
			if sort:
				body['sort'] = sort

			res = self.client.search(index=index, doc_type=type, body=body)
			return [Replace(hit['_source'], fn=self.decoder) for hit in res['hits']['hits']], res['hits']['total']['value'], res['aggregations'] if 'aggregations' in res else None
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)

	def count(self, index, query, aggs=None, type=None):
		try:
			body = {'query': query}

			if aggs:
				body['aggs'] = aggs

			body['size'] = 0

			res = self.client.search(index=index, doc_type=type, body=body)
			return res['hits']['total']['value']
		except ElasticSearchConnectionTimeout as e:
			raise DataAccessObjectConnectionTimeoutError(error=e)
		except ElasticSearchConnectionError as e:
			raise DataAccessObjectConnectionError(error=e)
		except Exception as e:
			raise DataAccessObjectError(str(e), error=e)
