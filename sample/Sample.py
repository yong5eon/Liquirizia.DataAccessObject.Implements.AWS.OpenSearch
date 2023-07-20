# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObjectHelper

from Liquirizia.DataAccessObject.Implements.AWS.OpenSearch import DataAccessObject, DataAccessObjectConfiguration

if __name__ == '__main__':

	# Set connection
	DataAccessObjectHelper.Set(
		'Sample',
		DataAccessObject,
		DataAccessObjectConfiguration(
			host='127.0.0.1',
			port=9200
		)
	)

	# Get connection
	con = DataAccessObjectHelper.Get('Sample')

	con.delete('sample')
	con.create(
		'sample',
		mappings={
			'properties': {
				'no': {
					'type': 'long',
				},
				'name': {
					'type': 'text',
					'analyzer': 'sample',
				},
				'address': {
					'type': 'text',
					'analyzer': 'sample',
				},
				'dateOfEntry': {
					'type': 'date',
				}
			}
		},
		tokenizer={
			'sample': {
				'type': 'ngram',
				'min_gram': 2,
				'max_gram': 3,
				'token_chars': [
					'letter',
					'digit'
				],
			}
		},
		analyzer={
			'sample': {
				'type': 'custom',
				'tokenizer': 'sample',
			}
		}
	)

	con.set(
		'sample',
		id='1',
		doc={
			'no': 3,
			'name': '허용선',
			'address': '서울특별시 중구',
			'dateOfEntry': '2021-04-01'
		}
	)
	con.set(
		'sample',
		id='2',
		doc={
			'no': 7,
			'name': '홍승걸',
			'address': '서울특별시 관악구',
			'dateOfEntry': '2021-06-01'
		}
	)
	con.set(
		'sample',
		id='3',
		doc={
			'no': 5,
			'name': '최준호',
			'address': '서울특별시 성동구',
			'created': '2021-05-20'
		}
	)

	print(con.query(
		'sample',
		query={
			'query_string': {
				'fields': ['name', 'address'],
				'query': '서울',
			}
		},
	))

	con.delete('sample')

