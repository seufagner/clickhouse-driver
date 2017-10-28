ITEMS = 100000
TYPE = 'Float32'
URL = 'http://localhost:8123'

drop_query = 'drop table if exists test'
create_query = 'create table test (a {type}, b {type}, c {type}, d {type}, e {type}, f {type}, g {type}, h {type}, i {type}, j {type}) engine = Memory'.format(type=TYPE)

data = [(x + 0.1, x + 0.11, x + 0.111, x + 0.1111, x + 0.11111, x + 0.11111, x + 0.111111, x + 0.1111111, x + 0.11111111, x + 0.111111111) for x in range(ITEMS)]

import requests
from datetime import date

requests.post(URL, params={'query': drop_query})
requests.post(URL, params={'query': create_query})

%timeit requests.post(URL, params={'query': 'INSERT INTO test (a, b, c, d, e, f, g, h, i, j) FORMAT TabSeparated'}, data='\n'.join(['\t'.join([str(x) for x in row]) for row in data]))
%timeit [float(x) for line in requests.get(URL, params={'query': 'SELECT * FROM test LIMIT {} FORMAT TabSeparated'.format(ITEMS)}).text.split('\n') for x in line.split('\t') if line]



from clickhouse_driver import Client

c = Client('localhost')
c.execute(drop_query)
c.execute(create_query)

%timeit c.execute('INSERT INTO test (a, b, c, d, e, f, g, h, i, j) VALUES', data)
%timeit c.execute('SELECT * FROM test LIMIT {}'.format(ITEMS))


