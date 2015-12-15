'use strict';

var cp     = require('child_process'),
	assert = require('assert'),
	should = require('should'),
	moment = require('moment'),
	storage;

var HOST     = 'ec2-54-86-228-115.compute-1.amazonaws.com',
	USER     = 'reekoh',
	PASSWORD = 'rozzwalla',
	PORT     = 9042,
	KEYSPACE = 'reekoh',
	TABLE    = 'reekoh_table',
	ID       = new Date().getTime();

var record = {
	id: ID,
	co2: '11%',
	temp: 23,
	quality: 11.25,
	reading_time: '2015-11-27T11:04:13.539Z',
	metadata: {metadata_json: 'reekoh metadata json'},
	random_data: 'abcdefg',
	is_normal: true
};

describe('Storage', function () {
	this.slow(5000);

	after('terminate child process', function () {
		storage.send({
			type: 'close'
		});

		setTimeout(function () {
			storage.kill('SIGKILL');
		}, 3000);
	});

	describe('#spawn', function () {
		it('should spawn a child process', function () {
			assert.ok(storage = cp.fork(process.cwd()), 'Child process not spawned.');
		});
	});

	describe('#handShake', function () {
		it('should notify the parent process when ready within 20 seconds', function (done) {
			this.timeout(20000);

			storage.on('message', function (message) {
				if (message.type === 'ready')
					done();
			});

			storage.send({
				type: 'ready',
				data: {
					options: {
						host: HOST,
						user: USER,
						password: PASSWORD,
						keyspace: KEYSPACE,
						table: TABLE,
						port: PORT,
						fields: JSON.stringify({
							id: {source_field: 'id', data_type: 'Integer'},
							co2_field: {source_field: 'co2', data_type: 'String'},
							temp_field: {source_field: 'temp', data_type: 'Integer'},
							quality_field: {source_field: 'quality', data_type: 'Float'},
							reading_time_field: {
								source_field: 'reading_time',
								data_type: 'DateTime',
								format: 'YYYY-MM-DDTHH:mm:ss.SSSSZ'
							},
							metadata_field: {source_field: 'metadata', data_type: 'JSON'},
							random_data_field: {source_field: 'random_data'},
							is_normal_field: {source_field: 'is_normal', data_type: 'Boolean'}
						})
					}
				}
			}, function (error) {
				assert.ifError(error);
			});
		});
	});

	describe('#data', function () {
		it('should process the data', function (done) {

			storage.send({
				type: 'data',
				data: record
			}, done);

		});
	});

	describe('#data', function () {
		it('should have inserted the data', function (done) {
			this.timeout(20000);

			var cassandra = require('cassandra-driver');

			var authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);

			var client = new cassandra.Client({
				contactPoints: [HOST],
				authProvider: authProvider,
				keyspace: KEYSPACE,
				protocolOptions: {port: PORT}
			});

			client.execute('select * from reekoh_table where id = ' + ID, function (error, response) {

				should.exist(response.rows[0]);

				var resp = response.rows[0];

				should.equal(record.co2, resp.co2_field, 'Data validation failed. Field: co2');
				should.equal(record.temp, resp.temp_field, 'Data validation failed. Field: temp');
				should.equal(record.quality, resp.quality_field, 'Data validation failed. Field: quality');
				should.equal(record.random_data, resp.random_data_field, 'Data validation failed. Field: random_data');
				should.equal(moment(record.reading_time).format('YYYY-MM-DDTHH:mm:ss.SSSSZ'),
					moment(resp.reading_time_field).format('YYYY-MM-DDTHH:mm:ss.SSSSZ'), 'Data validation failed. Field: reading_time');
				should.equal(JSON.stringify(record.metadata), JSON.stringify(resp.metadata_field), 'Data validation failed. Field: metadata');
				should.equal(record.is_normal, resp.is_normal_field, 'Data validation failed. Field: is_normal');
				done();
			});
		});
	});
});