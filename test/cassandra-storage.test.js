'use strict';

var cp     = require('child_process'),
	uuid   = require('node-uuid'),
	should = require('should'),
	moment = require('moment'),
	storage;

var HOST     = '52.207.97.35, 52.4.146.35, 52.23.116.133',
	USER     = 'iccassandra',
	PASSWORD = 'f312e8989361c1cdd4f04562d53742ea',
	PORT     = 9042,
	KEYSPACE = 'rkhcassandra',
	ID       = Date.now();

const REPLICATION_CONFIG = {
	AWS_VPC_US_EAST_1: 3
};

const SCHEMA = {
	fields: {
		id: {
			type: 'uuid',
			default: {'$db_function': 'uuid()'}
		},
		data_id: {
			type: 'bigint'
		},
		co2: {type: 'text'},
		temp: 'int',
		quality: 'float',
		reading_time: 'timestamp',
		metadata: {
			type: 'map',
			typeDef: '<varchar, text>'
		},
		list: {
			type: 'list',
			typeDef: '<varchar>'
		},
		set: {
			type: 'set',
			typeDef: '<varchar>'
		},
		random_data: 'varchar',
		is_normal: 'boolean',
		created: {
			type: 'timestamp',
			default: {
				'$db_function': 'toTimestamp(now())'
			}
		}
	},
	key: [['id'], 'created'],
	clustering_order: {'created': 'desc'},
	table_name: 'home_control'
};

const record = {
	data_id: ID,
	co2: '11%',
	temp: 23,
	quality: 11.25,
	reading_time: '2015-11-27T11:04:13.539Z',
	metadata: {
		metadata_json: 'reekoh metadata json'
	},
	list: ['value1', 'value1', 'value2'],
	set: ['value1', 'value2'],
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
			should.ok(storage = cp.fork(process.cwd()), 'Child process not spawned.');
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
						port: PORT,
						user: USER,
						password: PASSWORD,
						keyspace: KEYSPACE,
						replication_config: JSON.stringify(REPLICATION_CONFIG),
						schema: JSON.stringify(SCHEMA)
					}
				}
			}, function (error) {
				should.ifError(error);
			});
		});
	});

	describe('#data', function () {
		it('should process the data', function (done) {
			this.timeout(20000);

			storage.send({
				type: 'data',
				data: record
			}, (error) => {
				should.ifError(error);

				setTimeout(done, 19000);
			});
		});
	});

	describe('#data', function () {
		it('should have inserted the data', function (done) {
			this.timeout(20000);
			let Cassandra = require('express-cassandra');

			var cassandra = Cassandra.createClient({
				clientOptions: {
					contactPoints: `${HOST}`.replace(/\s/g, '').split(','),
					keyspace: KEYSPACE,
					protocolOptions: {
						port: PORT
					},
					authProvider: new Cassandra.driver.auth.PlainTextAuthProvider(USER, PASSWORD),
					queryOptions: {
						consistency: Cassandra.consistencies.one
					}
				},
				ormOptions: {
					defaultReplicationStrategy: Object.assign({
						class: 'NetworkTopologyStrategy'
					}, REPLICATION_CONFIG),
					migration: 'safe',
					createKeyspace: true
				}
			});

			cassandra.connect(function (connectionError) {
				should.ifError(connectionError);

				var Data = cassandra.loadSchema('Data', SCHEMA, function (modelError) {
					should.ifError(modelError);

					Data.findOne({data_id: cassandra.datatypes.Long.fromString(`${ID}`)}, {allow_filtering: true}, function (findError, data) {
						should.ifError(findError);
						should.exist(data);

						should.equal(record.co2, data.co2, 'Data validation failed. Field: co2');
						should.equal(record.temp, data.temp, 'Data validation failed. Field: temp');
						should.equal(record.quality, data.quality, 'Data validation failed. Field: quality');
						should.equal(record.random_data, data.random_data, 'Data validation failed. Field: random_data');
						should.equal(moment(record.reading_time).format('YYYY-MM-DDTHH:mm:ss.SSSSZ'),
							moment(data.reading_time).format('YYYY-MM-DDTHH:mm:ss.SSSSZ'), 'Data validation failed. Field: reading_time');
						should.equal(JSON.stringify(record.metadata), JSON.stringify(data.metadata), 'Data validation failed. Field: metadata');
						should.equal(record.is_normal, data.is_normal, 'Data validation failed. Field: is_normal');
						done();
					});
				});
			});
		});
	});
});