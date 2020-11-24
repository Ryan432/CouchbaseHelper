const CouchbaseHelper = require('./src/CouchbaseHelper');
const { sleep } = require('./helpers/utils');
// const couchbase = require('couchbase');

// // const cluster = new couchbase.Cluster();

// // couchbase.openBucket('test');

// const runLocksFunc = () => {
// 	const { cbHelper } = global;

// 	cbHelper.executeFuncWithLock({
// 		lockName: 'test',
// 		ttl: 10,
// 		func: async () => {
// 			console.log('Running number 1');
// 			await sleep(5000);
// 		}
// 	});

// 	// cbHelper.waitForLockAndExecute({
// 	// 	lockName: 'test',
// 	// 	ttl: 10,
// 	// 	func: async () => {
// 	// 		console.log('Running number 2');
// 	// 	}
// 	// });
// };

const HOST = 'couchbase://localhost';
const CONNECTION_CONFIG = {
	username: 'admin',
	password: 'password'
};

(async () => {
	try {
		global.cbHelper = await CouchbaseHelper.connectCluster(HOST, CONNECTION_CONFIG);
		console.log(global.cbHelper);
	} catch (error) {
		console.log(error);
	}
})();
// (async () => {
// 	try {
// 		const cbHelper = await CouchbaseHelper.connectCluster(HOST, CONNECTION_CONFIG);

// 		// runWithLock({
// 		// 	lockName: 'foo',
// 		// 	ttl: 10,
// 		// 	func: async () => {
// 		// 		try {
// 		// 			console.log('running 1');
// 		// 			await sleep('5s');
// 		// 		} catch (err) {
// 		// 			console.error(err);
// 		// 		}
// 		// 	}
// 		// });

// 		// waitForLock({
// 		// 	lockName: 'foo',
// 		// 	ttl: 10,
// 		// 	func: async () => {
// 		// 		try {
// 		// 			console.log('running 2');
// 		// 			await sleep('5s');
// 		// 		} catch (err) {
// 		// 			console.error(err);
// 		// 		}
// 		// 	}
// 		// });

// 		// const setLock = await cbHelper.setLock('test', 5);
// 		// const setLock1 = await cbHelper.setLock('test1', 5);
// 		// const setLock2 = await cbHelper.setLock('test2', 5);
// 		// // // const { cas } = setLockRes;
// 		// console.log({ setLock, setLock1, setLock2 });
// 		// await sleep(3000);
// 		// console.log();

// 		// cbHelper.disableLocking();

// 		// const extendLock = await cbHelper.extendLock('test', 2000);
// 		// console.log({ extendLock });
// 		// await sleep(2000);

// 		// const releaseLock = await cbHelper.releaseLock('test', extendLock.cas);
// 		// console.log({ releaseLock });

// 		// const dropBucketRes = await cbHelper.dropBucket('users');
// 		// console.log({ dropBucketRes });
// 		// await sleep(2000);

// 		// const createBucketObject = {
// 		// 	bucketName: 'users',
// 		// 	columns: [
// 		// 		{
// 		// 			name: 'id',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'type',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'firstName',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'lastName',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'age',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'email',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'password',
// 		// 			index: false
// 		// 		},
// 		// 		{
// 		// 			name: 'updateId',
// 		// 			index: true
// 		// 		}
// 		// 	]
// 		// };

// 		// const createBucketRes = await cbHelper.createBucket(createBucketObject);
// 		// console.log({ createBucketRes });
// 		// await sleep(2000);

// 		// // const upsertDocRes = await cbHelper.upsertDocument('test', { type: 'test', id: 123, foo: 'bar' });
// 		// // console.log({ upsertDocRes });
// 		// // await sleep(2000);

// 		// const insertDocRes = await cbHelper.insertDocument('users', {
// 		// 	type: 'user',
// 		// 	id: 123456,
// 		// 	firstName: 'ran',
// 		// 	lastName: 'amar',
// 		// 	age: 10,
// 		// 	email: 'ranamar121@gmail.com',
// 		// 	password: 'qwe123!@#',
// 		// 	updateId: 1
// 		// });
// 		// console.log({ insertDocRes });
// 		// await sleep(2000);

// 		// // const replaceDocRes = await cbHelper.replaceDocument('test', { type: 'test', id: 123, foo: 'rewqrq' });
// 		// // console.log({ replaceDocRes });
// 		// // await sleep(500);

// 		// const getDocRes = await cbHelper.getDocument('users', 'user_123');
// 		// console.log({ getDocRes });
// 		// await sleep(2000);

// 		// const removeDocRes = await cbHelper.removeDocument('users', 'user_123');
// 		// console.log({ removeDocRes });
// 	} catch (err) {
// 		console.log({ err });
// 	}
// })();

const getData = async (req, res) => {
	try {
		const { cbHelper } = global;
		const selectObject = {
			bucketName: 'users',
			selector: {
				where: [{ updateId: 3 }]
			}
		};
		const selectRes = await cbHelper.select(selectObject);
		// console.log(selectRes);
		// console.log(JSON.stringify({ selectRes }, 2, 5));
		res.json(selectRes);
	} catch (err) {
		console.error({ err });
		res.json({ err });
	}
};

// // setInterval(async () => {
// // 	try {
// // 		x();
// // 	} catch (err) {
// // 		console.log(err);
// // 	}
// // }, 500);

// // const cbHelper = CouchbaseHelper.connectCluster(HOST, CONNECTION_CONFIG);

// // const upsertDocument = async () => {
// // 	const doc = {
// // 		type: 'airline',
// // 		id: 8091,
// // 		callsign: 'CBS',
// // 		iata: null,
// // 		icao: null,
// // 		name: 'Couchbase Airways'
// // 	};

// // 	try {
// // 		const upsertDoc = cbHelper.upsertDocument({ bucket: 'test', doc });
// // 		// console.log({upsertDoc})
// // 	} catch (err) {}
// // };

// // upsertDocument();
