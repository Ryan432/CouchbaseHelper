const couchbase = require('couchbase');
const _ = require('lodash');
const Aigle = require('aigle');
const { selectQueryBuilder, updateQueryBuilder, queryBuilder } = require('../helpers/utils');
const { GeneralError, ConnectionError } = require('./Errors');

class CouchbaseHelper {
	#cluster;
	#bucketManager;
	#queryIndexManager;
	#searchIndexManager;
	#locksBucketCollection;
	#activeLocks = new Map();
	#locksDefaultTTL;
	#isLockingEnabled;

	constructor(cluster, locksOptions, locksBucketCollection) {
		const { locksDefaultTTL } = locksOptions;

		this.#cluster = cluster;
		this.#bucketManager = cluster.buckets();
		this.#queryIndexManager = cluster.queryIndexes();
		this.#searchIndexManager = cluster.searchIndexes();
		this.#locksBucketCollection = locksBucketCollection;
		this.#locksDefaultTTL = locksDefaultTTL;
		this.#isLockingEnabled = true;
	}

	static connectCluster = (host, { username, password }, createLocksBucket = false, locksOptions = { locksDefaultTTL: 5 }) => {
		return new Aigle(async (resolve, reject) => {
			try {
				const cluster = await couchbase.Cluster.connect(host, { username, password });
				if (!cluster._clusterConn) {
					reject(new ConnectionError({ extraDetails: cluster }));
				} else {
					const locksBucketCollection = await this.#initLocks(cluster, createLocksBucket);
					resolve(new CouchbaseHelper(cluster, locksOptions, locksBucketCollection));
				}
			} catch (error) {
				reject(new GeneralError({ extraDetails: error }));
			}
		});
	};

	static #initLocks = (cluster, createLocksBucket) => {
		const createLocksBucketObj = {
			name: 'locks',
			flushEnabled: false,
			ramQuotaMB: 100,
			numReplicas: 1,
			replicaIndexes: false,
			bucketType: couchbase.BucketType.Couchbase,
			ejectionMethod: couchbase.EvictionPolicy.ValueOnly
		};

		return new Aigle(async (resolve, reject) => {
			if (createLocksBucket) {
				try {
					const bucketsManager = cluster.buckets();
					const createLockBucket = await bucketsManager.createBucket(createLocksBucketObj, { timeout: 5000 });
					resolve(cluster.bucket('locks').defaultCollection());
				} catch (error) {
					if (error.constructor.name === 'BucketExistsError') {
						resolve(cluster.bucket('locks').defaultCollection());
					} else {
						reject(error);
					}
				}
			} else {
				resolve(cluster.bucket('locks').defaultCollection());
			}
		});
	};

	// Locks functions.
	setLock = async (lockName, ttl) => {
		return new Aigle(async (resolve, reject) => {
			const expiry = ttl || this.#locksDefaultTTL;
			let createLockRes;
			if (!this.#isLockingEnabled) {
				const error = new Error('Locking is disabled.');
				reject(error);
				return;
			}

			try {
				createLockRes = await this.#locksBucketCollection.insert(lockName, null, { expiry });
			} catch (error) {
				if (error.constructor.name === 'DocumentExistsError') {
					reject('Lock already in use.');
				}
				return;
			}

			this.#activeLocks.set(lockName, createLockRes.cas);
			resolve(createLockRes);
		});
	};

	releaseLock = async (lockName, cas) => {
		return new Aigle(async (resolve, reject) => {
			try {
				const releaseLockRes = await this.#locksBucketCollection.remove(lockName, { cas });
				this.#activeLocks.delete(lockName);
				resolve(releaseLockRes);
			} catch (error) {
				reject(error);
			}
		});
	};

	extendLock = async (lockName, newTtl) => {
		const expiry = newTtl || this.#locksDefaultTTL;

		return new Aigle(async (resolve, reject) => {
			if (!this.#isLockingEnabled) {
				const error = new Error('Locking is disabled.');
				reject(error);
				return;
			}

			try {
				const extendLockRes = await this.#locksBucketCollection.touch(lockName, expiry);

				this.#activeLocks.set(lockName, extendLockRes.cas);
				resolve(extendLockRes);
			} catch (error) {
				reject(error);
			}
		});
	};

	disableLocking = async () => {
		this.#isLockingEnabled = false;
		const locks = [...this.#activeLocks.entries()];

		await Aigle.eachLimit(locks, 10, async ([lockName, cas]) => {
			try {
				const releaseLock = await this.releaseLock(lockName);
			} catch (error) {
				console.log({ error });
			}
		});
	};

	executeFuncWithLock = async ({ lockName, ttl, func }) => {
		try {
			const res = await this.setLock(lockName, ttl);
			await func();
			await this.releaseLock(lockName, res.cas);
			return true;
		} catch (error) {
			// console.log({ error });
			return false;
		}
	};

	waitForLockAndExecute = async ({ lockName, ttl, func }) => {
		const retryOptions = {
			times: Infinity,
			interval: 100
		};
		await Aigle.retry(retryOptions, async () => {
			const isRan = await this.executeFuncWithLock({ lockName, ttl, func });
			// console.log({ isRan });
			if (!isRan) {
				throw new Error('Didnt ran');
			}
			return true;
		});
	};

	// N1QL functions
	executeQuery = (query, bucketName) => {
		return this.#cluster.query(query);
	};

	select = async ({ bucketName, selector }) => {
		const { query } = queryBuilder({ bucketName, selector }, 'select');
		// console.log(query);
		return new Aigle(async (resolve, reject) => {
			try {
				const { meta, rows } = await this.executeQuery(query, bucketName);
				const { metrics } = meta;

				resolve({ rows, ...metrics });
			} catch (error) {
				// console.error({ error });
				reject(error);
			}
		});
	};

	update = async () => {};

	upsert = async () => {};

	insert = async () => {};

	delete = async () => {};

	printQuery = () => {};

	// Documents crud operations.
	upsertDocument = (bucket, doc) => {
		const selectedBucket = this.#cluster.bucket(bucket);
		const collection = selectedBucket.defaultCollection();
		const key = `${doc.type}_${doc.id}`;
		return collection.upsert(key, doc);
	};

	insertDocument = (bucket, doc, options) => {
		const selectedBucket = this.#cluster.bucket(bucket);
		const collection = selectedBucket.defaultCollection();
		const key = `${doc.type}_${doc.id}`;
		return collection.insert(key, doc, options);
	};

	replaceDocument = (bucket, doc, options) => {
		const selectedBucket = this.#cluster.bucket(bucket);
		const collection = selectedBucket.defaultCollection();
		const key = `${doc.type}_${doc.id}`;

		return collection.replace(key, doc, options);
	};

	getDocument = (bucket, docId) => {
		const selectedBucket = this.#cluster.bucket(bucket);
		const collection = selectedBucket.defaultCollection();

		return collection.get(docId);
	};

	removeDocument = async (bucket, docId) => {
		// console.log('here');
		const selectedBucket = this.#cluster.bucket(bucket);
		const collection = selectedBucket.defaultCollection();
		// console.log(collection);

		// const res = await collection.remove(docId);
		// console.log(res);
		return collection.remove(docId);
	};

	// Buckets management.
	createBucket = async ({ bucketName, bucketOptions, createPrimaryIndex = true, columns = [], ignoreIfExists = true }) => {
		const options = {
			flushEnabled: false,
			ramQuotaMB: 256,
			numReplicas: 1,
			replicaIndexes: false,
			bucketType: couchbase.BucketType.Couchbase,
			ejectionMethod: couchbase.EvictionPolicy.ValueOnly,
			...bucketOptions
		};
		let createBucketRes;
		try {
			createBucketRes = await this.#bucketManager.createBucket({ name: bucketName, ...options });
		} catch (error) {
			const errorMessage = error.constructor.name;
			if (ignoreIfExists && errorMessage !== 'BucketExistsError') {
				throw new GeneralError({ extraDetails: error });
			} else if (!ignoreIfExists) {
				throw new GeneralError({ extraDetails: error });
			}

			if (ignoreIfExists && errorMessage === 'BucketExistsError') {
				createBucketRes = 'Bucket already exist.';
			}
		}

		if (createPrimaryIndex) {
			// console.log('here');
			const indexFields = await Aigle.transform(
				columns,
				(result, column) => {
					if (column.index) {
						result.push(column.name);
					}
				},
				[]
			);

			const createPrimaryIndexRes = await this.createPrimaryIndex(bucketName);
			const createColumnsIndex = _.size(indexFields) > 0 ? await this.createIndex(bucketName, indexFields) : 'There is not fields to index.';

			return {
				createBucketRes,
				createPrimaryIndexRes,
				createColumnsIndex
			};
		}
		return createBucketRes;
	};
	dropBucket = (bucketName) => this.#bucketManager.dropBucket(bucketName);

	// Index management.
	createPrimaryIndex = (bucketName, options = { ignoreIfExists: true }) => {
		return this.#queryIndexManager.createPrimaryIndex(bucketName, options);
	};

	createIndex = (bucketName, indexFields, options = { ignoreIfExists: true }) => {
		const indexName = `${bucketName}_index`;
		return this.#queryIndexManager.createIndex(bucketName, indexName, indexFields, options);
	};
}

module.exports = CouchbaseHelper;
