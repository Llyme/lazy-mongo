import { MongoClient, MongoError } from "mongodb";
import chalk from "chalk";

/**
 * @typedef {Object} InsertOneResponse
 * @property {boolean} ok
 * @property {import('mongodb').InsertOneResult} [result]
 * @property {boolean} isDuplicate
 * @property {MongoError} [error]
 */

/**
 * @typedef {Object} UpdateResponse
 * @property {boolean} ok
 * @property {import('mongodb').UpdateResult} [result]
 * @property {boolean} isDuplicate
 * @property {MongoError} [error]
 */

export class LazyMongo {
    /**
     * @type {MongoClient}
     */
    static mongo = null;
    /**
     * @type {string}
     */
    static defaultDatabase = null;
    /**
     * @type {string}
     */
    static defaultCollection = null;
    /**
     * @type {boolean}
     */
    static log = true;

    /**
     * 
     * @param {string} connectionString 
     * @param {import("mongodb").MongoClientOptions} options
     */
    static connect(connectionString, options = undefined) {
        if (connectionString == null)
            return null;

        if (connectionString.length == 0)
            return null;

        return this.mongo = new MongoClient(connectionString, options);
    }

    /**
     * 
     * @param {string} database 
     */
    static getDatabase(database = undefined) {
        return this.mongo.db(database ?? this.defaultDatabase);
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     */
    static getCollection(kwargs = {}) {
        const {
            database,
            collection
        } = kwargs;

        const db = this.getDatabase(database);

        return db.collection(collection ?? this.defaultCollection);
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {object} [kwargs.filter]
     * @param {import('mongodb').FindOptions<import('mongodb').Document>} [kwargs.options]
     */
    static async *find(kwargs = {}) {
        const {
            database,
            collection,
            filter,
            options
        } = kwargs;

        const coll = this.getCollection({ database, collection });
        const docs = coll.find(filter, options);

        for await (const doc of docs)
            yield doc;
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {import('mongodb').Filter<import('mongodb').Document>} [kwargs.filter]
     * @param {import('mongodb').FindOptions<import('mongodb').Document>} [kwargs.options]
     */
    static async findOne(kwargs = {}) {
        const {
            database,
            collection,
            filter,
            options
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        return await coll.findOne(filter, options);
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {import('mongodb').Filter<import('mongodb').Document>} [kwargs.filter]
     * @param {import('mongodb').Document[] | import('mongodb').UpdateFilter<import('mongodb').Document>} kwargs.update
     * @param {import('mongodb').UpdateOptions} [kwargs.options]
     * @param {boolean} [kwargs.log]
     * 
     * @returns {Promise<UpdateResponse>}
     */
    static async updateOne(kwargs = {}) {
        const {
            database,
            collection,
            filter,
            update,
            options,
            log = this.log
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        try {
            const result = await coll.updateOne(
                filter,
                update,
                options
            );

            if (log && result.acknowledged)
                console.log(
                    result.modifiedCount > 0
                        ? chalk.bgGreen('[MongoDB.Update]')
                        : chalk.bgYellow('[MongoDB.Update]'),
                    chalk.blue.bold(coll.dbName),
                    chalk.cyan.bold(coll.collectionName),
                    result.modifiedCount
                );

            return {
                ok: true,
                result,
                isDuplicate: false
            };

        } catch (error) {
            if (!(error instanceof MongoError))
                return {
                    ok: false,
                    error,
                    isDuplicate: false,
                };

            if (log)
                console.log(
                    chalk.bgRed('[MongoDB.Update]'),
                    chalk.blue.bold(coll.dbName),
                    chalk.cyan.bold(coll.collectionName),
                    chalk.red(error.code)
                );

            return {
                ok: false,
                error,
                isDuplicate: error.code == 11000
            };
        }
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {import('mongodb').Filter<import('mongodb').Document>} [kwargs.filter]
     * @param {object} kwargs.update
     * @param {import('mongodb').UpdateOptions} [kwargs.options]
     * 
     * @returns {Promise<UpdateResponse>}
     */
    static async updateSetOne(kwargs = {}) {
        const {
            update,
            options
        } = kwargs;

        return await this.updateOne({
            ...kwargs,
            update: {
                $set: update
            },
            options: {
                upsert: false,
                ...options
            }
        });
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {object} [kwargs.document]
     * @param {import('mongodb').InsertOneOptions} [kwargs.options]
     * @param {boolean} [kwargs.log]
     * 
     * @returns {Promise<InsertOneResponse>}
     */
    static async insertOne(kwargs = {}) {
        const {
            database,
            collection,
            document,
            options,
            log = this.log
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        try {
            const result = await coll.insertOne(document, options);

            if (log && result.acknowledged)
                console.log(
                    chalk.bgGreen('[MongoDB.Insert]'),
                    chalk.blue.bold(coll.dbName),
                    chalk.cyan.bold(coll.collectionName),
                    chalk.bold(result.insertedId.toHexString())
                );

            return {
                ok: true,
                result,
                isDuplicate: false
            };

        } catch (error) {
            if (!(error instanceof MongoError))
                return {
                    ok: false,
                    error,
                    isDuplicate: false,
                };

            if (log)
                console.log(
                    chalk.bgRed('[MongoDB.Insert]'),
                    chalk.blue.bold(coll.dbName),
                    chalk.cyan.bold(coll.collectionName),
                    chalk.red.bold(error.code)
                );

            return {
                ok: false,
                error,
                isDuplicate: error.code == 11000
            };
        }
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {object} [kwargs.filter]
     */
    static async count(kwargs = {}) {
        const {
            database,
            collection,
            filter
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        return await coll.countDocuments(filter);
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {import('mongodb').Document[]} [kwargs.pipeline]
     * @param {import('mongodb').AggregateOptions} [kwargs.options]
     */
    static aggregate(kwargs = {}) {
        const {
            database,
            collection,
            pipeline,
            options
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        return coll.aggregate(pipeline, options);
    }
}