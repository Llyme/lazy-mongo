import { MongoClient, MongoServerError, Document, AggregateOptions } from "mongodb";

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
     * @param {object} [kwargs.query]
     */
    static async *find(kwargs = {}) {
        const {
            database,
            collection,
            query
        } = kwargs;

        const coll = this.getCollection({ database, collection });
        const docs = coll.find(query);

        for await (const doc of docs)
            yield doc;
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {object} [kwargs.filter]
     * @param {object} [kwargs.update]
     * @param {import('mongodb').UpdateOptions} [kwargs.options]
     */
    static async updateSetOne(kwargs = {}) {
        const {
            database,
            collection,
            filter,
            update,
            options
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        try {
            const result = await coll.updateOne(
                filter,
                {
                    $set: update
                },
                {
                    upsert: false,
                    ...options
                }
            );

            if (this.log && result.acknowledged)
                console.log(`[MongoDB.Update] ${coll.dbName}.${coll.collectionName}.${result.modifiedCount}`);

            return {
                ok: true,
                result
            };
        } catch (error) {
            if (!(error instanceof MongoServerError))
                return {
                    ok: false,
                    error
                };

            if (this.log)
                console.log(`[MongoDB.Error] ${coll.dbName}.${coll.collectionName} ${error.code}`);

            return {
                ok: false,
                error
            };
        }
    }

    /**
     * 
     * @param {object} kwargs 
     * @param {string} [kwargs.database]
     * @param {string} [kwargs.collection]
     * @param {object} [kwargs.document]
     */
    static async insertOne(kwargs = {}) {
        const {
            database,
            collection,
            document
        } = kwargs;

        const coll = this.getCollection({ database, collection });

        try {
            const result = await coll.insertOne(document);

            if (this.log && result.acknowledged)
                console.log(`[MongoDB.Insert] ${coll.dbName}.${coll.collectionName}.${result.insertedId}`);

            return {
                ok: true,
                result
            };
        } catch (error) {
            if (!(error instanceof MongoServerError))
                return {
                    ok: false,
                    error
                };

            if (this.log)
                console.log(`[MongoDB.Error] ${coll.dbName}.${coll.collectionName} ${error.code}`);

            return {
                ok: false,
                error
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
     * @param {Document[]} [kwargs.pipeline]
     * @param {AggregateOptions} [kwargs.options]
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