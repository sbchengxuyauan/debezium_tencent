/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms.UpdateOperators;

import static org.fest.assertions.Assertions.assertThat;

import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;

import com.mongodb.MongoClient;

import io.debezium.connector.mongodb.transforms.AbstractExtractNewDocumentStateTestIT;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;

/**
 * Integration test for {@link ExtractNewDocumentState}. It sends operations into MongoDB and listens on messages that
 * are generated by Debezium plug-in. The messages are then run through the SMT itself.
 * <p>
 * This tries to cover every mongo update operation as described in the official documentation
 * {@see https://docs.mongodb.com/v3.6/reference/operator/update/#id1}
 *
 * @author Renato Mefi
 */
abstract class AbstractExtractNewDocumentStateUpdateOperatorsTestIT extends AbstractExtractNewDocumentStateTestIT {

    @Override
    protected String getCollectionName() {
        return "update_operators";
    }

    SourceRecord executeSimpleUpdateOperation(String updateDocument) throws InterruptedException {
        primary().execute("insert", createInsertItemDefault(1));

        SourceRecords records = consumeRecordsByTopic(1);

        assertThat(records.recordsForTopic(this.topicName())).hasSize(1);

        primary().execute("update", createUpdateOneItem(1, updateDocument));

        return getUpdateRecord();
    }

    private Consumer<MongoClient> createInsertItemDefault(int id) {
        return client -> client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                .insertOne(Document.parse("{" + "'_id': " + id + "," + "'dataStr': 'hello'," + "'dataInt': 123,"
                        + "'dataLong': 80000000000," + "'dataBoolean': true," + "'dataByte': -1,"
                        + "'dataArrayOfStr': ['a','c','e']," + "'nested': {" + "'dataStr': 'hello'," + "'dataInt': 123,"
                        + "'dataLong': 80000000000," + "'dataBoolean': true," + "'dataByte': -1" + "}}"));
    }

    private Consumer<MongoClient> createUpdateOneItem(int id, String document) {
        return client -> client.getDatabase(DB_NAME).getCollection(this.getCollectionName())
                .updateOne(Document.parse(String.format("{'_id' : %d}", id)), Document.parse(document));
    }
}
