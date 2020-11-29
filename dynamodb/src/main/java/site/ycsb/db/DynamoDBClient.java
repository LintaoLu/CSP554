/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db;


import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.Binary;
import site.ycsb.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * DynamoDB v1.10.48 client for YCSB.
 */

public class DynamoDBClient extends DB {

  private MongoClientURI clientURI;
  private MongoClient mongoClient;
  private MongoDatabase testDB;
  private static final String PK = "_id";
  private static final Integer INCLUDE = Integer.valueOf(1);

  public void init() {
    String template = "mongodb://%s:%s@%s/sample-database?replicaSet=rs0&readpreference=%s";
    String username = "llu25";
    String password = "huhu87588315";
    String clusterEndpoint = "llu25.cluster-cbo8ghduq2g8.us-east-1.docdb.amazonaws.com:27017";
    String readPreference = "secondaryPreferred";
    String connectionString = String.format(template, username, password, clusterEndpoint, readPreference);


    clientURI = new MongoClientURI(connectionString);
    mongoClient = new MongoClient(clientURI);

    testDB = mongoClient.getDatabase("userdb");
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = testDB.getCollection(table);
      Document query = new Document(PK, key);

      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    MongoCursor<Document> cursor = null;
    try {
      MongoCollection<Document> collection = testDB.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document(PK, scanRange);
      Document sort = new Document(PK, INCLUDE);

      FindIterable<Document> findIterable =
              collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
                new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
                new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = testDB.getCollection(table);

      Document query = new Document(PK, key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    MongoCollection<Document> numbersCollection = testDB.getCollection(table);
    Document doc = new Document(PK, key);
    for (Map.Entry<String, ByteIterator> value : values.entrySet()) {
      doc.append(value.getKey(), value.getValue().toArray());
    }
    numbersCollection.insertOne(doc);
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = testDB.getCollection(table);

      Document query = new Document(PK, key);
      DeleteResult result = collection.deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

}
