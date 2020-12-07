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

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.
        remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.
        traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.
        traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.
        util.empty.EmptyGraph;
import site.ycsb.*;
import java.util.*;

/**
 * DynamoDB v1.10.48 client for YCSB.
 */

public class DynamoDBClient extends DB {

  private GraphTraversalSource g;
  private Cluster cluster;

  @Override
  public void init() throws DBException {
    Cluster.Builder builder = Cluster.build();
    builder.addContactPoint("llu25.cluster-cbo8ghduq2g8.us-east-1.neptune.amazonaws.com");
    builder.port(8182);
    builder.enableSsl(true);
    builder.keyCertChainFile("SFSRootCAG2.pem");

    cluster = builder.create();

    g = EmptyGraph.instance().traversal()
            .withRemote(DriverRemoteConnection.using(cluster));
  }

  @Override
  public void cleanup() throws DBException {
    g.V().drop().iterate();
    cluster.close();
  }

  @Override
  public Status read(String table, String key,
                     Set<String> fields, Map<String, ByteIterator> result) {
    GraphTraversal<Vertex, Map<Object, Object>> curr = g.V(key).valueMap();
    while (curr.hasNext()) {
      Map<Object, Object> e = curr.next();
      for (Map.Entry<Object, Object> entry : e.entrySet()) {
        result.put((String) entry.getKey(), new StringByteIterator((String) entry.getValue()));
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    // This gets the vertices, only.
    GraphTraversal<Vertex, Map<Object, Object>> t =
            g.V().limit(recordcount).valueMap();
    while (t.hasNext()) {
      Map<Object, Object> e = t.next();
      HashMap<String, ByteIterator> map = new HashMap<>();
      for (String str : fields) {
        map.put(str, (ByteIterator) e.get(str));
      }
      result.add(map);
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    insert(table, key, values);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    // Add a vertex.
    // Note that a Gremlin terminal step, e.g. next(),
    // is required to make a request to the remote server.
    // The full list of Gremlin terminal steps is at
    // https://tinkerpop.apache.org/docs/current/reference/#terminal-steps
    // g.addV("Person").property("Name", "Justin").next();
    GraphTraversal<Vertex, Vertex> curr = g.addV(key);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      curr.property(entry.getKey(), entry.getValue().toString());
    }
    curr.next();
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    g.V(key).drop();
    return Status.OK;
  }

}