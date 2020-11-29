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
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * DynamoDB v1.10.48 client for YCSB.
 */

public class DynamoDBClient {

  public DynamoDBClient() {
    Cluster.Builder builder = Cluster.build();
    builder.addContactPoint("llu25.cluster-cbo8ghduq2g8.us-east-1.neptune.amazonaws.com");
    builder.port(8182);
    builder.enableSsl(true);
    builder.keyCertChainFile("SFSRootCAG2.pem");

    Cluster cluster = builder.create();

    GraphTraversalSource g = EmptyGraph.instance()
            .traversal()
            .withRemote(
                    DriverRemoteConnection.using(cluster)
            );

    // Add a vertex.
    // Note that a Gremlin terminal step, e.g. next(),
    // is required to make a request to the remote server.
    // The full list of Gremlin terminal steps is at
    // https://tinkerpop.apache.org/docs/current/reference/#terminal-steps
    g.addV("Person").property("Name", "Justin").next();

    // Add a vertex with a user-supplied ID.
    g.addV("Custom Label").property(T.id, "CustomId1")
            .property("name", "Custom id vertex 1").next();
    g.addV("Custom Label").property(T.id, "CustomId2")
            .property("name", "Custom id vertex 2").next();

    g.addE("Edge Label").from(g.V("CustomId1")).to(g.V("CustomId2")).next();

    // This gets the vertices, only.
    GraphTraversal t = g.V().limit(3).valueMap();

    t.forEachRemaining(
            e ->  System.out.println(e)
    );

    cluster.close();
  }


  public static void main(String[] args) {
    DynamoDBClient dynamoDBClient = new DynamoDBClient();
  }

}
