package site.ycsb.db;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

public class Test {

    public Test() {
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
        Test t = new Test();
    }
}
