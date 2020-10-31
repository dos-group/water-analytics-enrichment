package de.tu_berlin.cit.watergridsense_pipelines.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class Cassandra {

    public static class Connector {

        private static Connector self = null;
        public final Session session;

        private Connector(ClusterBuilder builder) {

            this.session = builder.getCluster().connect();
        }

        public static synchronized Connector getInstance(ClusterBuilder builder) {

            if (self == null) self = new Connector(builder);
            return self;
        }
    }

    public static ResultSet execute(String query, ClusterBuilder builder) {

        Connector connector = Connector.getInstance(builder);
        return connector.session.execute(query);
    }

    public static ResultSetFuture executeAsync(String query, ClusterBuilder builder) {
        Connector connector = Connector.getInstance(builder);
        return connector.session.executeAsync(query);
    }

    public static void close() {

        if (Connector.self != null) Connector.self.session.close();
    }
}
