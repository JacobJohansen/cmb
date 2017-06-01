package com.comcast.cmb.common.persistence;

import com.comcast.cmb.common.util.CMBProperties;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CassandraDataStaxPersistence {
    public static final String CLUSTER_NAME = CMBProperties.getInstance().getClusterName();
    public static final String CLUSTER_URL = CMBProperties.getInstance().getClusterUrl();

    public static final String CMB_KEYSPACE = CMBProperties.getInstance().getCMBKeyspace();
    public static final String CQS_KEYSPACE = CMBProperties.getInstance().getCQSKeyspace();
    public static final String CNS_KEYSPACE = CMBProperties.getInstance().getCNSKeyspace();

    public static final String CNS_TOPICS = "cns_topics";
    public static final String CNS_TOPICS_BY_USER_ID = "cns_topics_by_user_id";
    public static final String CNS_TOPIC_SUBSCRIPTIONS = "cns_topic_subscriptions";
    public static final String CNS_TOPIC_SUBSCRIPTIONS_INDEX = "cns_topic_subscriptions_index";
    public static final String CNS_TOPIC_SUBSCRIPTIONS_USER_INDEX = "cns_topic_subscriptions_user_index";
    public static final String CNS_TOPIC_SUBSCRIPTIONS_TOKEN_INDEX = "cns_topic_subscriptions_token_index";
    public static final String CNS_TOPIC_ATTRIBUTES = "cns_topic_attributes";
    public static final String CNS_SUBSCRIPTION_ATTRIBUTES = "cns_subscription_attributes";
    public static final String CNS_TOPIC_STATS = "cns_topic_stats";
    public static final String CNS_WORKERS = "cns_workers";
    public static final String CNS_API_SERVERS = "cns_api_servers";

    public static final String CQS_QUEUES = "cqs_queues";
    public static final String CQS_QUEUES_BY_USER_ID = "cqs_queues_by_user_id";
    public static final String CQS_PARTITIONED_QUEUE_MESSAGES = "cqs_partitioned_queue_messages";
    public static final String CQS_API_SERVERS = "cqs_api_servers";

    public static final String CMB_USERS = "users";

    private static Logger logger = Logger.getLogger(CassandraDataStaxPersistence.class);

    private static CassandraDataStaxPersistence instance;

    private static Cluster cluster;
    private static Session session;
    private static MappingManager mappingManager;

    public static CassandraDataStaxPersistence getInstance() {

        if (instance == null) {
            instance = new CassandraDataStaxPersistence();
        }

        return instance;
    }

    private CassandraDataStaxPersistence() {
        initPersistence();
    }

    private void initPersistence() {

        List<String> keyspaceNames = new ArrayList<String>();
        keyspaceNames.add(CMBProperties.getInstance().getCMBKeyspace());
        keyspaceNames.add(CMBProperties.getInstance().getCNSKeyspace());
        keyspaceNames.add(CMBProperties.getInstance().getCQSKeyspace());

        String dataCenter = CMBProperties.getInstance().getCassandraDataCenter();
        String username = CMBProperties.getInstance().getCassandraUsername();
        String password = CMBProperties.getInstance().getCassandraPassword();

        String[] urlAndPort = CLUSTER_URL.split(":");
        String host = urlAndPort.length >= 1 ? urlAndPort[0] : null;
        Integer port = urlAndPort.length >= 2 ? Integer.parseInt(urlAndPort[1]) : null;

        if (host == null) {
            logger.error("Missing Host From Config defaulting to localhost");
            host = "localhost";
        }
        if (port == null) {
            logger.warn("Missing Port From Config defaulting to 9042");
            port = 9042;
        }

        cluster = Cluster.builder().addContactPoint(host)
                         .withCredentials(username, password)
                         .withCompression(ProtocolOptions.Compression.LZ4)
                         .withPort(port)
                         .build();

        session = cluster.connect();
        mappingManager = new MappingManager(session);

    }

    public Session getSession() {
        return session;
    }

    public MappingManager getMappingManager() {
        return mappingManager;
    }
}
