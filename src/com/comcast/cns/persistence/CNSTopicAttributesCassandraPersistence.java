package com.comcast.cns.persistence;

import com.comcast.cmb.common.persistence.BaseCassandraDao;
import com.comcast.cmb.common.persistence.CassandraDataStaxPersistence;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CNSTopicAttributesCassandraPersistence extends BaseCassandraDao<CNSTopicAttributes> implements ICNSTopicAttributesPersistence {
    private static final String columnFamilyTopicAttributes = "cns_topic_attributes";
    private static final String columnFamilyTopicStats = "cns_topic_stats";
    private static Logger logger = Logger.getLogger(CNSSubscriptionAttributesCassandraPersistence.class);
    private static final CassandraDataStaxPersistence cassandraHandler = DurablePersistenceFactory.getInstance();

    private static CNSTopicAttributesCassandraPersistence cnsTopicAttributesCassandraPersistence;
    public static CNSTopicAttributesCassandraPersistence getInstance() {
        if(cnsTopicAttributesCassandraPersistence == null) {
            cnsTopicAttributesCassandraPersistence = new CNSTopicAttributesCassandraPersistence();
        }
        return cnsTopicAttributesCassandraPersistence;
    }

    private PreparedStatement saveTopicAttribute;
    private PreparedStatement findTopicAttribute;
    private PreparedStatement removeTopicAttribute;

    private PreparedStatement findStatusCount;

    private final PreparedStatement incrementCounter;
    private final PreparedStatement decrementCounter;

    private CNSTopicAttributesCassandraPersistence() {
        super(cassandraHandler.getSession());

        saveTopicAttribute = session.prepare(
              QueryBuilder.insertInto("cns", columnFamilyTopicAttributes)
                            .value("topic_arn", bindMarker("topic_arn"))
                            .value("effective_delivery_policy", bindMarker("effective_delivery_policy"))
                            .value("policy", bindMarker("policy"))
                            .value("user_id", bindMarker("user_id"))
        );

        findTopicAttribute = session.prepare(
              QueryBuilder.select()
                          .all()
                          .from("cns", columnFamilyTopicAttributes)
                          .where(eq("topic_arn", bindMarker("topic_arn")))
        );

        removeTopicAttribute = session.prepare(
              QueryBuilder.delete()
                          .from("cns", columnFamilyTopicAttributes)
                          .where(eq("topic_arn", bindMarker("topic_arn")))
        );

        findStatusCount = session.prepare(
              QueryBuilder.select().column("value")
                          .from("cns", columnFamilyTopicStats)
                          .where(eq("topic_arn", bindMarker("topic_arn")))
                          .and(eq("status", bindMarker("status")))
                          .limit(1)
        );

        incrementCounter = session.prepare(
              QueryBuilder.update("cns", columnFamilyTopicStats)
                          .where(eq("topic_arn", bindMarker("topic_arn")))
                          .and(eq("status", bindMarker("status")))
                          .with(incr("value", bindMarker("count")))
        );

        decrementCounter = session.prepare(
              QueryBuilder.update("cns", columnFamilyTopicStats)
                          .where(eq("topic_arn", bindMarker("topic_arn")))
                          .and(eq("status", bindMarker("status")))
                          .with(decr("value", bindMarker("count")))
        );
    }

    public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception {
        BoundStatement saveStatement = saveTopicAttribute.bind()
                                                         .setString("user_id",topicAttributes.getUserId())
                                                         .setString("topic_arn", topicArn)
                                                         .setString("effective_delivery_policy", topicAttributes.getDeliveryPolicy().toJSON().toString())
                                                         .setString("policy", topicAttributes.getPolicy());
        save(Lists.newArrayList(saveStatement));

        if (topicAttributes.getDisplayName() != null) {
            PersistenceFactory.getTopicPersistence().updateTopicDisplayName(topicArn, topicAttributes.getDisplayName());
        }

        CNSCache.removeTopicAttributes(topicArn);
    }

    public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception {

        CNSTopicAttributes topicAttributes = findOne(findTopicAttribute.bind().setString("topic_arn", topicArn));

        if (topicAttributes == null) {
            return null;
        }

        CNSTopic topic = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
        if(topic != null) {
            topicAttributes.setDisplayName(PersistenceFactory.getTopicPersistence().getTopic(topicArn).getDisplayName());
        }
        long subscriptionConfirmedCount = getTopicStats(topicArn, "subscriptionConfirmed");
        topicAttributes.setSubscriptionsConfirmed(subscriptionConfirmedCount);
        long subscriptionPendingCount = getTopicStats(topicArn, "subscriptionPending");
        topicAttributes.setSubscriptionsPending(subscriptionPendingCount);
        long subscriptionDeletedCount = getTopicStats(topicArn, "subscriptionDeleted");
        topicAttributes.setSubscriptionsDeleted(subscriptionDeletedCount);
        return topicAttributes;
    }

    @Override
    public void removeTopicAttributes(String topicArn) {
        delete(removeTopicAttribute.bind().setString("topic_arn",topicArn));
    }

    public long getTopicStats(String topicArn, String status) {


              Row resultSet = session.execute(findStatusCount.bind()
                                                       .setString("topic_arn", topicArn)
                                                       .setString("status", status)).one();
              if(resultSet != null && !resultSet.isNull("value")) {
                  return  resultSet.getLong("value");
              }
              return 0;
    }

    @Override
    protected CNSTopicAttributes convertToInstance(Row row) {
        CNSTopicAttributes cnsTopicAttributes = new CNSTopicAttributes(row.getString("topic_arn"), row.getString("user_id"));

        if(row.getColumnDefinitions().contains("policy")) {
            cnsTopicAttributes.setPolicy(row.getString("policy"));
        }

        try {
            CNSTopicDeliveryPolicy deliveryPolicy;
            if (row.getColumnDefinitions().contains("effective_delivery_policy") && !row.isNull("effective_delivery_policy")) {

                deliveryPolicy = new CNSTopicDeliveryPolicy(new JSONObject(row.getString("effective_delivery_policy")));
            } else {
                deliveryPolicy = new CNSTopicDeliveryPolicy();
            }
            cnsTopicAttributes.setEffectiveDeliveryPolicy(deliveryPolicy);
            cnsTopicAttributes.setDeliveryPolicy(deliveryPolicy);
        } catch (Exception e) {

        }

        return cnsTopicAttributes;
    }

    public void incrementCounter(String topicArn, String status) {
        incrementCounter(topicArn, status, 1);
    }

    public void incrementCounter(String topicArn, String status, int count) {
        save(Lists.newArrayList(
              incrementCounter.bind()
                              .setString("topic_arn", topicArn)
                              .setString("status", status)
                              .setLong("count", count)
        ));
    }

    public void decrementCounter(String topicArn, String status) {
        decrementCounter(topicArn, status, 1);
    }

    public void decrementCounter(String topicArn, String status, int count) {
        save(Lists.newArrayList(
              decrementCounter.bind()
                              .setString("topic_arn", topicArn)
                              .setString("status", status)
                              .setLong("count", count)
        ));
    }
}
