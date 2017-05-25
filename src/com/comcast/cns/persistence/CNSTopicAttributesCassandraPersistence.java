package com.comcast.cns.persistence;

import com.comcast.cmb.common.persistence.*;
import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CNSTopicAttributesCassandraPersistence extends BaseCassandraDao<CNSTopicAttributes> implements ICNSTopicAttributesPersistence {
    private static final String columnFamilyTopicAttributes = "CNSTopicAttributes";
    private static final String columnFamilyTopicStats = "CNSTopicStats";
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
    private PreparedStatement findStatusCount;

    private CNSTopicAttributesCassandraPersistence() {
        super(cassandraHandler.getSession());

        saveTopicAttribute = session.prepare(
              QueryBuilder.insertInto("CNS", columnFamilyTopicAttributes)
                            .value("topicArn", bindMarker("topicArn"))
                            .value("effectiveDeliveryPolicy", bindMarker("effectiveDeliveryPolicy"))
                            .value("policy", bindMarker("policy"))
                            .value("userId", bindMarker("userId"))
        );

        findTopicAttribute = session.prepare(
              QueryBuilder.select()
                          .all()
                          .from("CNS", columnFamilyTopicAttributes)
                          .where(eq("topicArn", bindMarker("topicArn")))
        );

        findStatusCount = session.prepare(
              QueryBuilder.select().column("value")
                          .from("CNS", columnFamilyTopicStats)
                          .where(eq("topicArn", bindMarker("topicArn")))
                          .and(eq("status", bindMarker("status")))
                          .limit(1)
        );

    }

    public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception {

        save(Lists.newArrayList(saveTopicAttribute.bind()
                                                  .setUUID("userId",topicAttributes.getUserId())
                                                  .setString("topicArn", topicArn)
                                                  .setString("effectiveDeliveryPolicy", topicAttributes.getEffectiveDeliveryPolicy().toJSON().toString())
                                                  .setString("policy", topicAttributes.getPolicy())

        ));

        if (topicAttributes.getDisplayName() != null) {
            PersistenceFactory.getTopicPersistence().updateTopicDisplayName(topicArn, topicAttributes.getDisplayName());
        }

        CNSCache.removeTopicAttributes(topicArn);
    }

    public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception {

        CNSTopicAttributes topicAttributes = findOne(findTopicAttribute.bind().setString("topicArn", topicArn));

        if (topicAttributes != null) {
            return null;
        }
        topicAttributes.setDisplayName(PersistenceFactory.getTopicPersistence().getTopic(topicArn).getDisplayName());
        long subscriptionConfirmedCount = session.execute(findStatusCount.bind().setString("topicArn", topicArn).setString("status", "subscriptionConfirmed")).one().getLong("value");
        topicAttributes.setSubscriptionsConfirmed(subscriptionConfirmedCount);
        long subscriptionPendingCount = session.execute(findStatusCount.bind().setString("topicArn", topicArn).setString("status", "subscriptionPending")).one().getLong("value");
        topicAttributes.setSubscriptionsPending(subscriptionPendingCount);
        long subscriptionDeletedCount = session.execute(findStatusCount.bind().setString("topicArn", topicArn).setString("status", "subscriptionDeleted")).one().getLong("value");
        topicAttributes.setSubscriptionsDeleted(subscriptionDeletedCount);
        return topicAttributes;
    }

    public long getTopicStats(String topicArn, String status) {
        return session.execute(findStatusCount.bind()
                                              .setString("topicArn", topicArn)
                                              .setString("status", status))
                      .one()
                      .getLong("value");
    }

    @Override
    protected CNSTopicAttributes convertToInstance(Row row) {
        CNSTopicAttributes cnsTopicAttributes = new CNSTopicAttributes(row.getString("topicArn"), row.getUUID("userId"));

        if(row.getColumnDefinitions().contains("policy")) {
            cnsTopicAttributes.setPolicy(row.getString("policy"));
        }

        try {
            CNSTopicDeliveryPolicy deliveryPolicy;

            if (row.getColumnDefinitions().contains("effectiveDeliveryPolicy") && !row.isNull("effectiveDeliveryPolicy")) {
                deliveryPolicy = new CNSTopicDeliveryPolicy(new JSONObject(row.getString("effectiveDeliveryPolicy")));
            } else {
                deliveryPolicy = new CNSTopicDeliveryPolicy();
            }
            cnsTopicAttributes.setEffectiveDeliveryPolicy(deliveryPolicy);
            cnsTopicAttributes.setDeliveryPolicy(deliveryPolicy);
        } catch (Exception e) {
            logger.error("failed to parse effective delivery policy: " + e.toString());
        }

        return cnsTopicAttributes;
    }
}
