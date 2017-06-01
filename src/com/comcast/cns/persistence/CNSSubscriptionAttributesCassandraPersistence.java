/**
 * Copyright 2012 Comcast Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.cns.persistence;

import com.comcast.cmb.common.persistence.*;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.model.CNSTopicDeliveryPolicy;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CNSSubscriptionAttributesCassandraPersistence extends BaseCassandraDao<CNSSubscriptionAttributes> implements ICNSSubscriptionAttributesPersistence {

	private static final String columnFamilySubscriptionAttributes = "cns_subscription_attributes";
	private static Logger logger = Logger.getLogger(CNSSubscriptionAttributesCassandraPersistence.class);
	private static final CassandraDataStaxPersistence cassandraHandler = DurablePersistenceFactory.getInstance();

	private final PreparedStatement getSubscriptionAttributes;
	private final PreparedStatement insertSubscriptionAttributes;

	public CNSSubscriptionAttributesCassandraPersistence() {
		super(cassandraHandler.getSession());

		getSubscriptionAttributes = session.prepare(
			QueryBuilder.select().all()
						.from("cns", "cns_subscription_attributes")
						.where(eq("subscription_arn", bindMarker("subscription_arn")))
						.limit(1)
			);

		insertSubscriptionAttributes =  session.prepare(
			QueryBuilder.insertInto("cns", "cns_subscription_attributes")
						.value("subscription_arn", bindMarker("subscription_arn"))
						.value("confirmation_was_authenticated", bindMarker("confirmation_was_authenticated"))
						.value("delivery_policy", bindMarker("delivery_policy"))
						.value("topic_arn", bindMarker("topic_arn"))
						.value("user_id", bindMarker("user_id"))
		);
	}



	public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception {
		BoundStatement statement = insertSubscriptionAttributes.bind()
															   .setString("subscription_arn", subscriptionArn)
															   .setBool("confirmation_was_authenticated", subscriptionAtributes.isConfirmationWasAuthenticated())
															   .setString("topic_arn", subscriptionAtributes.getTopicArn())
															   .setString("user_id", subscriptionAtributes.getUserId());
		if (subscriptionAtributes.getDeliveryPolicy() != null) {
			statement.setString("delivery_policy", subscriptionAtributes.getDeliveryPolicy().toJSON().toString());
		}

		save(Lists.newArrayList(statement));

		String topicArn = com.comcast.cns.util.Util.getCnsTopicArn(subscriptionArn);
		CNSCache.removeTopicAttributes(topicArn);
	}

	public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception {
		return findOne(getSubscriptionAttributes.bind().setString("subscription_arn", subscriptionArn));
	}

	@Override
	protected CNSSubscriptionAttributes convertToInstance(Row row) {
		CNSSubscriptionAttributes subscriptionAttributes = new CNSSubscriptionAttributes(row.getString("topic_arn"), row.getString("subscription_arn"), row.getString("user_id"));

		try {
			if (row.getColumnDefinitions().contains("confirmation_was_authenticated") && !row.isNull("confirmation_was_authenticated")) {
				subscriptionAttributes.setConfirmationWasAuthenticated(row.getBool("confirmation_was_authenticated"));
			}

			if (row.getColumnDefinitions().contains("delivery_policy") && !row.isNull("delivery_policy")) {
				subscriptionAttributes.setDeliveryPolicy(new CNSSubscriptionDeliveryPolicy(new JSONObject(row.getString("delivery_policy"))));
			}

			// if "ignore subscription override" is checked, get effective delivery policy from topic delivery policy, otherwise
			// get effective delivery policy from subscription delivery policy

			CNSSubscription subscription = PersistenceFactory.getSubscriptionPersistence().getSubscription(subscriptionAttributes.getSubscriptionArn());

			if (subscription == null) {
				throw new SubscriberNotFoundException("Subscription not found. arn=" + subscriptionAttributes.getSubscriptionArn());
			}

			CNSTopicAttributes topicAttributes = CNSTopicAttributesCassandraPersistence.getInstance().getTopicAttributes(subscriptionAttributes.getTopicArn());

			if (topicAttributes != null) {

				CNSTopicDeliveryPolicy topicEffectiveDeliveryPolicy = topicAttributes.getEffectiveDeliveryPolicy();

				if (topicEffectiveDeliveryPolicy != null) {

					if (topicEffectiveDeliveryPolicy.isDisableSubscriptionOverrides() || subscriptionAttributes.getDeliveryPolicy() == null) {
						CNSSubscriptionDeliveryPolicy effectiveDeliveryPolicy = new CNSSubscriptionDeliveryPolicy();
						effectiveDeliveryPolicy.setHealthyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultHealthyRetryPolicy());
						effectiveDeliveryPolicy.setSicklyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultSicklyRetryPolicy());
						effectiveDeliveryPolicy.setThrottlePolicy(topicEffectiveDeliveryPolicy.getDefaultThrottlePolicy());
						subscriptionAttributes.setEffectiveDeliveryPolicy(effectiveDeliveryPolicy);
					}
				}
			}

			if (subscriptionAttributes.getEffectiveDeliveryPolicy() == null) {
				subscriptionAttributes.setEffectiveDeliveryPolicy(subscriptionAttributes.getDeliveryPolicy());
			}

		} catch (Exception e) {
			logger.error("Error parsing subscription attributes: " + e.toString());
		}

		return subscriptionAttributes;
	}
}
