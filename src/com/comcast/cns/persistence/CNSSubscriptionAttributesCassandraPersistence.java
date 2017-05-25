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

	private static final String columnFamilySubscriptionAttributes = "CNSSubscriptionAttributes";
	private static Logger logger = Logger.getLogger(CNSSubscriptionAttributesCassandraPersistence.class);
	private static final CassandraDataStaxPersistence cassandraHandler = DurablePersistenceFactory.getInstance();

	private final PreparedStatement getSubscriptionAttributes;
	private final PreparedStatement insertSubscriptionAttributes;

	public CNSSubscriptionAttributesCassandraPersistence() {
		super(cassandraHandler.getSession());

		getSubscriptionAttributes = session.prepare(
			QueryBuilder.select().all()
						.from("CNS", "CNSSubscriptionAttributes")
						.where(eq("subscriptionArn", bindMarker("subscriptionArn")))
						.limit(1)
			);

		insertSubscriptionAttributes =  session.prepare(
			QueryBuilder.insertInto("CNS", "CNSSubscriptionAttributes")
						.value("subscriptionArn", bindMarker("subscriptionArn"))
						.value("confirmationWasAuthenticated", bindMarker("confirmationWasAuthenticated"))
						.value("deliveryPolicy", bindMarker("deliveryPolicy"))
						.value("effectiveDeliveryPolicy", bindMarker("effectiveDeliveryPolicy"))
						.value("topicArn", bindMarker("topicArn"))
						.value("userId", bindMarker("userId"))
		);
	}



	public void setSubscriptionAttributes(CNSSubscriptionAttributes subscriptionAtributes, String subscriptionArn) throws Exception {
		save(Lists.newArrayList(insertSubscriptionAttributes.bind()
															.setString("subscriptionArn", subscriptionArn)
															.setBool("confirmationWasAuthenticated", subscriptionAtributes.isConfirmationWasAuthenticated())
															.setString("deliveryPolicy", subscriptionAtributes.getDeliveryPolicy().toJSON().toString())
															.setString("effectiveDeliveryPolicy", subscriptionAtributes.getEffectiveDeliveryPolicy().toJSON().toString())
															.setString("topicArn", subscriptionAtributes.getTopicArn())
															.setString("userId", subscriptionAtributes.getUserId())
		));

		String topicArn = com.comcast.cns.util.Util.getCnsTopicArn(subscriptionArn);
		CNSCache.removeTopicAttributes(topicArn);
	}

	public CNSSubscriptionAttributes getSubscriptionAttributes(String subscriptionArn) throws Exception {
		return findOne(getSubscriptionAttributes.bind().setString("subscriptionArn", subscriptionArn));
	}

	@Override
	protected CNSSubscriptionAttributes convertToInstance(Row row) {
		CNSSubscriptionAttributes subscriptionAttributes = new CNSSubscriptionAttributes(row.getString("topicArn"), row.getString("subscriptionArn"), row.getString("userId"));

		try {
			if (row.getColumnDefinitions().contains("confirmationWasAuthenticated") && !row.isNull("confirmationWasAuthenticated")) {

				subscriptionAttributes.setConfirmationWasAuthenticated(row.getBool("confirmationWasAuthenticated"));
			}

			if (row.getColumnDefinitions().contains("deliveryPolicy") && !row.isNull("deliveryPolicy")) {
				subscriptionAttributes.setDeliveryPolicy(new CNSSubscriptionDeliveryPolicy(new JSONObject(row.getString("deliveryPolicy"))));

			}

			// if "ignore subscription override" is checked, get effective delivery policy from topic delivery policy, otherwise
			// get effective delivery policy from subscription delivery policy

			CNSSubscription subscription = PersistenceFactory.getSubscriptionPersistence().getSubscription(subscriptionAttributes.getSubscriptionArn());

			if (subscription == null) {
				throw new SubscriberNotFoundException("Subscription not found. arn=" + subscriptionAttributes.getSubscriptionArn());
			}

			CNSTopicAttributes topicAttributes = CNSTopicAttributesCassandraPersistence.getInstance().getTopicAttributes(subscription.getTopicArn());

			if (topicAttributes != null) {

				CNSTopicDeliveryPolicy topicEffectiveDeliveryPolicy = topicAttributes.getEffectiveDeliveryPolicy();

				if (topicEffectiveDeliveryPolicy != null) {

					if (topicEffectiveDeliveryPolicy.isDisableSubscriptionOverrides() || subscriptionAttributes.getDeliveryPolicy() == null) {
						CNSSubscriptionDeliveryPolicy effectiveDeliveryPolicy = new CNSSubscriptionDeliveryPolicy();
						effectiveDeliveryPolicy.setHealthyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultHealthyRetryPolicy());
						effectiveDeliveryPolicy.setSicklyRetryPolicy(topicEffectiveDeliveryPolicy.getDefaultSicklyRetryPolicy());
						effectiveDeliveryPolicy.setThrottlePolicy(topicEffectiveDeliveryPolicy.getDefaultThrottlePolicy());
						subscriptionAttributes.setEffectiveDeliveryPolicy(effectiveDeliveryPolicy);
					} else {
						subscriptionAttributes.setEffectiveDeliveryPolicy(subscriptionAttributes.getDeliveryPolicy());
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error parsing subscription attributes: " + e.toString());
		}

		return subscriptionAttributes;
	}
}
