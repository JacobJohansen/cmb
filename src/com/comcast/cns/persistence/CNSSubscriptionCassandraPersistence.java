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


import com.comcast.cmb.common.persistence.BaseCassandraDao;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CnsSubscriptionProtocol;
import com.comcast.cns.util.Util;
import com.comcast.cqs.model.CQSQueue;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * 
 * Column-name for CNSTopicSubscriptions table is composite(Endpoint, Protocol). row-key is topicARn
 * @author aseem, bwolf, vvenkatraman, tina, jorge
 * 
 * Class is immutable
 */
public class CNSSubscriptionCassandraPersistence extends BaseCassandraDao<CNSSubscription> implements ICNSSubscriptionPersistence {


	private static Logger logger = Logger.getLogger(CNSSubscriptionCassandraPersistence.class);

	private static final String columnFamilySubscriptions = "cns_topic_subscriptions";
	private static final String columnFamilySubscriptionsIndex = "cns_topic_subscriptions_index";
	private static final String columnFamilySubscriptionsUserIndex = "cns_topic_subscriptions_user_index";
	private static final String columnFamilySubscriptionsTokenIndex = "cns_topic_subscriptions_token_index";
	private static final String columnFamilyTopicStats = "cns_topic_stats";

	private final PreparedStatement upsertCNSTopicSubscriptions;
	private final PreparedStatement upsertCNSTopicSubscriptionsIndex;
	private final PreparedStatement upsertCNSTopicSubscriptionsUserIndex;
	private final PreparedStatement upsertCNSTopicSubscriptionsTokenIndex;

	private final PreparedStatement selectCNSTopicSubscriptions;
	private final PreparedStatement selectCNSTopicSubscriptionsIndex;
	private final PreparedStatement selectCNSTopicSubscriptionsUserIndex;
	private final PreparedStatement selectCNSTopicSubscriptionsTokenIndex;

	private final PreparedStatement deleteCNSTopicSubscriptions;
	private final PreparedStatement deleteAllCNSTopicSubscriptions;
	private final PreparedStatement deleteCNSTopicSubscriptionsIndex;
	private final PreparedStatement deleteCNSTopicSubscriptionsUserIndex;
	private final PreparedStatement deleteCNSTopicSubscriptionsTokenIndex;

	public CNSSubscriptionCassandraPersistence() {
		super(DurablePersistenceFactory.getInstance().getSession());
		upsertCNSTopicSubscriptions = session.prepare(
			QueryBuilder.insertInto("cns", columnFamilySubscriptions)
						.value("topic_arn", bindMarker("topic_arn"))
						.value("endpoint", bindMarker("endpoint"))
						.value("protocol", bindMarker("protocol"))
						.value("subscription_json", bindMarker("subscription_json"))
						.using(ttl(bindMarker()))
		);


		upsertCNSTopicSubscriptionsIndex = session.prepare(
			QueryBuilder.insertInto("cns", columnFamilySubscriptionsIndex)
						.value("subscription_arn", bindMarker("subscription_arn"))
						.value("endpoint", bindMarker("endpoint"))
						.value("topic_arn", bindMarker("topic_arn"))
						.value("protocol", bindMarker("protocol"))
						.using(ttl(bindMarker()))
		);

		upsertCNSTopicSubscriptionsUserIndex = session.prepare(
			QueryBuilder.insertInto("cns", columnFamilySubscriptionsUserIndex)
						.value("user_id", bindMarker("user_id"))
						.value("subscription_arn", bindMarker("subscription_arn"))
						.using(ttl(bindMarker()))
		);

		upsertCNSTopicSubscriptionsTokenIndex = session.prepare(
			QueryBuilder.insertInto("cns", columnFamilySubscriptionsTokenIndex)
						.value("subscription_token", bindMarker("subscription_token"))
						.value("subscription_arn", bindMarker("subscription_arn"))
						.using(ttl(bindMarker()))
		);

		selectCNSTopicSubscriptions = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cns", columnFamilySubscriptions)
				.where(eq("topic_arn", bindMarker("topic_arn")))
				.and(gt("endpoint", bindMarker("last_endpoint")))
		);

		selectCNSTopicSubscriptionsIndex = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cns", columnFamilySubscriptionsIndex)
				.where(eq("subscription_arn", bindMarker("subscription_arn")))
		);

		selectCNSTopicSubscriptionsUserIndex = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cns", columnFamilySubscriptionsUserIndex)
				.where(eq("user_id", bindMarker("user_id")))
				.and(gt("subscription_arn", bindMarker("last_subscription_arn")))
		);

		selectCNSTopicSubscriptionsTokenIndex = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cns", columnFamilySubscriptionsTokenIndex)
				.where(eq("subscription_token", bindMarker("subscription_token")))
		);

		deleteCNSTopicSubscriptions = session.prepare(
			QueryBuilder.delete()
						.from("cns", columnFamilySubscriptions)
						.where(eq("topic_arn", bindMarker("topic_arn")))
						.and(eq("endpoint", bindMarker("endpoint")))
						.and(eq("protocol", bindMarker("protocol")))
		);

		deleteAllCNSTopicSubscriptions = session.prepare(
			QueryBuilder.delete()
						.from("cns", columnFamilySubscriptions)
						.where(eq("topic_arn", bindMarker("topic_arn")))
		);

		deleteCNSTopicSubscriptionsIndex = session.prepare(
			QueryBuilder.delete()
						.from("cns", columnFamilySubscriptionsIndex)
						.where(eq("subscription_arn", bindMarker("subscription_arn")))
		);

		deleteCNSTopicSubscriptionsUserIndex = session.prepare(
			QueryBuilder.delete()
						.from("cns", columnFamilySubscriptionsUserIndex)
						.where(eq("user_id", bindMarker("user_id")))
						.and(eq("subscription_arn", bindMarker("subscription_arn")))
		);

		deleteCNSTopicSubscriptionsTokenIndex = session.prepare(
			QueryBuilder.delete()
						.from("cns", columnFamilySubscriptionsTokenIndex)
						.where(eq("subscription_token", bindMarker("subscription_token")))
						.and(eq("subscription_arn", bindMarker("subscription_arn")))
		);

	}

	private String getColumnValuesJSON(CNSSubscription s) throws JSONException {

		try {
			Writer writer = new StringWriter();
			JSONWriter jw = new JSONWriter(writer);
			jw = jw.object();

			if (s.getEndpoint() != null) {
				jw.key("endPoint").value(s.getEndpoint());
			}

			if (s.getToken() != null) {
				jw.key("subscription_token").value(s.getToken());
			}

			if (s.getArn() != null) {
				jw.key("subArn").value(s.getArn());
			}

			if (s.getUserId() != null) {
				jw.key("user_id").value(s.getUserId());
			}

			if (s.getConfirmDate() != null) {
				jw.key("confirmDate").value(s.getConfirmDate().getTime() + "");
			}

			if (s.getProtocol() != null) {
				jw.key("protocol").value(s.getProtocol().toString());
			}

			if (s.getRequestDate() != null) {
				jw.key("requestDate").value(s.getRequestDate().getTime() + "");
			}

			jw.key("authenticateOnSubscribe").value(s.isAuthenticateOnUnsubscribe() + "");
			jw.key("isConfirmed").value(s.isConfirmed() + "");

			if (s.getRawMessageDelivery() != null) {
				jw.key("rawMessageDelivery").value(s.getRawMessageDelivery() + "");
			} else {
				jw.key("rawMessageDelivery").value(Boolean.toString(false) + "");
			}

			jw.endObject();

			return writer.toString();
		} catch (Exception e) {
			return "";
		}
	}

	private static String getEndpointAndProtoIndexVal(String endpoint, CnsSubscriptionProtocol protocol) {
		return protocol.name() + ":" + endpoint;
	}

	private static String getEndpointAndProtoIndexValEndpoint(String composite) {
		String[] arr = composite.split(":");
		if (arr.length < 2) {
			throw new IllegalArgumentException("Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:" + composite);
		}
		StringBuffer sb = new StringBuffer(arr[1]);
		for (int i = 2; i < arr.length; i++) {
			sb.append(":").append(arr[i]);
		}
		return sb.toString();
	}

	private static CnsSubscriptionProtocol getEndpointAndProtoIndexValProtocol(String composite) {
		String[] arr = composite.split(":");
		if (arr.length < 2) {
			throw new IllegalArgumentException("Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:" + composite);
		}
		return CnsSubscriptionProtocol.valueOf(arr[0]);
	}

	private void insertOrUpdateSubsAndIndexes(final CNSSubscription subscription, Integer ttl) throws Exception {
		subscription.checkIsValid();
		save(Lists.newArrayList(
			saveCNSTopicSubscriptions(subscription, ttl),
			saveCNSTopicSubscriptionsIndex(subscription, ttl),
			saveCNSTopicSubscriptionsUserIndex(subscription, ttl),
			saveCNSTopicSubscriptionsTokenIndex(subscription, ttl)
		));
	}

	private Statement saveCNSTopicSubscriptions(CNSSubscription subscription, Integer ttl) throws JSONException {
		BoundStatement statement = upsertCNSTopicSubscriptions.bind()
															  .setString("topic_arn", subscription.getTopicArn())
															  .setString("endpoint", subscription.getEndpoint())
															  .setString("subscription_json", getColumnValuesJSON(subscription))
															  .setString("protocol", subscription.getProtocol().name());

		if (ttl != null) {
			statement.setInt("[ttl]", ttl);
		}

		return statement;
	}

	private Statement saveCNSTopicSubscriptionsIndex(CNSSubscription subscription, Integer ttl) {
		BoundStatement statement = upsertCNSTopicSubscriptionsIndex.bind()
																   .setString("subscription_arn", subscription.getArn())
																   .setString("topic_arn", subscription.getTopicArn())
																   .setString("endpoint", subscription.getEndpoint())
																   .setString("protocol", subscription.getProtocol().name());
		if (ttl != null) {
			statement.setInt("[ttl]", ttl);
		}
		return statement;
	}

	private Statement saveCNSTopicSubscriptionsUserIndex(CNSSubscription subscription, Integer ttl) {
		BoundStatement statement = upsertCNSTopicSubscriptionsUserIndex.bind()
												   .setString("user_id", subscription.getUserId())
												   .setString("subscription_arn", subscription.getArn());
		if (ttl != null) {
			statement.setInt("[ttl]", ttl);
		}
		return statement;
	}

	private Statement saveCNSTopicSubscriptionsTokenIndex(CNSSubscription subscription, Integer ttl) {
		BoundStatement statement = upsertCNSTopicSubscriptionsTokenIndex.bind()
													.setString("subscription_token", subscription.getToken())
													.setString("subscription_arn", subscription.getArn());
		if (ttl != null) {
			statement.setInt("[ttl]", ttl);
		}
		return statement;
	}

	public CNSSubscription subscribe(String endpoint, CnsSubscriptionProtocol protocol, String topicArn, String userId) throws Exception {

		// subscription is unique by protocol + endpoint + topic

		final CNSSubscription subscription = new CNSSubscription(endpoint, protocol, topicArn, userId);

		CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(topicArn);

		if (t == null) {
			throw new TopicNotFoundException("Resource not found.");
		}

		// check if queue exists for cqs endpoints

		if (protocol.equals(CnsSubscriptionProtocol.cqs)) {

			CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(com.comcast.cqs.util.Util.getRelativeQueueUrlForArn(endpoint));

			if (queue == null) {
				throw new CMBException(CMBErrorCodes.NotFound, "Queue with arn " + endpoint + " does not exist.");
			}
		}

		subscription.setArn(Util.generateCnsTopicSubscriptionArn(topicArn, protocol, endpoint));

		CNSSubscription retrievedSubscription = getSubscription(subscription.getArn());

		if (!CMBProperties.getInstance().getCNSRequireSubscriptionConfirmation()) {

			subscription.setConfirmed(true);
			subscription.setConfirmDate(new Date());

			insertOrUpdateSubsAndIndexes(subscription, 0);

			if (retrievedSubscription == null) {
				PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(subscription.getTopicArn(), "subscriptionConfirmed");
			}

		} else {

			// protocols that cannot confirm subscriptions (e.g. redisPubSub)
			// get an automatic confirmation here
			if (!protocol.canConfirmSubscription()) {
				subscription.setConfirmed(true);
				subscription.setConfirmDate(new Date());
				insertOrUpdateSubsAndIndexes(subscription, 0);

				// auto confirm subscription to cqs queue by owner
			} else if (protocol.equals(CnsSubscriptionProtocol.cqs)) {

				String queueOwner = com.comcast.cqs.util.Util.getQueueOwnerFromArn(endpoint);

				if (queueOwner != null && queueOwner.equals(userId)) {

					subscription.setConfirmed(true);
					subscription.setConfirmDate(new Date());

					insertOrUpdateSubsAndIndexes(subscription, 0);
					if (retrievedSubscription == null) {
						PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(subscription.getTopicArn(), "subscriptionConfirmed");

					}
				} else {

					// use cassandra ttl to implement expiration after 3 days
					insertOrUpdateSubsAndIndexes(subscription, 3 * 24 * 60 * 60);
					if (retrievedSubscription == null) {
						PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(subscription.getTopicArn(), "subscriptionPending");
					}
				}

			} else {

				// use cassandra ttl to implement expiration after 3 days 
				insertOrUpdateSubsAndIndexes(subscription, 3 * 24 * 60 * 60);
				if (retrievedSubscription == null) {
					PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(subscription.getTopicArn(), "subscriptionPending");
				}
			}
		}

		CNSSubscriptionAttributes attributes = new CNSSubscriptionAttributes(topicArn, subscription.getArn(), userId);
		PersistenceFactory.getCNSSubscriptionAttributePersistence().setSubscriptionAttributes(attributes, subscription.getArn());

		return subscription;
	}

	public CNSSubscription getSubscription(String arn) throws Exception {
		CNSSubscription subscription = findOne(selectCNSTopicSubscriptionsIndex.bind().setString("subscription_arn", arn));

		if (subscription != null) {
			return findOne(selectCNSTopicSubscriptions.bind().setString("topic_arn", subscription.getTopicArn()).setString("last_endpoint", ""));
		}

		return null;
	}

	private static CNSSubscription extractSubscriptionFromJson(String jsonSubscription, String topicArn) throws JSONException {

		JSONObject json = new JSONObject(jsonSubscription);
		CNSSubscription s = new CNSSubscription(json.getString("subArn"));

		s.setEndpoint(json.getString("endPoint"));
		s.setUserId(json.getString("user_id"));

		if (json.has("confirmDate")) {
			s.setConfirmDate(new Date(json.getLong("confirmDate")));
		}

		if (json.has("requestDate")) {
			s.setRequestDate(new Date(json.getLong("requestDate")));
		}

		if (json.has("protocol")) {
			s.setProtocol(CnsSubscriptionProtocol.valueOf(json.getString("protocol")));
		}

		if (json.has("isConfirmed")) {
			s.setConfirmed(json.getBoolean("isConfirmed"));
		}

		s.setToken(json.getString("subscription_token"));

		if (json.has("authenticateOnSubscribe")) {
			s.setAuthenticateOnUnsubscribe(json.getBoolean("authenticateOnSubscribe"));
		}

		if (json.has("rawMessageDelivery")) {
			s.setRawMessageDelivery(json.getBoolean("rawMessageDelivery"));
		}

		s.setTopicArn(topicArn);

		return s;
	}

	/**
	 * List all subscription for a user, unconfirmed subscriptions will not reveal their arns. Pagination for more
	 * than 100 subscriptions.
	 *
	 * @param nextToken initially null, on subsequent calls arn of last result from prior call
	 * @param protocol  optional filter by protocol (this parameter is not part of official AWS API)
	 * @return list of subscriptions. If nextToken is not null, the subscription corresponding to it is not returned.
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
		return listSubscriptions(nextToken, protocol, userId, true);
	}

	private List<CNSSubscription> listSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId, boolean hidePendingArn) throws Exception {
		//Algorithm is to keep reading in chunks of 100 till we've seen 500 from the nextToken-ARN
		List<CNSSubscription> l = new ArrayList<CNSSubscription>();
		List<CNSSubscription> userSubscriptionArnIndex;
		//read form index to get sub-arn
		userSubscriptionArnIndex = find(selectCNSTopicSubscriptionsUserIndex.bind().setString("user_id", userId).setString("last_subscription_arn", nextToken == null? "" : nextToken), null, 100);

		if (userSubscriptionArnIndex == null || userSubscriptionArnIndex.isEmpty()) {
			return l;
		}

		for (CNSSubscription userIndexSubscription : userSubscriptionArnIndex) {

			CNSSubscription subscription = getSubscription(userIndexSubscription.getArn());

			if (subscription == null) {
				throw new IllegalStateException("Subscriptions-user-index contains subscription-arn which doesn't exist in subscriptions-index. subArn:");
			}

			// ignore invalid subscriptions coming from Cassandra

			try {
				subscription.checkIsValid();
			} catch (CMBException ex) {
				logger.error("event=invalid_subscription " + subscription.toString(), ex);
				continue;
			}

			if (protocol != null && subscription.getProtocol() != protocol) {
				continue;
			}

			if (hidePendingArn && !subscription.isConfirmed()) {
				subscription.setArn("PendingConfirmation");
			}

			l.add(subscription);
		}

		return l;
	}

	public List<CNSSubscription> listAllSubscriptions(String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
		return listSubscriptions(nextToken, protocol, userId, false);
	}

	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
		return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100);
	}

	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize) throws Exception {
		return listSubscriptionsByTopic(nextToken, topicArn, protocol, pageSize, true);
	}

	/**
	 * Enumerate all subs in a topic
	 *
	 * @param nextToken      The ARN of the last sub-returned or null is first time call.
	 * @param topicArn
	 * @param protocol
	 * @param pageSize
	 * @param hidePendingArn
	 * @return The list of subscriptions given a topic. Note: if nextToken is provided, the returned list will not
	 * contain it for convenience
	 * @throws Exception
	 */
	public List<CNSSubscription> listSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize, boolean hidePendingArn) throws Exception {
		//read from index to get composite-col-name corresponding to nextToken
		List<CNSSubscription> l = new ArrayList<CNSSubscription>();
		CNSTopic topic = PersistenceFactory.getTopicPersistence().getTopic(topicArn);
		if (topic == null) {
			throw new TopicNotFoundException("Resource not found.");
		}


		List<CNSSubscription> subscriptions = find(selectCNSTopicSubscriptions.bind().setString("topic_arn", topicArn).setString("last_endpoint", nextToken == null ? "" : nextToken), null, pageSize);


		if (subscriptions == null || subscriptions.size() == 0) {
			return l;
		}

		for (CNSSubscription sub : subscriptions) {
			try {
				sub.checkIsValid();
			} catch (CMBException ex) {
				logger.error("event=invalid_subscription " + sub.toString(), ex);
				continue;
			}

			if (protocol != null && protocol != sub.getProtocol()) {
				continue;
			}

			if (hidePendingArn && !sub.isConfirmed()) {
				sub.setArn("PendingConfirmation");
			}
			l.add(sub);
		}

		return l;
	}

	public List<CNSSubscription> listAllSubscriptionsByTopic(String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
		return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100, false);
	}

	public CNSSubscription confirmSubscription(boolean authenticateOnUnsubscribe, String token, String topicArn) throws Exception {

		//get Sub-arn given token
		CNSSubscription subscriptionByToken = findOne(selectCNSTopicSubscriptionsTokenIndex.bind().setString("subscription_token", token));

		if (subscriptionByToken == null) {
			throw new CMBException(CMBErrorCodes.NotFound, "Resource not found.");
		}
		String subArn = subscriptionByToken.getArn();

		//get Subscription given subArn
		final CNSSubscription s = getSubscription(subArn);

		if (s == null) {
			throw new SubscriberNotFoundException("Could not find subscription given subscription arn " + subArn);
		}

		s.setAuthenticateOnUnsubscribe(authenticateOnUnsubscribe);
		s.setConfirmed(true);
		s.setConfirmDate(new Date());

		//re-insert with no TTL. will clobber the old one which had ttl
		insertOrUpdateSubsAndIndexes(s, 0);

		PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(s.getTopicArn(), "subscriptionPending");
		PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(s.getTopicArn(), "subscriptionConfirmed");

		return s;
	}

	private void deleteIndexesAll(List<CNSSubscription> subscriptionList) throws PersistenceException {
		List<Statement> statements = Lists.newArrayList();
		for (CNSSubscription sub : subscriptionList) {
			statements.add(deleteCNSTopicSubscriptionsIndex.bind().setString("subscription_arn", sub.getArn()));
			if (sub.getToken() != null && !sub.getToken().isEmpty()) {
				statements.add(deleteCNSTopicSubscriptionsTokenIndex.bind().setString("subscription_token", sub.getToken()).setString("subscription_arn", sub.getArn()));
			}
			statements.add(deleteCNSTopicSubscriptionsUserIndex.bind().setString("user_id", sub.getUserId()).setString("subscription_arn", sub.getArn()));
		}
		delete(statements);
	}

	private void deleteIndexes(String subArn, String userId, String token) throws PersistenceException {
		List<Statement> statements = Lists.newArrayList();
		statements.add(deleteCNSTopicSubscriptionsIndex.bind().setString("subscription_arn", subArn));
		if (token != null && !token.isEmpty()) {
			statements.add(deleteCNSTopicSubscriptionsTokenIndex.bind().setString("subscription_token", token).setString("subscription_arn", subArn));
		}
		statements.add(deleteCNSTopicSubscriptionsUserIndex.bind().setString("user_id", userId).setString("subscription_arn", subArn));
		delete(statements);
	}

	public void unsubscribe(String arn) throws Exception {

		CNSSubscription s = getSubscription(arn);

		if (s != null) {

			deleteIndexes(arn, s.getUserId(), s.getToken());
			save(deleteCNSTopicSubscriptions.bind().setString("topic_arn", s.getArn()).setString("protocol", s.getProtocol().name()).setString("endpoint", s.getEndpoint()));

			if (s.isConfirmed()) {
				PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(s.getTopicArn(), "subscriptionConfirmed");
			} else {
				PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(s.getTopicArn(), "subscriptionPending");
			}
			PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(s.getTopicArn(), "subscriptionDeleted");
		}
	}

	public long getCountSubscription(String topicArn, String status) throws Exception {
		return PersistenceFactory.getCNSTopicAttributePersistence().getTopicStats(topicArn, status);
	}

	public void unsubscribeAll(String topicArn) throws Exception {

		int pageSize = 1000;

		String nextToken = null;
		List<CNSSubscription> subs = listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);

		// for pagination to work we need the nextToken's corresponding sub to not be deleted.

		CNSSubscription nextTokenSub;

		while (subs.size() > 0) {
			if (subs.size() < pageSize) {
				deleteIndexesAll(subs);
				PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(topicArn, "subscriptionDeleted", subs.size());
				break;
			} else {
				//keep the last subscription for pagination purpose.
				nextTokenSub = subs.get(subs.size() - 1);
				nextToken = nextTokenSub.getArn();
				subs.remove(subs.size() - 1);
				deleteIndexesAll(subs);
				subs = listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);
				deleteIndexes(nextTokenSub.getArn(), nextTokenSub.getUserId(), nextTokenSub.getToken());
				PersistenceFactory.getCNSTopicAttributePersistence().incrementCounter(topicArn, "subscriptionDeleted", subs.size());
			}
		}

		long subscriptionConfirmed = getCountSubscription(topicArn, "subscriptionConfirmed");

		if (subscriptionConfirmed > 0) {
			PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(topicArn, "subscriptionConfirmed", (int) subscriptionConfirmed);
		}

		long subscriptionPending = getCountSubscription(topicArn, "subscriptionPending");

		if (subscriptionPending > 0) {
			PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(topicArn, "subscriptionPending", (int) subscriptionPending);
		}

		save(deleteAllCNSTopicSubscriptions.bind().setString("topic_arn", topicArn));
	}

	public void setRawMessageDelivery(String subscriptionArn, boolean rawMessageDelivery) throws Exception {
		CNSSubscription sub;
		sub = getSubscription(subscriptionArn);
		if (sub != null) {
			sub.setRawMessageDelivery(rawMessageDelivery);
			insertOrUpdateSubsAndIndexes(sub, 0);
		}
	}

	@Override
	protected CNSSubscription convertToInstance(Row row) {
		String topicArn = null;
		String endpoint = null;
		String protocol = null;
		String subscriptionArn = null;
		String token = null;
		String subscriptionJson = null;
		String userId = null;
		CNSSubscription subscription = null;

		if (row.getColumnDefinitions().contains("topic_arn")) {
			topicArn = row.getString("topic_arn");
		}

		if (row.getColumnDefinitions().contains("endpoint")) {
			endpoint = row.getString("endpoint");
		}

		if (row.getColumnDefinitions().contains("protocol")) {
			protocol = row.getString("protocol");
		}

		if (row.getColumnDefinitions().contains("subscription_json")) {
			subscriptionJson = row.getString("subscription_json");
		}

		if (row.getColumnDefinitions().contains("subscription_arn")) {
			subscriptionArn = row.getString("subscription_arn");
		}

		if (row.getColumnDefinitions().contains("subscription_token")) {
			token = row.getString("subscription_token");
		}

		if (row.getColumnDefinitions().contains("user_id")) {
			userId = row.getString("user_id");
		}


		if (subscriptionJson != null && !subscriptionJson.isEmpty()) {
			try {
				subscription = extractSubscriptionFromJson(subscriptionJson, topicArn);
			} catch (Exception e) {
				logger.error("failed to parse subscription: " + e.toString());
			}
		}

		if (subscription == null && protocol != null && !protocol.isEmpty()) {
			subscription = new CNSSubscription(endpoint, CnsSubscriptionProtocol.valueOf(protocol),topicArn, userId);
		} else if (subscription == null) {
			subscription = new CNSSubscription(subscriptionArn);
		}

		if (token != null) {
			subscription.setToken(token);
		}

		if (subscriptionArn != null) {
			subscription.setArn(subscriptionArn);
		}

		return subscription;
	}
}