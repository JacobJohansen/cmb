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
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;
import com.comcast.cqs.util.CQSErrorCodes;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Provide Cassandra persistence for topics
 * @author aseem, bwolf, jorge, vvenkatraman, tina, michael
 *
 * Class is immutable
 */
public class CNSTopicCassandraPersistence extends BaseCassandraDao<CNSTopic> implements ICNSTopicPersistence {

	private static Logger logger = Logger.getLogger(CNSTopicCassandraPersistence.class);

	private static final String columnFamilyTopics = "CNSTopics";
	private static final String columnFamilyTopicsByUserId = "CNSTopicsByUserId";


	PreparedStatement insertCNSTopics;
	PreparedStatement insertCNSTopicsByUserId;

	PreparedStatement selectCNSTopics;
	PreparedStatement selectCNSTopicsByUserId;

	PreparedStatement deleteCNSTopics;
	PreparedStatement deleteCNSTopicsByUserId;

	public CNSTopicCassandraPersistence() {
		super(DurablePersistenceFactory.getInstance().getSession());

		insertCNSTopics = session.prepare(
			QueryBuilder.insertInto("CNS", columnFamilyTopics)
				.value("topicArn", bindMarker("topicArn"))
				.value("displayName", bindMarker("displayName"))
				.value("name", bindMarker("name"))
				.value("userId", bindMarker("userId"))
		);

		insertCNSTopicsByUserId = session.prepare(
			QueryBuilder.insertInto("CNS", columnFamilyTopicsByUserId)
						.value("topicArn", bindMarker("topicArn"))
						.value("userId", bindMarker("userId"))
		);


		selectCNSTopics = session.prepare(
			QueryBuilder.select().all().from("CNS", columnFamilyTopics)
				.where(eq("topicArn", bindMarker("topicArn")))
		);

		selectCNSTopicsByUserId = session.prepare(
			QueryBuilder.select().all().from("CNS", columnFamilyTopicsByUserId)
				.where(eq("userId", bindMarker("userId")))
		);

		deleteCNSTopics = session.prepare(
			QueryBuilder.delete().from("CNS", columnFamilyTopics)
						.where(eq("topicArn", bindMarker("topicArn")))
		);

		deleteCNSTopicsByUserId = session.prepare(
			QueryBuilder.delete().from("CNS", columnFamilyTopicsByUserId)
						.where(eq("userId", bindMarker("userId")))
		);

	}

	public CNSTopic createTopic(String name, String displayName, String userId) throws Exception {

		String arn = Util.generateCnsTopicArn(name, CMBProperties.getInstance().getRegion(), userId);

		CNSTopic topic = getTopic(arn);

		if (topic != null) {
			return topic;
		}

		topic = new CNSTopic(arn, name, displayName, userId);
		topic.checkIsValid();

		save(Lists.newArrayList(
			insertCNSTopics.bind().setString("userId", topic.getUserId()).setString("topicArn", topic.getArn()).setString("displayName", topic.getDisplayName()).setString("name", topic.getName()),
			insertCNSTopicsByUserId.bind().setString("userId", topic.getUserId()).setString("topicArn", topic.getArn())
		));

		// note: deleteing rows or columns makes them permanently unavailable as counters!
		// http://stackoverflow.com/questions/13653681/apache-cassandra-delete-from-counter

		long subscriptionConfirmed = PersistenceFactory.getCNSTopicAttributePersistence().getTopicStats(arn, "subscriptionConfirmed");
		if (subscriptionConfirmed > 0) {
			PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(arn, "subscriptionConfirmed", (int)subscriptionConfirmed);
		}

		long subscriptionPending = PersistenceFactory.getCNSTopicAttributePersistence().getTopicStats( arn, "subscriptionPending");
		if (subscriptionPending > 0) {
			PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(arn, "subscriptionPending", (int)subscriptionPending);
		}

		long subscriptionDeleted = PersistenceFactory.getCNSTopicAttributePersistence().getTopicStats(arn, "subscriptionDeleted");
		if (subscriptionDeleted > 0) {
			PersistenceFactory.getCNSTopicAttributePersistence().decrementCounter(arn, "subscriptionDeleted", (int)subscriptionDeleted);
		}

		CNSTopicAttributes attributes = new CNSTopicAttributes(arn, userId);
		PersistenceFactory.getCNSTopicAttributePersistence().setTopicAttributes(attributes, arn);

		return topic;
	}

	public void deleteTopic(String arn) throws Exception {

		CNSTopic topic = getTopic(arn);

		if (topic == null) {
			throw new CMBException(CNSErrorCodes.CNS_NotFound, "Topic not found.");
		}

		// delete all subscriptions first
		PersistenceFactory.getSubscriptionPersistence().unsubscribeAll(topic.getArn());		
		save(Lists.newArrayList(
			deleteCNSTopics.bind().setString("topicArn", arn),
			deleteCNSTopicsByUserId.bind().setString("userId", topic.getUserId())
		));
		PersistenceFactory.getCNSTopicAttributePersistence().removeTopicAttributes(arn);
		
		CNSCache.removeTopic(arn);
	}

	public long getNumberOfTopicsByUser(String userId) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		return findAll(selectCNSTopicsByUserId.bind().setString("userId", userId)).size();
	}

	public List<CNSTopic> listTopics(String userId, String nextToken) throws Exception {


		List<CNSTopic> topics = new ArrayList<CNSTopic>();
		List<CNSTopic> userTopics = find(selectCNSTopicsByUserId.bind().setString("userId", userId), nextToken, 100);

		if (userTopics == null) {
			return topics;
		}
			
		for (CNSTopic userTopic : userTopics) {
			CNSTopic topic = getTopic(userTopic.getArn());
			if (topic!= null) {
				topic.setNextPage(userTopic.getNextPage());
				topics.add(topic);

			} else {
				save(deleteCNSTopicsByUserId.bind().setString("userId", userTopic.getUserId()).setString("topicArn", userTopic.getArn()));
			}
		}

		return topics;
	}

	public CNSTopic getTopic(String arn) throws Exception {
		return findOne(selectCNSTopics.bind().setString("topicArn", arn));
	}

	public void updateTopicDisplayName(String arn, String displayName) throws Exception {

		CNSTopic topic = getTopic(arn);

		if (topic != null) {
			topic.setDisplayName(displayName);
			topic.checkIsValid();
			save(insertCNSTopics.bind().setString("topicArn", arn).setString("displayName", topic.getDisplayName()));
		}
		
		CNSCache.removeTopic(arn);
	}

	@Override
	protected CNSTopic convertToInstance(Row row) {
		String topicArn = null;
		String displayName = null;
		String name = null;
		String userId = null;

		if (row.getColumnDefinitions().contains("topicArn")) {
			topicArn = row.getString("topicArn");
		}
		if (row.getColumnDefinitions().contains("displayName")) {
			displayName = row.getString("displayName");
		}
		if (row.getColumnDefinitions().contains("name")) {
			name = row.getString("name");
		}
		if (row.getColumnDefinitions().contains("userId")) {
			userId = row.getString("userId");
		}

		return new CNSTopic(topicArn, name, displayName, userId);
	}
}
