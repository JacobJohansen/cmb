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
package com.comcast.cqs.persistence;

import com.comcast.cmb.common.persistence.BaseCassandraDao;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Cassandra persistence for queues
 * @author aseem, jorge, bwolf, baosen, vvenkatraman
 *
 */
public class CQSQueueCassandraPersistence extends BaseCassandraDao<CQSQueue> implements ICQSQueuePersistence {
    
	private static final String COLUMN_FAMILY_QUEUES = "CQSQueues";
	private static final String COLUMN_FAMILY_QUEUES_BY_USER = "CQSQueuesByUserId";

	public static final Logger logger = Logger.getLogger(CQSQueueCassandraPersistence.class);

	private final PreparedStatement insertCQSQueues;
	private final PreparedStatement insertCQSQueuesByUserId;

	private final PreparedStatement selectCQSQueues;
	private final PreparedStatement selectCQSQueuesByUserId;

	private final PreparedStatement deleteCQSQueues;
	private final PreparedStatement deleteCQSQueuesByUserId;

	public CQSQueueCassandraPersistence() {
		super(DurablePersistenceFactory.getInstance().getSession());

		insertCQSQueues = session.prepare(
			QueryBuilder.insertInto("CQS", COLUMN_FAMILY_QUEUES)
				.value("relativeUrl", bindMarker("relativeUrl"))
				.value("queueArn", bindMarker("queueArn"))
				.value("name", bindMarker("name"))
				.value("ownerUserId", bindMarker("ownerUserId"))
				.value("region", bindMarker("region"))
				.value("serviceEndpoint", bindMarker("serviceEndpoint"))
				.value("visibilityTO", bindMarker("visibilityTO"))
				.value("maxMsgSize", bindMarker("maxMsgSize"))
				.value("msgRetentionPeriod", bindMarker("msgRetentionPeriod"))
				.value("delaySeconds", bindMarker("delaySeconds"))
				.value("policy", bindMarker("policy"))
				.value("createdTime", bindMarker("createdTime"))
				.value("waitTime", bindMarker("waitTime"))
				.value("numberOfPartitions", bindMarker("numberOfPartitions"))
				.value("numberOfShards", bindMarker("numberOfShards"))
				.value("compressed", bindMarker("compressed"))
				.value("redrivePolicy", bindMarker("redrivePolicy"))
		);

		insertCQSQueuesByUserId = session.prepare(
			QueryBuilder.insertInto("CQS", COLUMN_FAMILY_QUEUES_BY_USER)
				.value("userId", bindMarker("userId"))
				.value("queueArn", bindMarker("queueArn"))
		);

		selectCQSQueues = session.prepare(
			QueryBuilder.select()
				.all()
				.from("CQS", COLUMN_FAMILY_QUEUES)
				.where(eq("relativeUrl", bindMarker("relativeUrl")))
		);
		selectCQSQueuesByUserId = session.prepare(
			QueryBuilder.select()
				.all()
				.from("CQS", COLUMN_FAMILY_QUEUES_BY_USER)
				.where(eq("userId", bindMarker("userId")))
				.and(eq("queueArn", bindMarker("queueArn")))
		);

		deleteCQSQueues = session.prepare(
			QueryBuilder.delete()
						.from("CQS", COLUMN_FAMILY_QUEUES)
						.where(eq("relativeUrl", bindMarker("relativeUrl")))
		);
		deleteCQSQueuesByUserId = session.prepare(
			QueryBuilder.delete()
						.from("CQS", COLUMN_FAMILY_QUEUES_BY_USER)
						.where(eq("userId", bindMarker("userId")))
						.and(eq("queueArn", bindMarker("queueArn")))
		);

	}

	@Override
	public void createQueue(CQSQueue queue) throws PersistenceException {
		long createdTime = Calendar.getInstance().getTimeInMillis();
		queue.setCreatedTime(createdTime);

		save(Lists.newArrayList(
			insertCQSQueues.bind()
						   .setString("relativeUrl", queue.getRelativeUrl())
						   .setString("queueArn", queue.getArn())
						   .setString("name", queue.getName())
						   .setString("userId", queue.getOwnerUserId())
						   .setString("region", queue.getRegion())
						   .setString("serviceEndpoint", queue.getServiceEndpoint())
						   .setString("visibilityTO", Integer.toString(queue.getVisibilityTO()))
						   .setString("maxMsgSize", Integer.toString(queue.getMaxMsgSize()))
						   .setString("msgRetentionPeriod", Integer.toString(queue.getMsgRetentionPeriod()))
						   .setString("delaySeconds", Integer.toString(queue.getDelaySeconds()))
						   .setString("policy", queue.getPolicy())
						   .setString("createdTime", Long.toString(queue.getCreatedTime()))
						   .setString("waitTime", Integer.toString(queue.getReceiveMessageWaitTimeSeconds()))
						   .setString("numberOfPartitions", Integer.toString(queue.getNumberOfPartitions()))
						   .setString("numberOfShards", Integer.toString(queue.getNumberOfShards()))
						   .setString("compressed", Boolean.toString(queue.isCompressed()))
						   .setString("redrivePolicy", queue.getRedrivePolicy()),

			insertCQSQueuesByUserId.bind()
								   .setString("userId", queue.getOwnerUserId())
								   .setString("queueArn", queue.getArn())
								   .setString("createdTime", Long.toString(queue.getCreatedTime()))
		));
	}
	
	@Override
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData) throws PersistenceException {
		BoundStatement updateQueueAttributes = insertCQSQueues.bind();
		updateQueueAttributes.setString("relativeUrl", queueURL);

		if(queueData.containsKey("visibilityTO")) {
			updateQueueAttributes.setString("visibilityTO", queueData.get("visibilityTO"));
		}
		if(queueData.containsKey("policy")) {
			updateQueueAttributes.setString("policy", queueData.get("policy"));
		}
		if(queueData.containsKey("maxMsgSize")) {
			updateQueueAttributes.setString("maxMsgSize", queueData.get("maxMsgSize"));
		}
		if(queueData.containsKey("msgRetentionPeriod")) {
			updateQueueAttributes.setString("msgRetentionPeriod", queueData.get("msgRetentionPeriod"));
		}
		if(queueData.containsKey("delaySeconds")) {
			updateQueueAttributes.setString("delaySeconds", queueData.get("delaySeconds"));
		}
		if(queueData.containsKey("waitTime")) {
			updateQueueAttributes.setString("waitTime", queueData.get("waitTime"));
		}
		if(queueData.containsKey("numberOfPartitions")) {
			updateQueueAttributes.setString("numberOfPartitions", queueData.get("numberOfPartitions"));
		}
		if(queueData.containsKey("numberOfShards")) {
			updateQueueAttributes.setString("numberOfShards", queueData.get("numberOfShards"));
		}
		if(queueData.containsKey("compressed")) {
			updateQueueAttributes.setString("compressed", queueData.get("compressed"));
		}
		if(queueData.containsKey("redrivePolicy")) {
			updateQueueAttributes.setString("redrivePolicy", queueData.get("redrivePolicy"));
		}
		save(updateQueueAttributes);
	}

	@Override
	public void deleteQueue(String queueUrl) throws PersistenceException {
		if (getQueueByUrl(queueUrl) == null) {
			logger.error("event=delete_queue error_code=queue_does_not_exist queue_url=" + queueUrl);
			throw new PersistenceException (CQSErrorCodes.InvalidRequest, "No queue with the url " + queueUrl + " exists");
		}

		save(Lists.newArrayList(
			deleteCQSQueues.bind().setString("relativeUrl", queueUrl),
			deleteCQSQueuesByUserId.bind().setString("userId", Util.getUserIdForRelativeQueueUrl(queueUrl)).setString("queueArn", Util.getArnForRelativeQueueUrl(queueUrl))
		));
	}

	@Override
	public List<CQSQueue> listQueues(String userId, String queueNamePrefix, boolean containingMessagesOnly) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		List<CQSQueue> queueList = findAll(selectCQSQueuesByUserId.bind().setString("userId", userId));

		Iterator<CQSQueue> queueIterator = queueList.iterator();

		while (queueIterator.hasNext()) {
			CQSQueue queue = queueIterator.next();

			if (queueNamePrefix != null && !queue.getName().startsWith(queueNamePrefix)) {
				queueIterator.remove();
			}

			if (containingMessagesOnly) {
				try {
					if (PersistenceFactory.getCQSMessagePersistence().getQueueMessageCount(queue.getRelativeUrl(), true) <= 0) {
						queueIterator.remove();
					}
				} catch (Exception ex) {
					queueIterator.remove();
				}
			}
		}
		
		return queueList;
	}
	
	@Override
	public long getNumberOfQueuesByUser(String userId) throws PersistenceException {
		return findAll(selectCQSQueuesByUserId.bind().setString("userId", userId)).size();
	}

	private CQSQueue getQueueByUrl(String queueUrl) throws PersistenceException {
		return findOne(selectCQSQueues.bind().setString("relativeUrl", queueUrl));
	}

	@Override
	public CQSQueue getQueue(String userId, String queueName) throws PersistenceException {
		CQSQueue queue = new CQSQueue(queueName, userId);
		return getQueueByUrl(queue.getRelativeUrl());
	}

	@Override
	public CQSQueue getQueue(String queueUrl) throws PersistenceException {
		return getQueueByUrl(queueUrl);
	}

	@Override
	public boolean updatePolicy(String queueUrl, String policy) throws PersistenceException {
		if (queueUrl == null || queueUrl.trim().isEmpty() || policy == null || policy.trim().isEmpty()) {
			return false;
		}

		save(insertCQSQueues.bind()
							.setString("relativeUrl", queueUrl)
							.setString("policy", policy)
		);

		return true;
	}

	@Override
	protected CQSQueue convertToInstance(Row row) {
		try {
			String arn = "";
			if (row.getColumnDefinitions().contains("queueArn")) {
				arn = row.getString("queueArn");
			}

			String relativeUrl;
			if (row.getColumnDefinitions().contains("relativeUrl")) {
				relativeUrl = row.getString("relativeUrl");
			} else {
				relativeUrl = Util.getRelativeQueueUrlForArn(arn);
			}

			String name;
			if (row.getColumnDefinitions().contains("name")) {
				name = row.getString("name");
			} else {
				name = Util.getNameForArn(arn);
			}

			String ownerUserId = "";
			if (row.getColumnDefinitions().contains("userId")) {
				ownerUserId = row.getString("userId");
			}

			String region = "";
			if (row.getColumnDefinitions().contains("region")) {
				region = row.getString("region");
			}

			int visibilityTO = 0;
			if (row.getColumnDefinitions().contains("visibilityTO")) {
				visibilityTO = (new Long(row.getString("visibilityTO"))).intValue();
			}

			int maxMsgSize = 0;
			if (row.getColumnDefinitions().contains("maxMsgSize")) {
				maxMsgSize = (new Long(row.getString("maxMsgSize"))).intValue();
			}


			int msgRetentionPeriod = 0;
			if (row.getColumnDefinitions().contains("msgRetentionPeriod")) {
				msgRetentionPeriod = (new Long(row.getString("msgRetentionPeriod"))).intValue();
			}

			int delaySeconds = 0;
			if (row.getColumnDefinitions().contains("delaySecond")) {
				delaySeconds = row.isNull("delaySecond") ? 0 : (new Long(row.getString("delaySecond"))).intValue();
			}


			int waitTimeSeconds = 0;
			if (row.getColumnDefinitions().contains("waitTime")) {
				waitTimeSeconds = row.isNull("waitTime") ? 0 : (new Long(row.getString("waitTime"))).intValue();
			}

			int numPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
			if (row.getColumnDefinitions().contains("numberOfPartitions")) {
				numPartitions = row.isNull("numberOfPartitions") ? CMBProperties.getInstance().getCQSNumberOfQueuePartitions() : (new Long(row.getString("numberOfPartitions"))).intValue();
			}

			int numShards =  1;
			if (row.getColumnDefinitions().contains("numberOfShards")) {
				numShards = row.isNull("numberOfShards") ? 1 : (new Long(row.getString("numberOfShards"))).intValue();
			}

			String policy = "";
			if (row.getColumnDefinitions().contains("policy")) {
				policy = row.getString("policy");
			}

			long createdTime = 0;
			if (row.getColumnDefinitions().contains("createdTime")) {
				createdTime = (new Long(row.getString("createdTime")));
			}

			String hostName = "";
			if (row.getColumnDefinitions().contains("serviceEndpoint")) {
				hostName = row.getString("serviceEndpoint");
			}

			boolean isCompressed = false;
			if (row.getColumnDefinitions().contains("serviceEndpoint")) {
				isCompressed = !row.isNull("compressed") && Boolean.getBoolean(row.getString("compressed"));
			}

			String redrivePolicy = "";
			if (row.getColumnDefinitions().contains("redrivePolicy")) {
				hostName = row.isNull("redrivePolicy")? "" : row.getString("redrivePolicy");
			}

			CQSQueue queue = new CQSQueue(name, ownerUserId);
			queue.setRelativeUrl(relativeUrl);
			queue.setServiceEndpoint(hostName);
			queue.setArn(arn);
			queue.setRegion(region);
			queue.setPolicy(policy);
			queue.setVisibilityTO(visibilityTO);
			queue.setMaxMsgSize(maxMsgSize);
			queue.setMsgRetentionPeriod(msgRetentionPeriod);
			queue.setDelaySeconds(delaySeconds);
			queue.setReceiveMessageWaitTimeSeconds(waitTimeSeconds);
			queue.setNumberOfPartitions(numPartitions);
			queue.setNumberOfShards(numShards);
			queue.setCreatedTime(createdTime);
			queue.setCompressed(isCompressed);
			queue.setRedrivePolicy(redrivePolicy);

			return queue;
		} catch (Exception ex) {}

		return null;
	}
}
