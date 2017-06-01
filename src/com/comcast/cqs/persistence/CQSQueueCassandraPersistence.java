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
    
	private static final String COLUMN_FAMILY_QUEUES = "cqs_queues";
	private static final String COLUMN_FAMILY_QUEUES_BY_USER = "cqs_queues_by_user_id";

	public static final Logger logger = Logger.getLogger(CQSQueueCassandraPersistence.class);

	private final PreparedStatement insertCQSQueues;
	private final PreparedStatement insertCQSQueuesByUserId;

	private final PreparedStatement selectCQSQueues;
	private final PreparedStatement selectCQSQueuesByUserIdAndArn;
	private final PreparedStatement selectCQSQueuesByUserId;

	private final PreparedStatement deleteCQSQueues;
	private final PreparedStatement deleteCQSQueuesByUserId;

	public CQSQueueCassandraPersistence() {
		super(DurablePersistenceFactory.getInstance().getSession());

		insertCQSQueues = session.prepare(
			QueryBuilder.insertInto("cqs", COLUMN_FAMILY_QUEUES)
				.value("relative_url", bindMarker("relative_url"))
				.value("queue_arn", bindMarker("queue_arn"))
				.value("name", bindMarker("name"))
				.value("user_id", bindMarker("user_id"))
				.value("region", bindMarker("region"))
				.value("service_endpoint", bindMarker("service_endpoint"))
				.value("visibility_timeout", bindMarker("visibility_timeout"))
				.value("max_message_size", bindMarker("max_message_size"))
				.value("message_retention_period", bindMarker("message_retention_period"))
				.value("delay_seconds", bindMarker("delay_seconds"))
				.value("policy", bindMarker("policy"))
				.value("created_time", bindMarker("created_time"))
				.value("wait_time", bindMarker("wait_time"))
				.value("number_of_partitions", bindMarker("number_of_partitions"))
				.value("number_of_shards", bindMarker("number_of_shards"))
				.value("compressed", bindMarker("compressed"))
				.value("redrive_policy", bindMarker("redrive_policy"))
		);

		insertCQSQueuesByUserId = session.prepare(
			QueryBuilder.insertInto("cqs", COLUMN_FAMILY_QUEUES_BY_USER)
				.value("user_id", bindMarker("user_id"))
				.value("queue_arn", bindMarker("queue_arn"))
				.value("created_time", bindMarker("created_time"))
		);

		selectCQSQueues = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cqs", COLUMN_FAMILY_QUEUES)
				.where(eq("relative_url", bindMarker("relative_url")))
		);
		selectCQSQueuesByUserIdAndArn = session.prepare(
			QueryBuilder.select()
				.all()
				.from("cqs", COLUMN_FAMILY_QUEUES_BY_USER)
				.where(eq("user_id", bindMarker("user_id")))
				.and(eq("queue_arn", bindMarker("queue_arn")))
		);

		selectCQSQueuesByUserId = session.prepare(
			QueryBuilder.select()
						.all()
						.from("cqs", COLUMN_FAMILY_QUEUES_BY_USER)
						.where(eq("user_id", bindMarker("user_id")))
		);

		deleteCQSQueues = session.prepare(
			QueryBuilder.delete()
						.from("cqs", COLUMN_FAMILY_QUEUES)
						.where(eq("relative_url", bindMarker("relative_url")))
		);
		deleteCQSQueuesByUserId = session.prepare(
			QueryBuilder.delete()
						.from("cqs", COLUMN_FAMILY_QUEUES_BY_USER)
						.where(eq("user_id", bindMarker("user_id")))
						.and(eq("queue_arn", bindMarker("queue_arn")))
		);

	}

	@Override
	public void createQueue(CQSQueue queue) throws PersistenceException {
		long createdTime = Calendar.getInstance().getTimeInMillis();
		queue.setCreatedTime(createdTime);

		save(Lists.newArrayList(
			insertCQSQueues.bind()
						   .setString("relative_url", queue.getRelativeUrl())
						   .setString("queue_arn", queue.getArn())
						   .setString("name", queue.getName())
						   .setString("user_id", queue.getOwnerUserId())
						   .setString("region", queue.getRegion())
						   .setString("service_endpoint", queue.getServiceEndpoint())
						   .setString("visibility_timeout", Integer.toString(queue.getVisibilityTO()))
						   .setString("max_message_size", Integer.toString(queue.getMaxMsgSize()))
						   .setString("message_retention_period", Integer.toString(queue.getMsgRetentionPeriod()))
						   .setString("delay_seconds", Integer.toString(queue.getDelaySeconds()))
						   .setString("policy", queue.getPolicy())
						   .setString("created_time", Long.toString(queue.getCreatedTime()))
						   .setString("wait_time", Integer.toString(queue.getReceiveMessageWaitTimeSeconds()))
						   .setString("number_of_partitions", Integer.toString(queue.getNumberOfPartitions()))
						   .setString("number_of_shards", Integer.toString(queue.getNumberOfShards()))
						   .setString("compressed", Boolean.toString(queue.isCompressed()))
						   .setString("redrive_policy", queue.getRedrivePolicy()),

			insertCQSQueuesByUserId.bind()
								   .setString("user_id", queue.getOwnerUserId())
								   .setString("queue_arn", queue.getArn())
								   .setString("created_time", Long.toString(queue.getCreatedTime()))
		));
	}
	
	@Override
	public void updateQueueAttribute(String queueURL, Map<String, String> queueData) throws PersistenceException {
		BoundStatement updateQueueAttributes = insertCQSQueues.bind();
		updateQueueAttributes.setString("relative_url", queueURL);

		if(queueData.containsKey("visibility_timeout")) {
			updateQueueAttributes.setString("visibility_timeout", queueData.get("visibility_timeout"));
		}
		if(queueData.containsKey("policy")) {
			updateQueueAttributes.setString("policy", queueData.get("policy"));
		}
		if(queueData.containsKey("max_message_size")) {
			updateQueueAttributes.setString("max_message_size", queueData.get("max_message_size"));
		}
		if(queueData.containsKey("message_retention_period")) {
			updateQueueAttributes.setString("message_retention_period", queueData.get("message_retention_period"));
		}
		if(queueData.containsKey("delay_seconds")) {
			updateQueueAttributes.setString("delay_seconds", queueData.get("delay_seconds"));
		}
		if(queueData.containsKey("wait_time")) {
			updateQueueAttributes.setString("wait_time", queueData.get("wait_time"));
		}
		if(queueData.containsKey("number_of_partitions")) {
			updateQueueAttributes.setString("number_of_partitions", queueData.get("number_of_partitions"));
		}
		if(queueData.containsKey("number_of_shards")) {
			updateQueueAttributes.setString("number_of_shards", queueData.get("number_of_shards"));
		}
		if(queueData.containsKey("compressed")) {
			updateQueueAttributes.setString("compressed", queueData.get("compressed"));
		}
		if(queueData.containsKey("redrive_policy")) {
			updateQueueAttributes.setString("redrive_policy", queueData.get("redrive_policy"));
		}
		save(updateQueueAttributes);
	}

	@Override
	public void deleteQueue(String queueUrl) throws PersistenceException {
		if (getQueueByUrl(queueUrl) == null) {
			logger.error("event=delete_queue error_code=queue_does_not_exist queue_url=" + queueUrl);
			throw new PersistenceException (CQSErrorCodes.InvalidRequest, "No queue with the url " + queueUrl + " exists");
		}

		delete(Lists.newArrayList(
			deleteCQSQueues.bind().setString("relative_url", queueUrl),
			deleteCQSQueuesByUserId.bind().setString("user_id", Util.getUserIdForRelativeQueueUrl(queueUrl)).setString("queue_arn", Util.getArnForRelativeQueueUrl(queueUrl))
		));
	}

	@Override
	public List<CQSQueue> listQueues(String userId, String queueNamePrefix, boolean containingMessagesOnly) throws PersistenceException {
		
		if (userId == null || userId.trim().length() == 0) {
			logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
			throw new PersistenceException(CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
		}
			
		List<CQSQueue> queueList = findAll(selectCQSQueuesByUserId.bind().setString("user_id", userId));

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
		return findAll(selectCQSQueuesByUserId.bind().setString("user_id", userId)).size();
	}

	private CQSQueue getQueueByUrl(String queueUrl) throws PersistenceException {
		return findOne(selectCQSQueues.bind().setString("relative_url", queueUrl));
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
							.setString("relative_url", queueUrl)
							.setString("policy", policy)
		);

		return true;
	}

	@Override
	protected CQSQueue convertToInstance(Row row) {
		try {
			String arn = "";
			if (row.getColumnDefinitions().contains("queue_arn")) {
				arn = row.getString("queue_arn");
			}

			String relativeUrl;
			if (row.getColumnDefinitions().contains("relative_url")) {
				relativeUrl = row.getString("relative_url");
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
			if (row.getColumnDefinitions().contains("user_id")) {
				ownerUserId = row.getString("user_id");
			}

			String region = "";
			if (row.getColumnDefinitions().contains("region")) {
				region = row.getString("region");
			}

			int visibilityTO = 0;
			if (row.getColumnDefinitions().contains("visibility_timeout")) {
				visibilityTO = (new Long(row.getString("visibility_timeout"))).intValue();
			}

			int maxMsgSize = 0;
			if (row.getColumnDefinitions().contains("max_message_size")) {
				maxMsgSize = (new Long(row.getString("max_message_size"))).intValue();
			}


			int msgRetentionPeriod = 0;
			if (row.getColumnDefinitions().contains("message_retention_period")) {
				msgRetentionPeriod = (new Long(row.getString("message_retention_period"))).intValue();
			}

			int delaySeconds = 0;
			if (row.getColumnDefinitions().contains("delaySecond")) {
				delaySeconds = row.isNull("delaySecond") ? 0 : (new Long(row.getString("delaySecond"))).intValue();
			}


			int waitTimeSeconds = 0;
			if (row.getColumnDefinitions().contains("wait_time")) {
				waitTimeSeconds = row.isNull("wait_time") ? 0 : (new Long(row.getString("wait_time"))).intValue();
			}

			int numPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
			if (row.getColumnDefinitions().contains("number_of_partitions")) {
				numPartitions = row.isNull("number_of_partitions") ? CMBProperties.getInstance().getCQSNumberOfQueuePartitions() : (new Long(row.getString("number_of_partitions"))).intValue();
			}

			int numShards =  1;
			if (row.getColumnDefinitions().contains("number_of_shards")) {
				numShards = row.isNull("number_of_shards") ? 1 : (new Long(row.getString("number_of_shards"))).intValue();
			}

			String policy = "";
			if (row.getColumnDefinitions().contains("policy")) {
				policy = row.getString("policy");
			}

			long createdTime = 0;
			if (row.getColumnDefinitions().contains("created_time")) {
				createdTime = (new Long(row.getString("created_time")));
			}

			String hostName = "";
			if (row.getColumnDefinitions().contains("service_endpoint")) {
				hostName = row.getString("service_endpoint");
			}

			boolean isCompressed = false;
			if (row.getColumnDefinitions().contains("service_endpoint")) {
				isCompressed = !row.isNull("compressed") && Boolean.getBoolean(row.getString("compressed"));
			}

			String redrivePolicy = "";
			if (row.getColumnDefinitions().contains("redrive_policy")) {
				hostName = row.isNull("redrive_policy")? "" : row.getString("redrive_policy");
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
