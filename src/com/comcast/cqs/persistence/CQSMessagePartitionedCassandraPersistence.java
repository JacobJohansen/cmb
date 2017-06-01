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

import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.BaseCassandraDao;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.controller.CQSCache;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSMessageAttribute;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.RandomNumberCollection;
import com.comcast.cqs.util.Util;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.eaio.uuid.UUIDGen;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Cassandra persistence for CQS Message
 * @author aseem, vvenkatraman, bwolf
 *
 */
public class CQSMessagePartitionedCassandraPersistence extends BaseCassandraDao<CQSMessage> implements ICQSMessagePersistence {
	
	private static final String COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES = "cqs_partitioned_queue_messages";
	private static final Random rand = new Random();
	private static Logger logger = Logger.getLogger(CQSMessagePartitionedCassandraPersistence.class);

	private final PreparedStatement insertMessage;
	private final PreparedStatement selectMessage;
	private final PreparedStatement selectMessages;
	private final PreparedStatement deleteMessages;
	private final PreparedStatement deleteAllMessagesPartitionAndShard;

	public CQSMessagePartitionedCassandraPersistence() {
		super(DurablePersistenceFactory.getInstance().getSession());
		insertMessage = session.prepare(
			QueryBuilder.insertInto("cqs", COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES)
				.value("queue_shard_partition", bindMarker("queue_shard_partition"))
				.value("become_visible_time", bindMarker("become_visible_time"))
				.value("node_uuid", bindMarker("node_uuid"))
				.value("message", bindMarker("message"))
				.using(ttl(bindMarker()))
		);

		selectMessage = session.prepare(
			QueryBuilder.select()
						.all()
						.from("cqs", COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES)
						.where(eq("queue_shard_partition", bindMarker("queue_shard_partition")))
						.and(eq("become_visible_time", bindMarker("become_visible_time")))
						.and(eq("node_uuid", bindMarker("node_uuid")))
		);

		selectMessages = session.prepare(
			QueryBuilder.select()
						.all()
						.from("cqs", COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES)
						.where(eq("queue_shard_partition", bindMarker("queue_shard_partition")))
						.and(lte("become_visible_time", bindMarker("latest")))
						.and(gte("become_visible_time", bindMarker("oldest")))
		);

		deleteMessages = session.prepare(
			QueryBuilder.delete()
						.from("cqs", COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES)
						.where(eq("queue_shard_partition", bindMarker("queue_shard_partition")))
						.and(eq("become_visible_time", bindMarker("become_visible_time")))
						.and(eq("node_uuid", bindMarker("node_uuid")))
		);

		deleteAllMessagesPartitionAndShard = session.prepare(
			QueryBuilder.delete()
						.from("cqs", COLUMN_FAMILY_PARTITIONED_QUEUE_MESSAGES)
						.where(eq("queue_shard_partition", bindMarker("queue_shard_partition")))
		);
	}

	@Override
	public String sendMessage(CQSQueue queue, int shard, CQSMessage message) throws PersistenceException, IOException, InterruptedException, NoSuchAlgorithmException, JSONException {
		
		if (queue == null) {
			throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue does not exist");
		}
		
		if (message == null) {
			throw new PersistenceException(CQSErrorCodes.InvalidMessageContents, "The supplied message is invalid");
		}
		
		int delaySeconds = 0;
		
		if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
			delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
		}
		
		long ts = System.currentTimeMillis() + delaySeconds*1000;
		long becomeVisibleTime = AbstractDurablePersistence.newTime(ts, false);
		long nodeUUID = UUIDGen.getClockSeqAndNode();
		int ttl = queue.getMsgRetentionPeriod();

		int partition = rand.nextInt(queue.getNumberOfPartitions());
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + partition;

		if(message.getAttributes() == null) {
			message.setAttributes(new HashMap<>());
		}

		message.getAttributes().put("isCompressed", Boolean.toString(queue.isCompressed()));

		if (queue.isCompressed()) {
			message.setBody(Util.compress(message.getBody()));
		}

		message.setMessageId(key + ":" + becomeVisibleTime + ":" + nodeUUID);

		logger.debug("event=send_message ttl=" + ttl + " delay_sec=" + delaySeconds + " msg_id=" + message.getMessageId() + " key=" + key + " col=" + message.getMessageId());

		save(
			insertMessage.bind()
						 .setString("queue_shard_partition", key)
						 .setLong("become_visible_time", becomeVisibleTime)
						 .setLong("node_uuid", nodeUUID)
						 .setString("message", getMessageJSON(message))
						 .setInt("[ttl]", ttl)
		);

		return message.getMessageId();
	}
	
	private String getMessageJSON(CQSMessage message) throws JSONException {
		
		Writer writer = new StringWriter(); 
    	JSONWriter jw = new JSONWriter(writer);

    	jw = jw.object();
    	jw.key("MessageId").value(message.getMessageId());
    	jw.key("MD5OfBody").value(message.getMD5OfBody());
    	jw.key("Body").value(message.getBody());
		
		if (message.getAttributes() == null) {
			message.setAttributes(new HashMap<String, String>());
		}
		
		if (!message.getAttributes().containsKey(CQSConstants.SENT_TIMESTAMP)) {
			message.getAttributes().put(CQSConstants.SENT_TIMESTAMP, "" + System.currentTimeMillis());
		}
		
		if (!message.getAttributes().containsKey(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) {
			message.getAttributes().put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
		}
		
		if (message.getAttributes() != null) {
			for (String key : message.getAttributes().keySet()) {
				String value = message.getAttributes().get(key);
				if (value == null || value.isEmpty()) {
					value = "";
				}
				jw.key(key).value(value);
			}
		}
		
		if (message.getMessageAttributes() != null && message.getMessageAttributes().size() > 0) {
	    	jw.key("MD5OfMessageAttributes").value(message.getMD5OfMessageAttributes());
			jw.key("MessageAttributes");
			jw.object();
			for (String key : message.getMessageAttributes().keySet()) {
				jw.key(key);
				jw.object();
				CQSMessageAttribute messageAttribute = message.getMessageAttributes().get(key);
				if (messageAttribute.getStringValue() != null) {
					jw.key("StringValue").value(messageAttribute.getStringValue());
				} else if (messageAttribute.getBinaryValue() != null) {
					jw.key("BinaryValue").value(messageAttribute.getBinaryValue());
				}
				jw.key("DataType").value(messageAttribute.getDataType());
				jw.endObject();
			}
			jw.endObject();
		}

		jw.endObject();
		
		return writer.toString();
	}

	@Override
	public Map<String, String> sendMessageBatch(CQSQueue queue,	int shard, List<CQSMessage> messages) throws PersistenceException,	IOException, InterruptedException, NoSuchAlgorithmException, JSONException {

		if (queue == null) {
			throw new PersistenceException(CQSErrorCodes.NonExistentQueue, "The supplied queue doesn't exist");
		}

		if (messages == null || messages.size() == 0) {
			throw new PersistenceException(CQSErrorCodes.InvalidQueryParameter,	"No messages are supplied.");
		}
		List<Statement> boundStatements = Lists.newArrayList();

		Map<String, String> ret = new HashMap<String, String>();
		int ttl = queue.getMsgRetentionPeriod();
		String key = Util.hashQueueUrl(queue.getRelativeUrl()) + "_" + shard + "_" + rand.nextInt(queue.getNumberOfPartitions());
		
		for (CQSMessage message : messages) {

			if (message == null) {
				throw new PersistenceException(CQSErrorCodes.InvalidMessageContents, "The supplied message is invalid");
			}
			
			if (queue.isCompressed()) {
				message.setBody(Util.compress(message.getBody()));
			}
			
			int delaySeconds = 0;
			
			if (message.getAttributes().containsKey(CQSConstants.DELAY_SECONDS)) {
				delaySeconds = Integer.parseInt(message.getAttributes().get(CQSConstants.DELAY_SECONDS));
			}


			long ts = System.currentTimeMillis() + delaySeconds*1000;
			long becomeVisibleTime = AbstractDurablePersistence.newTime(ts, false);
			long nodeUUID = UUIDGen.getClockSeqAndNode();


			message.setMessageId(key + ":" + becomeVisibleTime + ":" + nodeUUID);
			
			logger.debug("event=send_message_batch msg_id=" + message.getMessageId() + " ttl=" + ttl + " delay_sec=" + delaySeconds + " key=" + key + " col=" + message.getMessageId());

			boundStatements.add(insertMessage.bind()
											 .setString("queue_shard_partition", key)
											 .setLong("become_visible_time", becomeVisibleTime)
											 .setLong("node_uuid", nodeUUID)
											 .setString("message", getMessageJSON(message))
											 .setInt("[ttl]", ttl));

			ret.put(message.getSuppliedMessageId(), message.getMessageId());
		}

		save(boundStatements);
		
		return ret;
	}

	@Override
	public void deleteMessage(String queueUrl, String receiptHandle) throws PersistenceException {
		
		if (receiptHandle == null) {
			logger.error("event=delete_message event=no_receipt_handle queue_url=" + queueUrl);
			return;
		}
		
		String[] receiptHandleParts = receiptHandle.split(":");
		
		if (receiptHandleParts.length != 3) {
			logger.error("event=delete_message event=invalid_receipt_handle queue_url=" + queueUrl + " receipt_handle=" + receiptHandle);
			return;
		}

		logger.debug("event=delete_message receipt_handle=" + receiptHandle + " col=" + receiptHandle);
		delete(
			deleteMessages.bind()
						  .setString("queue_shard_partition", receiptHandleParts[0])
						  .setLong("become_visible_time", Long.parseLong(receiptHandleParts[1]))
						  .setLong("node_uuid", Long.parseLong(receiptHandleParts[2]))
		);
	}

	@Override
	public List<CQSMessage> receiveMessage(CQSQueue queue, Map<String, String> receiveAttributes) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
		throw new UnsupportedOperationException("ReceiveMessage is not supported, please call getMessages instead");
	}

	@Override
	public boolean changeMessageVisibility(CQSQueue queue, String receiptHandle, int visibilityTO) throws PersistenceException, IOException, NoSuchAlgorithmException, InterruptedException {
		throw new UnsupportedOperationException("ChangeMessageVisibility is not supported");
	}

	@Override
	public List<CQSMessage> peekQueue(String queueUrl, int shard, String previousReceiptHandle, String nextReceiptHandle, int length) throws IOException, NoSuchAlgorithmException, JSONException, PersistenceException {
		List<CQSMessage> messageList = new ArrayList<CQSMessage>();
		String queueHash = Util.hashQueueUrl(queueUrl);
		String key = queueHash + "_" + shard + "_0";
		String handle = null;
		
		int numberPartitions = getNumberOfPartitions(queueUrl);
		int numberShards = getNumberOfShards(queueUrl);

		if (nextReceiptHandle != null) {
			handle = nextReceiptHandle;
		} else if (previousReceiptHandle != null) {
			handle = previousReceiptHandle;
		}

		if (handle != null) {
			String[] handleParts = handle.split(":");

			if (handleParts.length != 3) {
				logger.error("event=peek_queue error_code=corrupt_receipt_handle receipt_handle=" + handle);
				throw new IllegalArgumentException("Corrupt receipt handle " + handle);
			}
			key = handleParts[0];
		}

		String[] queueParts = key.split("_");

		if (queueParts.length != 3) {
			logger.error("event=peek_queue error_code=invalid_queue_key key=" + key);
			throw new IllegalArgumentException("Invalid queue key " + key);
		}



		logger.debug("event=peek_queue queue_url=" + queueUrl + " prev_receipt_handle=" + previousReceiptHandle + " next_receipt_handle=" + nextReceiptHandle + " length=" + length + " num_partitions=" + numberPartitions);


		int shardNumber = Integer.parseInt(queueParts[1]);
		int partitionNumber = Integer.parseInt(queueParts[2]);
		
		if (shardNumber < 0 || shardNumber > numberShards-1) {
			logger.error("event=peek_queue error_code=invalid_shard_number shard_number=" + shardNumber);
			throw new IllegalArgumentException("Invalid queue shard number " + shardNumber);			
		}

		while (messageList.size() < length && -1 < partitionNumber && partitionNumber < numberPartitions) {
			key = queueHash + "_" + shardNumber + "_" + partitionNumber;
			List<CQSMessage> page = find(selectMessages.bind().setString("queue_shard_partition", key), null, length - messageList.size() + 1);



			partitionNumber++;
		}
		
		return messageList;
	}

	@Override
	public void clearQueue(String queueUrl, int shard) throws PersistenceException, NoSuchAlgorithmException, UnsupportedEncodingException {
		
		int numberPartitions = getNumberOfPartitions(queueUrl);
		
		logger.debug("event=clear_queue queue_url=" + queueUrl + " num_partitions=" + numberPartitions);
		List<Statement> clearAllPartitions = Lists.newArrayList();
		
		for (int i=0; i<numberPartitions; i++) {
			String key = Util.hashQueueUrl(queueUrl) + "_" + shard + "_" + i;
			clearAllPartitions.add(
				deleteAllMessagesPartitionAndShard.bind().setString("queue_shard_partition", key)
			);
		}

		delete(clearAllPartitions);
	}

	@Override
	public Map<String, CQSMessage> getMessages(String queueUrl, List<String> ids) throws NoSuchAlgorithmException, IOException, JSONException, PersistenceException {
		
		Map<String, CQSMessage> messageMap = new HashMap<String, CQSMessage>();
		
		logger.debug("event=get_messages ids=" + ids);
		
		if (ids == null || ids.size() == 0) {
			return messageMap;
		}
		List<Statement> statements = Lists.newArrayList();
		for (String id: ids) {
			
			String[] idParts = id.split(":");
			
			if (idParts.length != 3) {
				logger.error("event=get_messages error_code=invalid_message_id id=" + id);
				throw new IllegalArgumentException("Invalid message id " + id);
			}

			statements.add(selectMessage.bind().setString("queue_shard_partition", idParts[0]).setLong("become_visible_time", Long.parseLong(idParts[1])).setLong("node_uuid",Long.parseLong(idParts[2])));
		}
		
		return find(statements).stream().collect(Collectors.toMap(CQSMessage::getMessageId, Function.identity()));
	}
	
	private Map<String, Map<String, String>> getFirstAndLastIdsForEachPartition(List<String> ids, Set<String> messageIdSet) {
		
		Map<String, Map<String, String>> firstLastForEachPartitionMap = new HashMap<String, Map<String, String>>();
		
		if (ids == null || ids.size() == 0) {
			return firstLastForEachPartitionMap;
		}
		
		for (String id: ids) {			
			
			messageIdSet.add(id);
			String[] idParts = id.split(":");
			
			if (idParts.length != 3) {
				logger.error("action=get_messages_bulk error_code=corrupt_receipt_handle receipt_handle=" + id);
				throw new IllegalArgumentException("Corrupt receipt handle " + id);
			}
			
			String queuePartition = idParts[0];
			final String messageId = idParts[1] + ":" + idParts[2];
			
			if (!firstLastForEachPartitionMap.containsKey(queuePartition)) {
				firstLastForEachPartitionMap.put(queuePartition, new HashMap<String, String>() {{put("First", messageId); put("Last", messageId);}});
			} else {
				Map<String, String> firstLastForPartition = firstLastForEachPartitionMap.get(queuePartition);
				if (firstLastForPartition.get("First").compareTo(messageId) > 0) {
					firstLastForPartition.put("First", messageId);
				} 
				else if (firstLastForPartition.get("Last").compareTo(messageId) < 0) {
					firstLastForPartition.put("Last", messageId);
				}
			}
		}
		
		return firstLastForEachPartitionMap;
	}
	
	private int getNumberOfPartitions(String queueUrl) {

		int numberPartitions = CMBProperties.getInstance().getCQSNumberOfQueuePartitions();
		
		try {
			
			CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
			
			if (queue != null) {
				numberPartitions = queue.getNumberOfPartitions();
			}
			
		} catch (Exception ex) {
			logger.warn("event=queue_cache_failure queue_url=" + queueUrl, ex);
		}

		return numberPartitions;
	}

	private int getNumberOfShards(String queueUrl) {

		int numberShards = 1;
		
		try {
			
			CQSQueue queue = CQSCache.getCachedQueue(queueUrl);
			
			if (queue != null) {
				numberShards = queue.getNumberOfShards();
			}
			
		} catch (Exception ex) {
			logger.warn("event=queue_cache_failure queue_url=" + queueUrl, ex);
		}

		return numberShards;
	}

	@Override
    public List<CQSMessage> peekQueueRandom(String queueUrl, int shard, int length) throws IOException, NoSuchAlgorithmException, JSONException, PersistenceException {
        
    	String queueHash = Util.hashQueueUrl(queueUrl);

    	int numberPartitions = getNumberOfPartitions(queueUrl);

    	logger.debug("event=peek_queue_random queue_url=" + queueUrl + " shard=" + shard + " queue_hash=" + queueHash + " num_partitions=" + numberPartitions);
    	
    	List<CQSMessage> messageList = new ArrayList<CQSMessage>();
        
        if (length > numberPartitions) {
            
        	// no randomness, get from all rows, subsequent calls will return the same result
            
        	return peekQueue(queueUrl, shard, null, null, length);
        
        } else {
            
        	// get from random set of rows
            // note: as a simplification we may return less messages than length if not all rows contain messages
            
        	RandomNumberCollection rc = new RandomNumberCollection(numberPartitions);
            
            for (int i = 0; i < numberPartitions && messageList.size() < length; i++) {
                
            	int partition = rc.getNext();
                String key = queueHash + "_" + shard + "_" + partition;
                messageList.addAll(find(selectMessages.bind().setString("queue_shard_partition", key), null, 1));
            }
            
            return messageList;
        }        
    }

	@Override
	public List<String> getIdsFromHead(String queueUrl, int shard, int num) throws PersistenceException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getQueueMessageCount(String queueUrl) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean checkCacheConsistency(String queueUrl, int shard, boolean trueOnFiller) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getQueueMessageCount(String queueUrl, boolean processHiddenIds) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getNumConnections() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCacheQueueMessageCount(String queueUrl) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getQueueNotVisibleMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getQueueDelayedMessageCount(String queueUrl, boolean visibilityProcessFlag) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void flushAll() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getNumberOfRedisShards() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Map<String, String>> getInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMemQueueMessageCreatedTS(String memId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override protected CQSMessage convertToInstance(Row row) {
		try {
			CQSMessage m = new CQSMessage();

			JSONObject json = new JSONObject(row.getString("message"));

			m.setMessageId(json.getString("MessageId"));
			m.setReceiptHandle(json.getString("MessageId"));
			m.setMD5OfBody(json.getString("MD5OfBody"));
			m.setBody(json.getString("Body"));

			if (m.getAttributes() == null) {
				m.setAttributes(new HashMap<String, String>());
			}

			if (json.has(CQSConstants.SENT_TIMESTAMP)) {
				m.getAttributes().put(CQSConstants.SENT_TIMESTAMP, json.getString(CQSConstants.SENT_TIMESTAMP));
			}

			if (json.has(CQSConstants.APPROXIMATE_RECEIVE_COUNT)) {
				m.getAttributes().put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, json.getString(CQSConstants.APPROXIMATE_RECEIVE_COUNT));

			}

			if (json.has(CQSConstants.SENDER_ID)) {
				m.getAttributes().put(CQSConstants.SENDER_ID, json.getString(CQSConstants.SENDER_ID));
			}

			if (json.has("MessageAttributes")) {
				m.setMD5OfMessageAttributes(json.getString("MD5OfMessageAttributes"));
				JSONObject messageAttributes = json.getJSONObject("MessageAttributes");
				Map<String, CQSMessageAttribute> ma = new HashMap<String, CQSMessageAttribute>();
				Iterator<String> iter = messageAttributes.keys();
				while (iter.hasNext()) {
					String key = iter.next();
					ma.put(key, new CQSMessageAttribute(messageAttributes.getJSONObject(key).getString("StringValue"), messageAttributes.getJSONObject(key).getString("DataType")));
				}
				m.setMessageAttributes(ma);
			}

			m.setTimebasedId(row.getLong("become_visible_time"));

			if (m.getAttributes() != null && m.getAttributes().containsKey("isCompressed") && Boolean.parseBoolean(m.getAttributes().get("isCompressed"))) {
				m.setBody(Util.decompress(m.getBody()));
			}
			return m;
		} catch (Exception e) {

		}
		return null;
	}
}
