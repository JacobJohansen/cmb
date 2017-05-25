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
package com.comcast.cmb.common.persistence;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.CQSErrorCodes;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class UserCassandraPersistence extends BaseCassandraDao<User> implements IUserPersistence {
	private static final Logger logger = Logger.getLogger(UserCassandraPersistence.class);
	private static final CassandraDataStaxPersistence cassandraDataStaxPersistence = DurablePersistenceFactory.getInstance();

	private final String CMB_KEYSPACE = "CMB";
	private final String USER_BY_ID = "Users";
	private final String USERID_BY_ACCESSKEY = "userIdsByAccessKey";
	private final String USERID_BY_NAME = "UserIdByName";


	private final PreparedStatement saveUser;
	private final PreparedStatement saveUserByAccessKey;
	private final PreparedStatement saveUserByName;

	private final PreparedStatement findUserById;
	private final PreparedStatement findUserByName;
	private final PreparedStatement findUserByAccessKey;
	private final PreparedStatement findAll;

	private final PreparedStatement deleteUserById;
	private final PreparedStatement deleteUserByAccessKey;
	private final PreparedStatement deleteUserByName;



	public UserCassandraPersistence() {
		super(cassandraDataStaxPersistence.getSession());

		saveUser = session.prepare(
			QueryBuilder.insertInto(CMB_KEYSPACE, USER_BY_ID)
						.value("userId", bindMarker("userId"))
						.value("userName", bindMarker("userName"))
						.value("hashedPassword", bindMarker("hashedPassword"))
						.value("accessKey", bindMarker("accessKey"))
						.value("accessSecret", bindMarker("accessSecret"))
						.value("isAdmin", bindMarker("isAdmin"))
						.value("description", bindMarker("description"))
		);

		saveUserByAccessKey = session.prepare(
			QueryBuilder.insertInto(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.value("userId", bindMarker("userId"))
						.value("accessKey", bindMarker("accessKey"))
						.value("accessSecret", bindMarker("accessSecret"))
		);

		saveUserByName = session.prepare(
			QueryBuilder.insertInto(CMB_KEYSPACE, USERID_BY_NAME)
						.value("userId", bindMarker("userId"))
						.value("userName", bindMarker("userName"))
						.value("hashedPassword", bindMarker("hashedPassword"))
		);

		deleteUserById = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USER_BY_ID)
						.where(eq("userId", bindMarker("userId")))
		);

		deleteUserByName = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USERID_BY_NAME)
						.where(eq("userName", bindMarker("userName")))
		);

		deleteUserByAccessKey = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.where(eq("accessKey", bindMarker("accessKey")))
		);

		findUserById = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USER_BY_ID)
						.where(eq("userId", bindMarker("userId")))
						.limit(1)
		);

		findUserByAccessKey = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.where(eq("accessKey", bindMarker("accessKey")))
						.limit(1)
		);

		findUserByName = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USERID_BY_NAME)
						.where(eq("userName", bindMarker("userName")))
						.limit(1)
		);

		findAll = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USER_BY_ID)
		);

	}


	public User createUser(String userName, String password) throws PersistenceException {
		return this.createUser(userName, password, false);
	}
	

	public User createUser(String userName, String password, Boolean isAdmin) throws PersistenceException {
		return this.createUser(userName, password, isAdmin, "");
	}
	

	public User createUser(String userName, String password, Boolean isAdmin, String description) throws PersistenceException {
		User user = null;
		String hashedPassword = null;
		String accessSecret = null;
		String accessKey = null;

		if (userName == null || userName.length() < 0 || userName.length() > 25) {
			logger.error("event=create_user error_code=invalid_user_name user_name=" + userName);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid user name " + userName);
		}
		
		if (password == null || password.length() < 0 || password.length() > 25) {
			logger.error("event=create_user error_code=invalid_password");
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid password");
		}

		if (getUserByName(userName) != null) {
			logger.error("event=create_user error_code=user_already_exists user_name=" + userName);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "User with user name " + userName + " already exists");
		}
		UUID userId = UUID.fromString(userName);

		try {
			hashedPassword = AuthUtil.hashPassword(password);
			accessSecret = AuthUtil.generateRandomAccessSecret();
			accessKey = AuthUtil.generateRandomAccessKey();
		} catch (Exception e) {
			logger.error("event=create_user", e);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, e.getMessage());
		}

		user = new User(userId, userName, hashedPassword, accessKey, accessSecret, isAdmin, description);

		saveUser(user);
		return user;
		
	}

	private void saveUser(User user) {
		save(Lists.newArrayList(upsertUser(user), upsertAcessKeyByUserId(user), upsertUserIdByName(user)));
	}

	private BoundStatement upsertUser(User user) {
		return saveUser.bind()
					   .setUUID("userId", user.getUserId())
					   .setString("userName", user.getUserName())
					   .setString("hashedPassword", user.getHashedPassword())
					   .setString("accessKey", user.getAccessKey())
					   .setString("accessSecret", user.getAccessSecret())
					   .setBool("isAdmin", user.getAdmin())
					   .setString("description", user.getDescription());
	}

	private BoundStatement upsertAcessKeyByUserId(User user) {
		return saveUserByAccessKey.bind()
						 		  .setString("accessKey", user.getAccessKey())
								  .setString("accessSecret", user.getAccessSecret())
								  .setUUID("userId", user.getUserId());
	}

	private BoundStatement upsertUserIdByName(User user) {
		return saveUserByName.bind()
							 .setString("userName", user.getUserName())
							 .setString("hashedPassword", user.getHashedPassword())
							 .setUUID("userId", user.getUserId());
	}

	public void deleteUser(String userName) {
		User user = getUserByName(userName);
		save(Lists.newArrayList(deleteUserByAccessKey(user), deleteUserById(user), deleteUserIdByName(user)));
	}

	private BoundStatement deleteUserIdByName(User user) {
		return deleteUserByName.bind()
								.setString("userName", user.getUserName());
	}

	private BoundStatement deleteUserById(User user) {
		return deleteUserById.bind()
							 .setUUID("userId", user.getUserId());
	}

	private BoundStatement deleteUserByAccessKey(User user) {
		return deleteUserByAccessKey.bind()
									.setString("accessKey", user.getAccessKey());
	}

	public long getNumUserQueues(String userId) throws PersistenceException {
		return PersistenceFactory.getQueuePersistence().getNumberOfQueuesByUser(userId);
	}

	public long getNumUserTopics(String userId) throws PersistenceException {
		return PersistenceFactory.getTopicPersistence().getNumberOfTopicsByUser(userId);
	}

	public User getUserById(UUID userId) {
		User user = findOne(findUserById.bind().setUUID("userId", userId));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;
	}

	public User getUserByName(String userName) {
		User user = findOne(findUserByName.bind().setString("userName", userName));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		user = findOne(findUserById.bind().setUUID("userId", user.getUserId()));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;
	}

	public User getUserByAccessKey(String accessKey) {
		User user = findOne(findUserByAccessKey.bind().setString("accessKey", accessKey));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		user = findOne(findUserById.bind().setUUID("userId", user.getUserId()));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;

	}

	public List<User> getAllUsers() {
		return find(Lists.newArrayList(findAll.bind()));
	}

	public User createDefaultUser() throws PersistenceException {
		return createUser(CMBProperties.getInstance().getCNSUserName(), CMBProperties.getInstance().getCNSUserPassword(), true);
	}

	@Override protected User convertToInstance(Row row) {
		UUID userId = row.getUUID("userId");


		String userName = null;
		String hashedPassword = null;
		String accessKey = null;
		String accessSecret = null;
		Boolean isAdmin = null;
		String description = null;


		if(row.getColumnDefinitions().contains("userName")) {
			userName = row.getString("userName");
		}

		if(row.getColumnDefinitions().contains("hashedPassword")) {
			hashedPassword = row.getString("hashedPassword");
		}

		if(row.getColumnDefinitions().contains("accessKey")) {
			accessKey = row.getString("accessKey");
		}

		if(row.getColumnDefinitions().contains("accessSecret")) {
			accessSecret = row.getString("accessSecret");
		}

		if(row.getColumnDefinitions().contains("isAdmin")) {
			isAdmin = row.getBool("isAdmin");
		}

		if(row.getColumnDefinitions().contains("description")) {
			description = row.getString("description");
		}

		return new User(userId, userName, hashedPassword, accessKey, accessSecret, isAdmin, description);
	}
}

