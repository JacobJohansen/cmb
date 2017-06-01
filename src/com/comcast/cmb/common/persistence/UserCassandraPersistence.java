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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class UserCassandraPersistence extends BaseCassandraDao<User> implements IUserPersistence {
	private static final Logger logger = Logger.getLogger(UserCassandraPersistence.class);
	private static final CassandraDataStaxPersistence cassandraDataStaxPersistence = DurablePersistenceFactory.getInstance();

	private final String CMB_KEYSPACE = "cmb";
	private final String USER_BY_ID = "users";
	private final String USERID_BY_ACCESSKEY = "user_ids_by_access_key";
	private final String USERID_BY_NAME = "user_id_by_name";


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
						.value("user_id", bindMarker("user_id"))
						.value("user_name", bindMarker("user_name"))
						.value("hash_password", bindMarker("hash_password"))
						.value("access_key", bindMarker("access_key"))
						.value("access_secret", bindMarker("access_secret"))
						.value("is_admin", bindMarker("is_admin"))
						.value("description", bindMarker("description"))
		);

		saveUserByAccessKey = session.prepare(
			QueryBuilder.insertInto(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.value("user_id", bindMarker("user_id"))
						.value("access_key", bindMarker("access_key"))
						.value("access_secret", bindMarker("access_secret"))
		);

		saveUserByName = session.prepare(
			QueryBuilder.insertInto(CMB_KEYSPACE, USERID_BY_NAME)
						.value("user_id", bindMarker("user_id"))
						.value("user_name", bindMarker("user_name"))
						.value("hash_password", bindMarker("hash_password"))
		);

		deleteUserById = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USER_BY_ID)
						.where(eq("user_id", bindMarker("user_id")))
		);

		deleteUserByName = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USERID_BY_NAME)
						.where(eq("user_name", bindMarker("user_name")))
		);

		deleteUserByAccessKey = session.prepare(
			QueryBuilder.delete()
						.from(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.where(eq("access_key", bindMarker("access_key")))
		);

		findUserById = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USER_BY_ID)
						.where(eq("user_id", bindMarker("user_id")))
		);

		findUserByAccessKey = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USERID_BY_ACCESSKEY)
						.where(eq("access_key", bindMarker("access_key")))
						.limit(1)
		);

		findUserByName = session.prepare(
			QueryBuilder.select()
						.all()
						.from(CMB_KEYSPACE, USERID_BY_NAME)
						.where(eq("user_name", bindMarker("user_name")))
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
		String hash_password = null;
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

		User tempUser = null;
		try {
			tempUser = 	getUserByName(userName);
		} catch (Exception e) {

		}

		if (tempUser != null) {
			logger.error("event=create_user error_code=user_already_exists user_name=" + userName);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, "User with user name " + userName + " already exists");
		}
		String userId = Long.toString(System.currentTimeMillis()).substring(1);

		try {
			hash_password = AuthUtil.hashPassword(password);
			accessSecret = AuthUtil.generateRandomAccessSecret();
			accessKey = AuthUtil.generateRandomAccessKey();
		} catch (Exception e) {
			logger.error("event=create_user", e);
			throw new PersistenceException(CQSErrorCodes.InvalidRequest, e.getMessage());
		}

		user = new User(userId, userName, hash_password, accessKey, accessSecret, isAdmin, description);

		saveUser(user);
		return user;
		
	}

	private void saveUser(User user) {
		save(Lists.newArrayList(upsertUser(user), upsertAcessKeyByUserId(user), upsertUserIdByName(user)));
	}

	private BoundStatement upsertUser(User user) {
		return saveUser.bind()
					   .setString("user_id", user.getUserId())
					   .setString("user_name", user.getUserName())
					   .setString("hash_password", user.getHashedPassword())
					   .setString("access_key", user.getAccessKey())
					   .setString("access_secret", user.getAccessSecret())
					   .setString("is_admin", Boolean.toString(user.getAdmin()))
					   .setString("description", user.getDescription());
	}

	private BoundStatement upsertAcessKeyByUserId(User user) {
		return saveUserByAccessKey.bind()
						 		  .setString("access_key", user.getAccessKey())
								  .setString("access_secret", user.getAccessSecret())
								  .setString("user_id", user.getUserId());
	}

	private BoundStatement upsertUserIdByName(User user) {
		return saveUserByName.bind()
							 .setString("user_name", user.getUserName())
							 .setString("hash_password", user.getHashedPassword())
							 .setString("user_id", user.getUserId());
	}

	public void deleteUser(String userName) {
		User user = getUserByName(userName);
		save(Lists.newArrayList(deleteUserByAccessKey(user), deleteUserById(user), deleteUserIdByName(user)));
	}

	private BoundStatement deleteUserIdByName(User user) {
		return deleteUserByName.bind()
								.setString("user_name", user.getUserName());
	}

	private BoundStatement deleteUserById(User user) {
		return deleteUserById.bind()
							 .setString("user_id", user.getUserId());
	}

	private BoundStatement deleteUserByAccessKey(User user) {
		return deleteUserByAccessKey.bind()
									.setString("access_key", user.getAccessKey());
	}

	public long getNumUserQueues(String userId) throws PersistenceException {
		return PersistenceFactory.getQueuePersistence().getNumberOfQueuesByUser(userId);
	}

	public long getNumUserTopics(String userId) throws PersistenceException {
		return PersistenceFactory.getTopicPersistence().getNumberOfTopicsByUser(userId);
	}

	public User getUserById(String userId) {
		User user = findOne(findUserById.bind().setString("user_id", userId));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;
	}

	public User getUserByName(String userName) {
		User user = findOne(findUserByName.bind().setString("user_name", userName));
		if(user == null) {
			return null;
		}
		user = findOne(findUserById.bind().setString("user_id", user.getUserId()));
		return user;
	}

	public User getUserByAccessKey(String accessKey) {
		User user = findOne(findUserByAccessKey.bind().setString("access_key", accessKey));
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		user = findOne(findUserById.bind().setString("user_id", user.getUserId()));
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
		String userId = row.getString("user_id");
		String userName = null;
		String hashPassword = null;
		String accessKey = null;
		String accessSecret = null;
		Boolean isAdmin = null;
		String description = null;


		if(row.getColumnDefinitions().contains("user_name")) {
			userName = row.getString("user_name");
		}

		if(row.getColumnDefinitions().contains("hash_password")) {
			hashPassword = row.getString("hash_password");
		}

		if(row.getColumnDefinitions().contains("access_key")) {
			accessKey = row.getString("access_key");
		}

		if(row.getColumnDefinitions().contains("access_secret")) {
			accessSecret = row.getString("access_secret");
		}

		if(row.getColumnDefinitions().contains("is_admin")) {
			isAdmin = Boolean.parseBoolean(row.getString("is_admin"));
		}

		if(row.getColumnDefinitions().contains("description")) {
			description = row.getString("description");
		}

		return new User(userId, userName, hashPassword, accessKey, accessSecret, isAdmin, description);
	}
}

