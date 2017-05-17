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
import com.comcast.cmb.common.model.UserAccessor;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.CQSErrorCodes;
import org.apache.log4j.Logger;

import java.util.*;

public class UserCassandraPersistence implements IUserPersistence {
	private static final Logger logger = Logger.getLogger(UserCassandraPersistence.class);
	
	private static final CassandraDataStaxPersistence cassandraDataStaxPersistence = DurablePersistenceFactory.getInstance();
	UserAccessor userDao;
	public UserCassandraPersistence() {
		userDao = cassandraDataStaxPersistence.getMappingManager().createAccessor(UserAccessor.class);
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
		userDao.save(user);
		userDao.saveAccessKeyByById(user);
		userDao.saveUserIdByName(user);
		
		return user;
		
	}

	public void deleteUser(String userName) {
		User user = getUserByName(userName);

		userDao.deleteAccessKeyByById(user.getAccessKey());
		userDao.deleteUserIdByName(user.getUserName());
		userDao.delete(user.getUserId());
	}

	public long getNumUserQueues(String userId) throws PersistenceException {
		return PersistenceFactory.getQueuePersistence().getNumberOfQueuesByUser(userId);
	}

	public long getNumUserTopics(String userId) throws PersistenceException {
		return PersistenceFactory.getTopicPersistence().getNumberOfTopicsByUser(userId);
	}

	public User getUserById(UUID userId) {
		User user = userDao.find(userId);
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;
	}

	public User getUserByName(String userName) {
		User user = userDao.findUserIdByName(userName);
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		user = userDao.find(user.getUserId());
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;
	}


	public User getUserByAccessKey(String accessKey) {
		User user = userDao.findAccessKeyByById(accessKey);
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		user = userDao.find(user.getUserId());
		if(user == null) {
			throw new RuntimeException("User Does not exist");
		}
		return user;

	}

	public List<User> getAllUsers() {
		return userDao.findAll().all();
	}

	public User createDefaultUser() throws PersistenceException {
		return createUser(CMBProperties.getInstance().getCNSUserName(), CMBProperties.getInstance().getCNSUserPassword(), true);
	}

}

