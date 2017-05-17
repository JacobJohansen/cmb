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
package com.comcast.cmb.common.model;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import javax.persistence.Transient;
import java.util.UUID;

@Table(keyspace = "CMB", name = "Users")
public class User {

	// system generated user ID, must be globally unique
    @PartitionKey
	UUID userId;
	
	// user name, must be globally unique
    private String userName;
    
    // hashed password    
    private String hashedPassword;
    private String accessKey;
    private String accessSecret;
    private Boolean isAdmin;
    private String description = "";
    
    // some stats about the user
    @Transient
    private long numQueues;
    @Transient
	private long numTopics;
    
    public User(UUID userId, String userName, String hashedPassword, String accessKey, String accessSecret, Boolean isAdmin) {
        new User(userId, userName, hashedPassword, accessKey, accessKey, isAdmin, "");
    }
    
    public User(UUID userId, String userName, String hashedPassword, String accessKey, String accessSecret) {
        new User(userId, userName, hashedPassword, accessKey, accessKey, false, "");
    }
    
    public User(UUID userId, String userName, String hashedPassword, String accessKey, String accessSecret, Boolean isAdmin, String description) {
        this.userId = userId;
        this.userName = userName;
        this.hashedPassword = hashedPassword;
        this.accessKey = accessKey;
        this.accessSecret = accessSecret;
        this.isAdmin = isAdmin;
        this.description = description;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getHashedPassword() {
        return hashedPassword;
    }

    public void setHashedPassword(String hashedPassword) {
        this.hashedPassword = hashedPassword;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public void setAccessSecret(String accessSecret) {
        this.accessSecret = accessSecret;
    }

    public Boolean getAdmin() {
        return isAdmin;
    }

    public void setAdmin(Boolean admin) {
        isAdmin = admin;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getNumQueues() {
        return numQueues;
    }

    public void setNumQueues(long numQueues) {
        this.numQueues = numQueues;
    }

    public long getNumTopics() {
        return numTopics;
    }

    public void setNumTopics(long numTopics) {
        this.numTopics = numTopics;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User)) return false;

        User user = (User) o;

        if (numQueues != user.numQueues) return false;
        if (numTopics != user.numTopics) return false;
        if (userId != null ? !userId.equals(user.userId) : user.userId != null) return false;
        if (userName != null ? !userName.equals(user.userName) : user.userName != null) return false;
        if (hashedPassword != null ? !hashedPassword.equals(user.hashedPassword) : user.hashedPassword != null)
            return false;
        if (accessKey != null ? !accessKey.equals(user.accessKey) : user.accessKey != null) return false;
        if (accessSecret != null ? !accessSecret.equals(user.accessSecret) : user.accessSecret != null) return false;
        if (isAdmin != null ? !isAdmin.equals(user.isAdmin) : user.isAdmin != null) return false;
        return description != null ? description.equals(user.description) : user.description == null;
    }

    @Override public int hashCode() {
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        result = 31 * result + (hashedPassword != null ? hashedPassword.hashCode() : 0);
        result = 31 * result + (accessKey != null ? accessKey.hashCode() : 0);
        result = 31 * result + (accessSecret != null ? accessSecret.hashCode() : 0);
        result = 31 * result + (isAdmin != null ? isAdmin.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (int) (numQueues ^ (numQueues >>> 32));
        result = 31 * result + (int) (numTopics ^ (numTopics >>> 32));
        return result;
    }
}
