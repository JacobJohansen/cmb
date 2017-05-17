package com.comcast.cmb.common.model;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;

import java.util.UUID;

@Accessor
public interface UserAccessor {

    @Query("insert into CMB.Users (userId, userName, hashedPassword, accessKey, accessSecret, isAdmin, description) " +
          "values (:user.userId, :user.userName, :user.hashedPassword, :user.accessKey, :user.accessSecret, :user.isAdmin, :user.description)")
    ResultSetFuture save(@Param("user") User user);

    @Query("insert into CMB.userIdsByAccessKey (userId, accessKey, accessSecret) " +
          "values (:user.userId, :user.accessKey, :user.accessSecret)")
    ResultSetFuture saveAccessKeyByById(@Param("user") User user);

    @Query("insert into CMB.UserIdByName (userId, userName, hashedPassword) " +
          "values (:user.userId, :user.userName, :user.hashedPassword)")
    ResultSetFuture saveUserIdByName(@Param("user")User user);

    @Query("delete from CMB.Users where userId = :userId")
    ResultSetFuture delete(@Param("userId") UUID userId);

    @Query("delete from CMB.userIdsByAccessKey where accessKey = :accessKey")
    ResultSetFuture deleteAccessKeyByById(@Param("accessKey") String accessKey);

    @Query("delete from CMB.UserIdByName where userName = :userName")
    ResultSetFuture deleteUserIdByName(@Param("userName")String userName);

    @Query("select * from CMB.Users where userId = :userId")
    User find(@Param("userId") UUID userId);

    @Query("select * from CMB.userIdsByAccessKey where accessKey = :accessKey")
    User findAccessKeyByById(@Param("accessKey") String accessKey);

    @Query("select * from CMB.UserIdByName where userName = :userName")
    User findUserIdByName(@Param("userName")String userName);

    @Query("select * from CMB.Users")
    Result<User> findAll();
}
