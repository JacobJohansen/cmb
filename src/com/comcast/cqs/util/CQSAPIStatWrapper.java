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
package com.comcast.cqs.util;

import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.model.CQSAPIStats;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CQSAPIStatWrapper {
	
	public static final String CNS_API_SERVERS = "cns_api_servers";
	public static final String CQS_API_SERVERS = "cqs_api_servers";
	
	public static List<CQSAPIStats> getCNSAPIStats() throws PersistenceException{
		ResultSet resultSet = DurablePersistenceFactory.getInstance().getSession().execute(QueryBuilder.select().all().from("cns", CNS_API_SERVERS));
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
			
		for (Row row : resultSet.all()) {

			CQSAPIStats stats = new CQSAPIStats();
			stats.setIpAddress(row.getString("host"));

			if (!row.isNull("timestamp")) {
				stats.setTimestamp(Long.parseLong(row.getString("timestamp")));
			}

			if (!row.isNull("jmxport")) {
				stats.setJmxPort(Long.parseLong(row.getString("jmxport")));
			}

			if (!row.isNull("data_center")) {
				stats.setDataCenter(row.getString("data_center"));
			}

			if (!row.isNull("service_url")) {
				stats.setServiceUrl(row.getString("service_url"));
			}

			if (stats.getIpAddress().contains(":")) {
				statsList.add(stats);
			}
		}

		return statsList;
	}
	
	//the first data center is the the local data center
	
	public static List<String> getCNSDataCenterNames() throws PersistenceException {
		
		List<CQSAPIStats> statsList = getCNSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		
		for (CQSAPIStats currentCQSAPIStat : statsList) {
			if ((currentCQSAPIStat.getDataCenter()!=null)&&(!currentCQSAPIStat.getDataCenter().equals(localDataCenter))) {
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	//the first data center is the the local data center
	public static List<String> getCQSDataCenterNames() throws PersistenceException {
		
		List<CQSAPIStats> statsList = getCQSAPIStats();
		String localDataCenter = CMBProperties.getInstance().getCMBDataCenter();
		List <String> dataCenterList = new ArrayList<String>();
		dataCenterList.add(localDataCenter);
		Set <String> dataCenterNameSet = new HashSet<String>();
		
		for (CQSAPIStats currentCQSAPIStat : statsList) {
			if ((currentCQSAPIStat.getDataCenter()!=null) && (!currentCQSAPIStat.getDataCenter().equals(localDataCenter))) {
				dataCenterNameSet.add(currentCQSAPIStat.getDataCenter());
			}
		}

		dataCenterList.addAll(dataCenterNameSet);
		return dataCenterList;
	}
	
	public static List<CQSAPIStats> getCNSAPIStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CQSAPIStats> cqsAPIStatsList = getCNSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList) {
			if (currentCQSAPIStats.getDataCenter().equals(dataCenter)) {
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		
		return cqsAPIStatsByDataCenterList;
	}
	
	public static List<CQSAPIStats> getCQSAPIStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CQSAPIStats> cqsAPIStatsList = getCQSAPIStats();
		List<CQSAPIStats> cqsAPIStatsByDataCenterList = new ArrayList<CQSAPIStats>();
		for (CQSAPIStats currentCQSAPIStats: cqsAPIStatsList) {
			if (currentCQSAPIStats.getDataCenter().equals(dataCenter)) {
				cqsAPIStatsByDataCenterList.add(currentCQSAPIStats);
			}
		}
		return cqsAPIStatsByDataCenterList;
	}
	
	public static List<CQSAPIStats> getCQSAPIStats() throws PersistenceException {
		ResultSet resultSet = DurablePersistenceFactory.getInstance().getSession().execute(QueryBuilder.select().all().from("cqs", CQS_API_SERVERS));
		List<CQSAPIStats> statsList = new ArrayList<CQSAPIStats>();
			
		for (Row row : resultSet.all()) {

			CQSAPIStats stats = new CQSAPIStats();
			stats.setIpAddress(row.getString("host"));

			if (!row.isNull("timestamp") ) {
				stats.setTimestamp(Long.parseLong(row.getString("timestamp")));
			}

			if (!row.isNull("jmxport")) {
				stats.setJmxPort(Long.parseLong(row.getString("jmxport")));
			}

			if (!row.isNull("port")) {
				stats.setLongPollPort(Long.parseLong(row.getString("port")));
			}

			if (!row.isNull("data_center")) {
				stats.setDataCenter(row.getString("data_center"));
			}

			if (!row.isNull("service_url")) {
				stats.setServiceUrl(row.getString("service_url"));
			}

			if (!row.isNull("redis_server_list")) {
				stats.setRedisServerList(row.getString("redis_server_list"));
			}

			if (stats.getIpAddress().contains(":")) {
				statsList.add(stats);
			}
		}

		return statsList;
	}
}
