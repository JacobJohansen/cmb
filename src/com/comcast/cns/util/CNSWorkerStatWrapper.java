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
package com.comcast.cns.util;

import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.tools.CNSWorkerMonitor;
import com.comcast.cns.tools.CNSWorkerMonitorMBean;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.log4j.Logger;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

public class CNSWorkerStatWrapper {
	
	private static Logger logger = Logger.getLogger(CNSWorkerStatWrapper.class);
	private static final String CNS_WORKERS = "cns_workers";
	public static List<CNSWorkerStats> getCassandraWorkerStats() throws PersistenceException {
		List<CNSWorkerStats> statsList = new ArrayList<CNSWorkerStats>();
		ResultSet resultSet = DurablePersistenceFactory.getInstance().getSession().execute(
			QueryBuilder.select().all().from("cns", CNS_WORKERS)
		);


		for (Row row : resultSet.all()) {

			CNSWorkerStats stats = new CNSWorkerStats();

			stats.setIpAddress(row.getString("host"));

			if (!row.isNull("producer_timestamp")) {
				stats.setProducerTimestamp(Long.parseLong(row.getString("producer_timestamp")));
			}

			if (!row.isNull("consumer_timestamp")) {
				stats.setConsumerTimestamp(Long.parseLong(row.getString("consumer_timestamp")));
			}

			if (!row.isNull("jmxport")) {
				stats.setJmxPort(Long.parseLong(row.getString("jmxport")));
			}

			if (!row.isNull("mode")) {
				stats.setMode(row.getString("mode"));
			}

			if (!row.isNull("data_center")) {
				stats.setDataCenter(row.getString("data_center"));
			}
			statsList.add(stats);
		}


		return statsList;
	}

	public static List<CNSWorkerStats> getCassandraWorkerStatsByDataCenter(String dataCenter) throws PersistenceException {
		List<CNSWorkerStats> cnsWorkerStatsList = getCassandraWorkerStats();
		List<CNSWorkerStats> cnsWorkerStatsByDataCenterList = new ArrayList<CNSWorkerStats>();
		for (CNSWorkerStats currentWorkerStats: cnsWorkerStatsList) {
			if (currentWorkerStats.getDataCenter().equals(dataCenter)) {
				cnsWorkerStatsByDataCenterList.add(currentWorkerStats);
			}
		}
		return cnsWorkerStatsByDataCenterList;
	}

	private static void callOperation(String operation, List<CNSWorkerStats> cnsWorkerStats) throws Exception{

		//register JMX Bean
		
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
		ObjectName name = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");

		if (!mbs.isRegistered(name)) {
			mbs.registerMBean(CNSWorkerMonitor.getInstance(), name);
		}

		if ((operation!=null)&&(operation.equals("startWorker")||operation.equals("stopWorker"))) {
			JMXConnector jmxConnector = null;
			String url = null;
			String host = null;
			long port = 0;
			for (CNSWorkerStats stats:cnsWorkerStats) {
				try {

					host = stats.getIpAddress(); 
					port = stats.getJmxPort();
					url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

					JMXServiceURL serviceUrl = new JMXServiceURL(url);
					jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

					MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
					ObjectName cnsWorkerMonitor = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");
					CNSWorkerMonitorMBean mbeanProxy = JMX.newMBeanProxy(mbeanConn, cnsWorkerMonitor, CNSWorkerMonitorMBean.class, false);
					if (operation.equals("startWorker")) {
						mbeanProxy.startCNSWorkers();
					} else {
						mbeanProxy.stopCNSWorkers();
					}
				} catch (Exception e) {
					logger.error("event=error_in_"+operation+" Hose:"+host+" port:"+port+"Exception: "+e);
					String operationString = null;
					if(operation.equals("startWorker")){
						operationString = "start";
					} else {
						operationString = "stop";
					}
					throw new CMBException(CMBErrorCodes.InternalError, "Cannot " + operationString + " CNS workers");
				} finally {

					if (jmxConnector != null) {
						jmxConnector.close();
					}
				}
			}
		}
	}

	public static void stopWorkers(List<CNSWorkerStats> cnsWorkersList) throws Exception{
		callOperation("stopWorker", cnsWorkersList);
	}

	public static void startWorkers(List<CNSWorkerStats> cnsWorkersList)throws Exception{
		callOperation("startWorker", cnsWorkersList);
	}

	public static void startWorkers(String dataCenter) throws Exception{
		List<CNSWorkerStats> cnsWorkerStarts = getCassandraWorkerStatsByDataCenter(dataCenter);
		callOperation("startWorker", cnsWorkerStarts);
	}

	public static void stopWorkers(String dataCenter) throws Exception{
		List<CNSWorkerStats> cnsWorkerStarts = getCassandraWorkerStatsByDataCenter(dataCenter);
		callOperation("stopWorker", cnsWorkerStarts);
	}
}
