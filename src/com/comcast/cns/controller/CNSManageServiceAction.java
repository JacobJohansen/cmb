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
package com.comcast.cns.controller;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSPopulator;
import com.comcast.cns.io.CNSWorkerStatsPopulator;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.tools.CNSWorkerMonitorMBean;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.CNSWorkerStatWrapper;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.log4j.Logger;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
/**
 * Subscribe action
 * @author bwolf
 *
 */
public class CNSManageServiceAction extends CNSAction {

	private static Logger logger = Logger.getLogger(CNSManageServiceAction.class);
	
	public static final String CNS_API_SERVERS = "CNSAPIServers";
	public static final String CNS_WORKERS = "CNSWorkers";
	public CNSManageServiceAction() {
		super("ManageService");
	}
	
    @Override
    public boolean isActionAllowed(User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    	return true;
    }

	/**
	 * Manage cns service and workers
	 * @param user the user for whom we are subscribing.
	 * @param asyncContext
	 */
	@Override
	public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

        HttpServletRequest request = (HttpServletRequest)asyncContext.getRequest();
        HttpServletResponse response = (HttpServletResponse)asyncContext.getResponse();
		
		String task = request.getParameter("Task");

		if (task == null || task.equals("")) {
			logger.error("event=cns_manage_service error_code=missing_parameter_task");
			throw new CMBException(CNSErrorCodes.MissingParameter,"Request parameter Task missing.");
		}

		String host = request.getParameter("Host");
		//for some task, Host is mandatory. Check it.
		if (!task.equals("ClearAPIStats") && (!task.equals("StartWorker")) && (!task.equals("StopWorker")) && (host == null || host.equals(""))) {
			logger.error("event=cns_manage_service error_code=missing_parameter_host");
			throw new CMBException(CNSErrorCodes.MissingParameter,"Request parameter Host missing.");
		}

		if (task.equals("ClearWorkerQueues")) {

			ResultSet rows = DurablePersistenceFactory.getInstance().getSession().execute(QueryBuilder.select().all().from("CNS", CNS_WORKERS));
			List<CNSWorkerStats> statsList = new ArrayList<CNSWorkerStats>();

			for (Row row : rows.all()) {

				CNSWorkerStats stats = new CNSWorkerStats();
				String endpoint = row.getString("host");
				stats.setIpAddress(endpoint);

				if (!row.isNull("producerTimestamp")) {
					stats.setProducerTimestamp(Long.parseLong(row.getString("producerTimestamp")));
				}
				if (!row.isNull("consumerTimestamp")) {
					stats.setConsumerTimestamp(Long.parseLong(row.getString("consumerTimestamp")));
				}
				if (!row.isNull("jmxport")) {
					stats.setJmxPort(Long.parseLong(row.getString("jmxport")));
				}
				if (!row.isNull("mode")) {
					stats.setMode(row.getString("mode"));
				}
			}

			for (CNSWorkerStats stats : statsList) {

				if (stats.getIpAddress().equals(host) && stats.getJmxPort() > 0) {

					JMXConnector jmxConnector = null;
					String url = null;

					try {

						long port = stats.getJmxPort();
						url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

						JMXServiceURL serviceUrl = new JMXServiceURL(url);
						jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

						MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
						ObjectName cnsWorkerMonitor = new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");
						CNSWorkerMonitorMBean mbeanProxy = JMX.newMBeanProxy(mbeanConn, cnsWorkerMonitor,	CNSWorkerMonitorMBean.class, false);

						mbeanProxy.clearWorkerQueues();

						String res = CNSWorkerStatsPopulator.getGetManageWorkerResponse();	
						response.getWriter().println(res);

						return true;

					} finally {

						if (jmxConnector != null) {
							jmxConnector.close();
						}
					}
				}
			}

			throw new CMBException(CMBErrorCodes.NotFound, "Cannot clear worker queues: Host " + host + " not found.");

		} else if (task.equals("RemoveWorkerRecord")) {
			DurablePersistenceFactory.getInstance().getSession().execute(QueryBuilder.delete().all().from("CNS", CNS_WORKERS).where(QueryBuilder.eq("host", host)));
			String out = CNSPopulator.getResponseMetadata();
	        writeResponse(out, response);
			return true;
			
		} else if (task.equals("ClearAPIStats")) {
			
            CMBControllerServlet.initStats();
            String out = CNSPopulator.getResponseMetadata();
	        writeResponse(out, response);
	    	return true;

		} else if (task.equals("RemoveRecord")) {
			DurablePersistenceFactory.getInstance().getSession().execute(QueryBuilder.delete().all().from("CNS", CNS_API_SERVERS).where(QueryBuilder.eq("host", host)));
			String out = CNSPopulator.getResponseMetadata();
	        writeResponse(out, response);
			return true;
			
		} else if (task.equals("StartWorker")||task.equals("StopWorker")) {
			String dataCenter = request.getParameter("DataCenter");
			if(task.equals("StartWorker")){
				CNSWorkerStatWrapper.startWorkers(dataCenter);
			} else {
				CNSWorkerStatWrapper.stopWorkers(dataCenter);
			}
			String out = CNSPopulator.getResponseMetadata();
	        writeResponse(out, response);
			return true;
			
		} else {
			logger.error("event=cns_manage_service error_code=invalid_task_parameter valid_values=ClearWorkerQueues,RemoveWorkerRecord,RemoveRecord,ClearAPIStats");
			throw new CMBException(CNSErrorCodes.InvalidParameterValue,"Request parameter Task missing is invalid. Valid values are ClearWorkerQueues, RemoveWorkerRecord, RemoveRecord, ClearAPIStats.");
		}
	}
}
