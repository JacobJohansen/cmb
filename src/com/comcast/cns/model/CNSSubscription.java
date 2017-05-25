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
package com.comcast.cns.model;

import com.comcast.cmb.common.persistence.ICassandraPaging;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;

import java.util.Date;
import java.util.UUID;


/**
 * Represents a Subscription
 *
 * Class is not thread-safe. Caller must ensure thread safety
 */
public class CNSSubscription implements ICassandraPaging {
	private String arn;
	private String topicArn;
	private UUID userId;
	private CnsSubscriptionProtocol protocol;
	private String endpoint;
	private Date requestDate;
	private Date confirmDate;
	private boolean confirmed;
	private String token;
	private boolean authenticateOnUnsubscribe;
	private CNSSubscriptionDeliveryPolicy deliveryPolicy;
	private Boolean rawMessageDelivery = false;
	private String nextPage;

	public CNSSubscription() {

	}

	public CNSSubscription(String endpoint, CnsSubscriptionProtocol protocol, String topicArn, UUID userId) {

		this.endpoint = endpoint;
		this.protocol = protocol;
		this.topicArn = topicArn;
		this.userId = userId;

		this.token = UUID.randomUUID().toString();
		this.requestDate = new Date();
		this.confirmed = false;
		this.authenticateOnUnsubscribe = false;
		this.setRawMessageDelivery(false);
	}

	public CNSSubscription(String arn) {
		this.arn = arn;
	}

	public String getArn() {
		return arn;
	}

	public void setArn(String arn) {
		this.arn = arn;
	}

	public String getTopicArn() {
		return topicArn;
	}

	public void setTopicArn(String topicArn) {
		this.topicArn = topicArn;
	}

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	public CnsSubscriptionProtocol getProtocol() {
		return protocol;
	}

	public void setProtocol(CnsSubscriptionProtocol protocol) {
		this.protocol = protocol;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public Date getRequestDate() {
		return requestDate;
	}

	public void setRequestDate(Date requestDate) {
		this.requestDate = requestDate;
	}

	public Date getConfirmDate() {
		return confirmDate;
	}

	public void setConfirmDate(Date confirmDate) {
		this.confirmDate = confirmDate;
	}

	public boolean isConfirmed() {
		return confirmed;
	}

	public void setConfirmed(boolean confirmed) {
		this.confirmed = confirmed;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public boolean isAuthenticateOnUnsubscribe() {
		return authenticateOnUnsubscribe;
	}

	public void setAuthenticateOnUnsubscribe(boolean authenticateOnUnsubscribe) {
		this.authenticateOnUnsubscribe = authenticateOnUnsubscribe;
	}

	public CNSSubscriptionDeliveryPolicy getDeliveryPolicy() {
		return deliveryPolicy;
	}

	public void setDeliveryPolicy(CNSSubscriptionDeliveryPolicy deliveryPolicy) {
		this.deliveryPolicy = deliveryPolicy;
	}

	public Boolean getRawMessageDelivery() {
		return rawMessageDelivery;
	}

	public void setRawMessageDelivery(Boolean rawMessageDelivery) {
		this.rawMessageDelivery = rawMessageDelivery;
	}

	public boolean isTokenExpired() {
		if ((new Date()).getTime() - getRequestDate().getTime() > 3 * 24 * 60 * 60 * 1000) {
			return true;
		}

		return false;
	}

	public String getNextPage() {
		return nextPage;
	}

	public void setNextPage(String nextPage) {
		this.nextPage = nextPage;
	}

	/**
	 * Verify this instance of subscription
	 * @throws CMBException if not valid
	 */
	public void checkIsValid() throws CMBException {

		if (arn == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set arn for subscription");
		}
		
		if (!com.comcast.cns.util.Util.isValidSubscriptionArn(arn)) {
			throw new CMBException(CMBErrorCodes.InternalError, "Invalid subscription arn");
		}

		if (topicArn == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set topic arn for subscription");
		}
		
		if (!com.comcast.cns.util.Util.isValidTopicArn(topicArn)) {
			throw new CMBException(CMBErrorCodes.InternalError, "Invalid topic arn");
		}
		
		if (userId == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set user id for subscription");
		}
		
		if (protocol == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set protocol for subscription");
		}
		
		if (endpoint == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Must set endpoint for subscription");
		}
		
		if (confirmed && confirmDate == null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Bad confirmation data");
		}
		
		if (!confirmed && confirmDate != null) {
			throw new CMBException(CMBErrorCodes.InternalError, "Bad confirmation data");
		}
	}

	@Override
	public String toString() {
		return "arn=" + getArn() + " topicArn=" + getTopicArn() + " user_id=" + getUserId() + 
				" protocol=" + getProtocol() + " endpoint=" + getEndpoint() + 
				" request_date=" + getRequestDate() + " confirm_date=" + getConfirmDate() + 
				" confirmed=" + isConfirmed() + " token=" + getToken() + 
				" rawMessageDelivery=" + getRawMessageDelivery();
	}

	@Override public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof CNSSubscription)) return false;

		CNSSubscription that = (CNSSubscription) o;

		if (confirmed != that.confirmed) return false;
		if (authenticateOnUnsubscribe != that.authenticateOnUnsubscribe) return false;
		if (arn != null ? !arn.equals(that.arn) : that.arn != null) return false;
		if (topicArn != null ? !topicArn.equals(that.topicArn) : that.topicArn != null) return false;
		if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
		if (protocol != that.protocol) return false;
		if (endpoint != null ? !endpoint.equals(that.endpoint) : that.endpoint != null) return false;
		if (requestDate != null ? !requestDate.equals(that.requestDate) : that.requestDate != null) return false;
		if (confirmDate != null ? !confirmDate.equals(that.confirmDate) : that.confirmDate != null) return false;
		if (token != null ? !token.equals(that.token) : that.token != null) return false;
		if (deliveryPolicy != null ? !deliveryPolicy.equals(that.deliveryPolicy) : that.deliveryPolicy != null)
			return false;
		return rawMessageDelivery != null ? rawMessageDelivery.equals(that.rawMessageDelivery) : that
			.rawMessageDelivery == null;
	}

	@Override public int hashCode() {
		int result = arn != null ? arn.hashCode() : 0;
		result = 31 * result + (topicArn != null ? topicArn.hashCode() : 0);
		result = 31 * result + (userId != null ? userId.hashCode() : 0);
		result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
		result = 31 * result + (endpoint != null ? endpoint.hashCode() : 0);
		result = 31 * result + (requestDate != null ? requestDate.hashCode() : 0);
		result = 31 * result + (confirmDate != null ? confirmDate.hashCode() : 0);
		result = 31 * result + (confirmed ? 1 : 0);
		result = 31 * result + (token != null ? token.hashCode() : 0);
		result = 31 * result + (authenticateOnUnsubscribe ? 1 : 0);
		result = 31 * result + (deliveryPolicy != null ? deliveryPolicy.hashCode() : 0);
		result = 31 * result + (rawMessageDelivery != null ? rawMessageDelivery.hashCode() : 0);
		return result;
	}
}
