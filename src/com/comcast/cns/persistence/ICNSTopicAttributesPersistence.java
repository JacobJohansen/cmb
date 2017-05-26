package com.comcast.cns.persistence;

import com.comcast.cns.model.CNSTopicAttributes;

public interface ICNSTopicAttributesPersistence {
    /**
     * setTopicAttributes
     * @param topicArn
     * @return
     * @throws Exception
     */
    void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception;

    /**
     * getTopicAttributes
     * @param topicArn
     * @return CNSTopicAttributes
     * @throws Exception
     */
    CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception;

    void removeTopicAttributes(String topicArn);

    long getTopicStats(String topicArn, String status);

    void incrementCounter(String topicArn, String status, int count);

    void incrementCounter(String topicArn, String status);

    void decrementCounter(String topicArn, String status);

    void decrementCounter(String topicArn, String status, int count);
}
