package com.comcast.cns.persistence;

import com.comcast.cns.model.CNSTopicAttributes;

public interface ICNSTopicAttributesPersistence {
    /**
     * setTopicAttributes
     * @param topicArn
     * @return
     * @throws Exception
     */
    public void setTopicAttributes(CNSTopicAttributes topicAttributes, String topicArn) throws Exception;

    /**
     * getTopicAttributes
     * @param topicArn
     * @return CNSTopicAttributes
     * @throws Exception
     */
    public CNSTopicAttributes getTopicAttributes(String topicArn) throws Exception;


    public long getTopicStats(String topicArn, String status);
}
