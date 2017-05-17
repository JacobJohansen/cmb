package com.comcast.cns.model;


import java.util.regex.Matcher;

public enum CnsSubscriptionProtocol { http, https, email, email_json, cqs, sqs, redis;
    /**
     *
     * @return true if this protocol supports subscription confirmation
     */

    public boolean canConfirmSubscription() {
        switch (this) {
            case redis:
                return false;
            default:
                return true;
        }
    }

    /**
     *
     * @param endpoint
     * @return true if endpoint is correctly formatted given the protocol
     */
    public boolean isValidEndpoint(String endpoint) {

        switch (this) {

            case https:
                if (!endpoint.substring(0, 8).equals("https://")) {
                    return false;
                }
                break;
            case http:
                if (!endpoint.substring(0, 7).equals("http://")) {
                    return false;
                }
                break;
            case email:
            case email_json:
                if (!endpoint.contains("@")) {
                    return false;
                }
                break;
            case redis:
                Matcher m = com.comcast.cns.util.Util.redisPubSubPattern.matcher(endpoint);
                return m.matches();
            case sqs:
                if (!com.comcast.cqs.util.Util.isValidQueueArn(endpoint) && !com.comcast.cqs.util.Util.isValidQueueUrl(endpoint)) {
                    return false;
                }
                break;
            case cqs:
                if (!com.comcast.cqs.util.Util.isValidQueueArn(endpoint) && !com.comcast.cqs.util.Util.isValidQueueUrl(endpoint)) {
                    return false;
                }
                break;
        }
        return true;
    }
}
