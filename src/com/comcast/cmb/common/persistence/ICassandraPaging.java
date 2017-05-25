package com.comcast.cmb.common.persistence;

public interface ICassandraPaging {
    String getNextPage();
    void setNextPage(String nextPage);
}
