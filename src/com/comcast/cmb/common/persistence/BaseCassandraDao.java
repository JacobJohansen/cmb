package com.comcast.cmb.common.persistence;

import com.datastax.driver.core.*;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseCassandraDao <T> {
    protected Session session;
    protected BaseCassandraDao(Session session) {
        this.session = session;
    }

    public void save(List<Statement> statements) {
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        statements.forEach(statement -> resultSetFutures.add(session.executeAsync(statement)));
        for (ResultSetFuture result: resultSetFutures) {
            while (!result.isDone()){
                Thread.yield();
            }
            if (!result.getUninterruptibly().wasApplied()) {
                throw new RuntimeException("failed to write to cassandra");
            }
        }
    }

    public void save(Statement statement) {
        ResultSetFuture resultSetFuture = session.executeAsync(statement);
        while (!resultSetFuture.isDone()){
            Thread.yield();
        }
        if (!resultSetFuture.getUninterruptibly().wasApplied()) {
            throw new RuntimeException("failed to write to cassandra");
        }

    }

    protected List<T> find(List<? extends Statement> statements) {
        List<T> results = new ArrayList<>();
        List<ResultSetFuture> resultSetFutures = Lists.newArrayList();
        statements.forEach(statement -> resultSetFutures.add(session.executeAsync(statement)));
        for (ResultSetFuture result: resultSetFutures) {
            while (!result.isDone()){
                Thread.yield();
            }
            ResultSet resultSet = result.getUninterruptibly();
            if (!resultSet.wasApplied()) {
                throw new RuntimeException("failed to query from cassandra");
            }

            results.addAll(resultSet.all().stream().map(this::convertToInstance).collect(Collectors.toList()));
        }
        return results;
    }

    protected List<T> findAll(Statement statement) {
        ResultSetFuture resultSetFuture = session.executeAsync(statement);

        while (!resultSetFuture.isDone()){
            Thread.yield();
        }

        ResultSet resultSet = resultSetFuture.getUninterruptibly();
        if (!resultSet.wasApplied()) {
            throw new RuntimeException("failed to query from cassandra");
        }

        return resultSet.all().stream().map(this::convertToInstance).collect(Collectors.toList());
    }

    protected List<T> find(Statement statement, String pagingState, Integer pageSize) {
        List<T> results = new ArrayList<>();
        PagingState paging = null;
        final PagingState currentPagingState;

        if(pagingState != null) {
            paging = PagingState.fromString(pagingState);
        }

        if(paging != null) {
            statement.setPagingState(paging);
        }

        if (pageSize != null) {
            statement.setFetchSize(pageSize);
        }

        ResultSetFuture resultSetFuture = session.executeAsync(statement);

        while (!resultSetFuture.isDone()){
            Thread.yield();
        }

        ResultSet resultSet = resultSetFuture.getUninterruptibly();
        if (!resultSet.wasApplied()) {
            throw new RuntimeException("failed to query from cassandra");
        }

        if(!resultSet.isFullyFetched()) {
            currentPagingState = resultSet.getExecutionInfo().getPagingState();
        } else {
            currentPagingState = null;
        }
        results.addAll(resultSet.all().stream().map(x->convertToInstance(x, currentPagingState)).collect(Collectors.toList()));

        return results;
    }

    protected T findOne(Statement statement) {
        statement.setFetchSize(1);
        ResultSet result = session.execute(statement);

        return convertToInstance(result.one());
    }

    protected abstract T convertToInstance(Row row);

    protected T convertToInstance(Row row, PagingState pagingState) {
        T model = convertToInstance(row);
        if (model instanceof ICassandraPaging) {
           ((ICassandraPaging) model).setNextPage(pagingState.toString());
        }
        return model;
    }
}
