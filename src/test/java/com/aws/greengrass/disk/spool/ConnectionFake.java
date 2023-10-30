/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Delegate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

public class ConnectionFake implements Connection {

    // exceptions to throw on executeUpdate
    private final Queue<SQLException> throwOnUpdate = new LinkedList<>();

    @Setter
    @Delegate(excludes = ConnectionExcludes.class)
    private volatile Connection connection;

    public void addExceptionOnUpdate(SQLException exception) {
        throwOnUpdate.add(exception);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql, autoGeneratedKeys));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql, columnIndexes));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return new PreparedStatementFake(connection.prepareStatement(sql, columnNames));
    }

    @RequiredArgsConstructor
    class PreparedStatementFake implements PreparedStatement {

        @Delegate(excludes = PreparedStatementExcludes.class)
        private final PreparedStatement statement;

        @Override
        public int executeUpdate() throws SQLException {
            // for testing, choose to intercept update and throw an exception
            SQLException err = throwOnUpdate.poll();
            if (err != null) {
                throw err;
            }
            return statement.executeUpdate();
        }
    }

    @SuppressWarnings("PMD.UseVarargs")
    interface ConnectionExcludes {
        PreparedStatement prepareStatement(String sql) throws SQLException;
        PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException;
        PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException;
        PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException;
        PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException;
        PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException;
    }

    interface PreparedStatementExcludes {
        int executeUpdate() throws SQLException;
    }
}