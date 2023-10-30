/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import lombok.Getter;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A {@link DiskSpoolDAO} with customizable behavior of {@link Connection} or {@link Statement},
 * via {@link ConnectionFake}.
 */
public class DiskSpoolDAOFake extends DiskSpoolDAO {

    @Getter
    private final ConnectionFake connection = new ConnectionFake();

    public DiskSpoolDAOFake(Path rootDir) {
        super(rootDir.resolve("spooler.db"));
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public Connection getDbInstance() throws SQLException {
        Connection conn = super.getDbInstance();
        connection.setConnection(conn);
        return connection;
    }
}
