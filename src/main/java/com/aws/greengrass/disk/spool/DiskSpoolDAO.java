/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.NucleusPaths;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

import static com.aws.greengrass.disk.spool.DiskSpool.PERSISTENCE_SERVICE_NAME;
import static java.nio.file.Files.deleteIfExists;

public class DiskSpoolDAO {
    private final String url;
    protected static final String DATABASE_CONNECTION_URL = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";
    private final Path databasePath;
    private static final int MAX_TRY = 3;
    private static final int SQLITE_CORRUPT_CODE = 11;
    private static final Logger logger = LogManager.getLogger(DiskSpoolDAO.class);

    /**
     * This method will construct the database path.
     * @param paths The path to the working directory
     * @throws IOException when fails to set up the database
     */
    @Inject
    public DiskSpoolDAO(NucleusPaths paths) throws IOException {
        databasePath = paths.workPath(PERSISTENCE_SERVICE_NAME).resolve(DATABASE_FILE_NAME);
        url = String.format(DATABASE_CONNECTION_URL, databasePath);

        try {
            setUpDatabase();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    /**
     * This method will query the existing database for the existing queue of MQTT request Ids
     * and return them in order.
     * @return ordered list of the existing ids in the persistent queue
     * @throws SQLException when fails to get SpoolMessage IDs
     */
    public Iterable<Long> getAllSpoolMessageIds() throws SQLException {
        List<Long> currentIds;
        String query = "SELECT message_id FROM spooler;";
        try (Connection conn = getDbInstance();
             PreparedStatement pstmt = conn.prepareStatement(query);
             ResultSet rs = executeQueryWithRetries(pstmt)) {
            currentIds = getIdsFromRs(rs);
        } catch (SQLException e) {
            checkAndHandleCorruption(e);
            throw e;
        }
        return currentIds;
    }

    private List<Long> getIdsFromRs(ResultSet rs) throws SQLException {
        List<Long> currentIds = new ArrayList<>();
        while (rs.next()) {
            currentIds.add(rs.getLong("message_id"));
        }
        return currentIds;
    }

    /**
     * This method will query a SpoolMessage and return it given an id.
     * @param messageId the id of the SpoolMessage
     * @return SpoolMessage
     * @throws SQLException when fails to get a SpoolMessage by id
     */
    public SpoolMessage getSpoolMessageById(long messageId) throws SQLException {
        String query = "SELECT retried, topic, qos, retain, payload, userProperties, messageExpiryIntervalSeconds, "
                + "correlationData, responseTopic, payloadFormat, contentType FROM spooler WHERE message_id = ?;";
        try (Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setLong(1, messageId);
            try (ResultSet rs = executeQueryWithRetries(pstmt)) {
                return getSpoolMessageFromRs(messageId, rs);
            }
        } catch (SQLException e) {
            checkAndHandleCorruption(e);
            throw e;
        }
    }

    /**
     * This method will insert a SpoolMessage into the database.
     * @param message instance of SpoolMessage
     * @throws SQLException when fails to insert SpoolMessage in the database
     */
    @SuppressWarnings("PMD.ExceptionAsFlowControl")
    public void insertSpoolMessage(SpoolMessage message) throws SQLException {
        String sqlString =
                "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload, userProperties, "
                        + "messageExpiryIntervalSeconds, correlationData, responseTopic, payloadFormat, contentType) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
        Publish request = message.getRequest();
        try (Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(sqlString)) {
            pstmt.setLong(1, message.getId());
            pstmt.setInt(2, message.getRetried().get());

            // MQTT 3 & 5 fields
            pstmt.setString(3, request.getTopic());
            pstmt.setInt(4, request.getQos().getValue());
            pstmt.setBoolean(5, request.isRetain());
            pstmt.setBytes(6, request.getPayload());

            if (request.getUserProperties() == null) {
                pstmt.setNull(7, Types.NULL);
            } else {
                try {
                    pstmt.setBytes(7, userPropertiesToByteArray(request.getUserProperties()));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
            if (request.getMessageExpiryIntervalSeconds() == null) {
                pstmt.setNull(8, Types.NULL);
            } else {
                pstmt.setLong(8, request.getMessageExpiryIntervalSeconds());
            }
            if (request.getCorrelationData() == null) {
                pstmt.setNull(9, Types.NULL);
            } else {
                pstmt.setBytes(9, request.getCorrelationData());
            }
            if (request.getResponseTopic() == null) {
                pstmt.setNull(10, Types.NULL);
            } else {
                pstmt.setString(10, request.getResponseTopic());
            }
            if (request.getPayloadFormat() == null) {
                pstmt.setNull(11, Types.NULL);
            } else {
                pstmt.setInt(11, request.getPayloadFormat().getValue());
            }
            if (request.getContentType() == null) {
                pstmt.setNull(12, Types.NULL);
            } else {
                pstmt.setString(12, request.getContentType());
            }
            executeUpdateWithRetries(pstmt);
        } catch (SQLException e) {
            checkAndHandleCorruption(e);
            throw e;
        }
    }

    /**
     * This method will remove a SpoolMessage from the database given its id.
     * @param messageId the id of the SpoolMessage
     * @throws SQLException when fails to remove a SpoolMessage by id
     */
    public void removeSpoolMessageById(Long messageId) throws SQLException {
        String deleteSQL = "DELETE FROM spooler WHERE message_id = ?;";
        try (Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(deleteSQL)) {
            pstmt.setLong(1, messageId);
            executeUpdateWithRetries(pstmt);
        }
    }

    /**
     * This method creates a connection instance of the SQLite database.
     * @return Connection for SQLite database instance
     * @throws SQLException When fails to get Database Connection
     */
    public Connection getDbInstance() throws SQLException {
        return DriverManager.getConnection(url);
    }

    private void setUpDatabase() throws SQLException {
        String tableCreationString = "CREATE TABLE IF NOT EXISTS spooler ("
                + "message_id INTEGER PRIMARY KEY, "
                + "retried INTEGER NOT NULL, "
                + "topic STRING NOT NULL,"
                + "qos INTEGER NOT NULL,"
                + "retain BOOLEAN,"
                + "payload BLOB,"
                + "userProperties BLOB,"
                + "messageExpiryIntervalSeconds INTEGER,"
                + "correlationData BLOB,"
                + "responseTopic STRING,"
                + "payloadFormat INTEGER,"
                + "contentType STRING"
                + ");";
        DriverManager.registerDriver(new org.sqlite.JDBC());
        try (Connection conn = getDbInstance();
            Statement st = conn.createStatement()) {
            //create new table if table doesn't exist
            st.executeUpdate(tableCreationString);
        }
    }

    private SpoolMessage getSpoolMessageFromRs(long messageId, ResultSet rs) throws SQLException {
        if (rs.next()) {
            Publish request = Publish.builder()
                    .qos(QOS.fromInt(rs.getInt("qos")))
                    .retain(rs.getBoolean("retain"))
                    .topic(rs.getString("topic"))
                    .payload(rs.getBytes("payload"))
                    .payloadFormat(Publish.PayloadFormatIndicator.fromInt(rs.getInt("payloadFormat")))
                    .messageExpiryIntervalSeconds(rs.getLong("messageExpiryIntervalSeconds"))
                    .responseTopic(rs.getString("responseTopic"))
                    .correlationData(rs.getBytes("correlationData"))
                    .contentType(rs.getString("contentType"))
                    .userProperties(byteArrayToUserProperties(rs.getBytes("userProperties"))).build();

            return SpoolMessage.builder()
                    .id(messageId)
                    .retried(new AtomicInteger(rs.getInt("retried")))
                    .request(request).build();
        } else {
            return null;
        }
    }

    private byte[] userPropertiesToByteArray(List<UserProperty> userProps) throws IOException {
        byte[] serialized;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(userProps);
            oos.flush();
            serialized = baos.toByteArray();
        }
        return serialized;
    }

    private List<UserProperty> byteArrayToUserProperties(byte[] userProperties) {
        if (userProperties == null || userProperties.length == 0) {
            return null;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(userProperties))) {
            return (List<UserProperty>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            // TODO: Add logging and exception handling
        }
        return null;
    }

    private void executeUpdateWithRetries(PreparedStatement statement) throws SQLException {
        int count = 0;
        while (true) {
            try {
                statement.executeUpdate();
                break;
            } catch (SQLTransientException e) {
                count++;
                if (count == MAX_TRY) {
                    throw e;
                }
            }
        }
    }

    private ResultSet executeQueryWithRetries(PreparedStatement statement) throws SQLException {
        int count = 0;
        while (true) {
            try {
                return statement.executeQuery();
            } catch (SQLTransientException e) {
                count++;
                if (count == MAX_TRY) {
                    throw e;
                }
            }
        }
    }

    void checkAndHandleCorruption(SQLException e) throws SQLException {
        if (e.getErrorCode() == SQLITE_CORRUPT_CODE) {
            logger.atWarn().log(String.format("Database %s is corrupted", databasePath));
            try {
                deleteIfExists(databasePath);
            } catch (IOException e2) {
                throw new SQLException(e2);
            }
            setUpDatabase();
        }
    }
}
