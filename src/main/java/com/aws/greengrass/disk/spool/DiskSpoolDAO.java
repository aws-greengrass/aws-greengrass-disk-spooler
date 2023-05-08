/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.NucleusPaths;

import javax.inject.Inject;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aws.greengrass.disk.spool.DiskSpool.PERSISTENCE_SERVICE_NAME;

public class DiskSpoolDAO {
    private final String url;
    protected static final String DATABASE_FORMAT = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";

    @Inject
    public DiskSpoolDAO(NucleusPaths paths) throws IOException {
        Path databasePath = paths.workPath(PERSISTENCE_SERVICE_NAME).resolve(DATABASE_FILE_NAME);
        url = String.format(DATABASE_FORMAT, databasePath);
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
     */
    public Iterable<Long> getAllSpoolStorageDocumentIds() throws IOException {
        List<Long> currentIds;
        String query = "SELECT message_id FROM spooler;";
        try(Connection conn = getDbInstance();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)) {
            currentIds = getIdsFromRs(rs);
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return currentIds;
    }

    private List<Long> getIdsFromRs(ResultSet rs) throws SQLException {
        List<Long> currentIds = new ArrayList<>();
        if (!rs.next()) {
            //return empty list
            return currentIds;
        } else {
            //if not empty we create return iterable of the contents
            do {
                Long id = rs.getLong("message_id");
                currentIds.add(id);
            } while (rs.next());
        }
        return currentIds;
    }

    /**
     * This method will query a SpoolStorageDocument and return it given an id.
     * @param messageId the id of the SpoolStorageDocument
     * @return SpoolStorageDocument
     */
    public SpoolMessage getSpoolStorageDocumentById(long messageId) throws SQLException {
        String query = "SELECT retried, topic, qos, retain, payload, userProperties, messagesExpiryIntervalSeconds, "
                + "corelationData, responseTopic, payloadFormat, contentType FROM spooler WHERE message_id = ?;";
        try(Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setLong(1, messageId);
            try(ResultSet rs = pstmt.executeQuery()) {
                return getSpoolMessageFromRs(messageId, rs);
            }
        }
    }

    /**
     * This method will insert a SpoolStorageDocument into the database.
     * @param message instance of SpoolStorageDocument
     */
    public void insertSpoolStorageDocument(SpoolMessage message) throws SQLException {
        String sqlString =
                "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload, userProperties, "
                        + "messagesExpiryIntervalSeconds, corelationData, responseTopic, payloadFormat, contentType) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
        Publish request = message.getRequest();
        try(Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(sqlString)) {
            pstmt.setLong(1, message.getId());
            pstmt.setInt(2, message.getRetried().get());
            pstmt.setString(3, request.getTopic());
            pstmt.setInt(4, request.getQos().getValue());
            pstmt.setBoolean(5, request.isRetain());
            pstmt.setBytes(6, request.getPayload());
            pstmt.setBytes(7, userPropertiesToByteArray(request.getUserProperties()));
            pstmt.setLong(8, request.getMessageExpiryIntervalSeconds());
            pstmt.setBytes(9, request.getCorrelationData());
            pstmt.setString(10, request.getResponseTopic());
            pstmt.setInt(12, request.getPayloadFormat().getValue());
            pstmt.setString(13, request.getContentType());
            pstmt.executeUpdate();
        }
    }

    /**
     * This method will remove a SpoolStorageDocument from the database given its id.
     * @param messageId the id of the SpoolStorageDocument
     */
    public void removeSpoolStorageDocumentById(Long messageId) throws SQLException {
        String deleteSQL = "DELETE FROM spooler WHERE message_id = ?;";
        try(Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(deleteSQL)) {
            pstmt.setLong(1, messageId);
            pstmt.executeUpdate();
        }
    }

    /**
     * This method creates a connection instance of the SQLite database.
     * @return Connection for SQLite database instance
     */
    private Connection getDbInstance() throws SQLException {
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
        try(Connection conn = getDbInstance();
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

    private byte[] userPropertiesToByteArray(List<UserProperty> userProps) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(userProps);
            oos.flush();
        } catch (java.io.IOException e) {
            // TODO: Add logging and exception handling
        } finally {
            try {
                baos.close();
            } catch (java.io.IOException e) {
                // TODO: Add logging and exception handling
            }
        }
        return baos.toByteArray();
    }

    private List<UserProperty> byteArrayToUserProperties(byte[] userProperties) {
        if (userProperties != null && userProperties.length > 0 ) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(userProperties));
                List<UserProperty> temp = (List<UserProperty>) ois.readObject();
                ois.close();
                return temp;
            } catch (java.io.IOException | ClassNotFoundException e) {
                // TODO: Add logging and exception handling
            }
        }
        return null;
    }
}
