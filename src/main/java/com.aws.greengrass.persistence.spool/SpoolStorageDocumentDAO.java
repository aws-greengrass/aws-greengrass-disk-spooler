/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.util.NucleusPaths;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.aws.greengrass.persistence.spool.PersistenceSpool.PERSISTENCE_SERVICE_NAME;

public class SpoolStorageDocumentDAO {
    private final String url;
    protected static final String DATABASE_FORMAT = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";

    @Inject
    public SpoolStorageDocumentDAO(NucleusPaths paths) throws IOException {
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
    public SpoolStorageDocument getSpoolStorageDocumentById(long messageId) throws SQLException {
        String query = "SELECT retried, topic, qos, retain, payload FROM spooler WHERE message_id = ?;";
        try(Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setLong(1, messageId);
            try(ResultSet rs = pstmt.executeQuery()) {
                return getSpoolStorageDocumentFromRs(messageId, rs);
            }
        }
    }

    /**
     * This method will insert a SpoolStorageDocument into the database.
     * @param document instance of SpoolStorageDocument
     */
    public void insertSpoolStorageDocument(SpoolStorageDocument document) throws SQLException {
        String sqlString =
                "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload) VALUES (?,?,?,?,?,?);";
        try(Connection conn = getDbInstance();
            PreparedStatement pstmt = conn.prepareStatement(sqlString)) {
            pstmt.setLong(1, document.getMessageId());
            pstmt.setInt(2, document.getRetried());
            pstmt.setString(3, document.getMessageTopic());
            pstmt.setInt(4, document.getMessageQOS());
            pstmt.setBoolean(5, document.isRetain());
            pstmt.setBytes(6, document.getMessagePayload());
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
                + "payload BLOB"
                + ");";
        DriverManager.registerDriver(new org.sqlite.JDBC());
        try(Connection conn = getDbInstance();
            Statement st = conn.createStatement()) {
            //create new table if table doesn't exist
            st.executeUpdate(tableCreationString);
        }
    }

    private SpoolStorageDocument getSpoolStorageDocumentFromRs(long messageId, ResultSet rs) throws SQLException {
        if (rs.next()) {
            int retried = rs.getInt("retried");
            String topic = rs.getString("topic");
            int qosNum = rs.getInt("qos");
            boolean retain = rs.getBoolean("retain");
            byte[] payload = rs.getBytes("payload");
            return new SpoolStorageDocument(messageId, retried, topic, qosNum, retain, payload);
        } else {
            return null;
        }
    }
}
