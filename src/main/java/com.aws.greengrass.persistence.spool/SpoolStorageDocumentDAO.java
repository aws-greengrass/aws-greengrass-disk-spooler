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
        setUpDatabase();
    }

    /**
     * This method will query the existing database for the existing queue of MQTT request Ids
     * and return them in order.
     * @return ordered list of the existing ids in the persistent queue
     */
    public Iterable<Long> getAllSpoolStorageDocumentIds() {
        List<Long> currentIds;
        String query = "SELECT message_id FROM spooler;";
        try(Connection conn = getDbInstance();
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query)) {
            currentIds = getIdsFromRs(rs);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return currentIds;
    }

    private List<Long> getIdsFromRs(ResultSet rs) throws SQLException {
        List<Long> currentIds;
        //check if rs empty
        if (!rs.next()) {
            //if empty we will return null
            currentIds = null;
        } else {
            //if not empty we create return iterable of the contents
            currentIds = new ArrayList<>();
            do {
                Long id = rs.getLong(1);
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
    public SpoolStorageDocument getSpoolStorageDocumentById(long messageId) {
        return null;
    }

    /**
     * This method will insert a SpoolStorageDocument into the database.
     * @param document instance of SpoolStorageDocument
     */
    public void insertSpoolStorageDocument(SpoolStorageDocument document) {

    }

    /**
     * This method will remove a SpoolStorageDocument from the database given its id.
     * @param messageId the id of the SpoolStorageDocument
     */
    public void removeSpoolStorageDocumentById(Long messageId) {

    }

    /**
     * This method creates a connection instance of the SQLite database.
     * @return Connection for SQLite database instance
     */
    private Connection getDbInstance() throws SQLException, IOException {
        return DriverManager.getConnection(url);
    }

    private void setUpDatabase() {
        String tableCreationString = "CREATE TABLE IF NOT EXISTS spooler ("
                + "message_id INTEGER PRIMARY KEY, "
                + "retried INTEGER NOT NULL, "
                + "topic STRING NOT NULL,"
                + "qos INTEGER NOT NULL,"
                + "retain BOOLEAN,"
                + "payload BLOB"
                + ");";
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try(Connection conn = getDbInstance();
            Statement st = conn.createStatement()) {
            //create new table if table doesn't exist
            st.executeUpdate(tableCreationString);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
