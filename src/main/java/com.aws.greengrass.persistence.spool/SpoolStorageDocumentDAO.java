/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.util.NucleusPaths;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.aws.greengrass.persistence.spool.PersistenceSpool.PERSISTENCE_SERVICE_NAME;

public class SpoolStorageDocumentDAO {
    private final String url;
    private static final String DATABASE_FORMAT = "jdbc:sqlite:%s/spooler.db";

    @Inject
    public SpoolStorageDocumentDAO(NucleusPaths paths) throws IOException {
        Path workPath = paths.workPath(PERSISTENCE_SERVICE_NAME);
        url = String.format(DATABASE_FORMAT, workPath);
    }

    /**
     * This method will query the existing database for the existing queue of MQTT request Ids
     * and return them in order.
     * @return ordered list of the existing ids in the persistent queue
     */
    public List<Long> getAllSpoolStorageDocumentIds() {
        return null;
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
     * @param messageID the id of the SpoolStorageDocument
     */
    public void removeSpoolStorageDocumentById(Long messageID) {

    }
}