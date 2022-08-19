/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.spool.CloudMessageSpool;
import com.aws.greengrass.lifecyclemanager.PluginService;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@ImplementsService(name = PersistenceSpool.PERSISTENCE_SERVICE_NAME, autostart = true)
public class PersistenceSpool extends PluginService implements CloudMessageSpool {

    public static final String PERSISTENCE_SERVICE_NAME = "aws.greengrass.persistence.spooler";

    private final SpoolStorageDocumentDAO dao;

    @Inject
    public PersistenceSpool(Topics topics, SpoolStorageDocumentDAO dao) throws IOException {
        super(topics);
        this.dao = dao;
    }

    @Override
    public boolean isBootstrapRequired(Map<String, Object> newServiceConfig) {
        return true;
    }

    /**
     * This function takes an id and returns a SpoolMessage from the db with the same id.
     * @param id : id assigned to MQTT message
     * @return payload of the MQTT message stored with id
     */
    @Override
    public SpoolMessage getMessageById(long id) {
        SpoolStorageDocument document = dao.getSpoolStorageDocumentById(id);
        return document.getSpoolMessage();
    }


    /**
     * This function takes an id and removes the row in the db with the corrosponding id.
     * @param id : id assigned to MQTT message
     */
    @Override
    public void removeMessageById(long id) {
        dao.removeSpoolStorageDocumentById(id);
    }

    /**
     * This function takes in an id and SpoolMessage and inserts them as a row into the spooler db.
     * @param id : id assigned to MQTT message
     * @param message :
     */
    @Override
    public void add(long id, SpoolMessage message) {
        SpoolStorageDocument document = new SpoolStorageDocument(message);
        dao.insertSpoolStorageDocument(document);
    }

    @Override
    public Iterable<Long> getAllSpoolMessageIds() throws IOException {
        return dao.getAllSpoolStorageDocumentIds();
    }
}
