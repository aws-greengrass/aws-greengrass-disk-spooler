/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.CloudMessageSpool;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;

import java.io.IOException;
import java.sql.SQLException;
import javax.inject.Inject;

@ImplementsService(name = DiskSpool.PERSISTENCE_SERVICE_NAME, autostart = true)
public class DiskSpool extends PluginService implements CloudMessageSpool {

    public static final String PERSISTENCE_SERVICE_NAME = "aws.greengrass.DiskSpooler";
    private static final Logger logger = LogManager.getLogger(DiskSpool.class);
    private static final String KV_MESSAGE_ID = "messageId";
    private final DiskSpoolDAO dao;

    @Inject
    public DiskSpool(Topics topics, DiskSpoolDAO dao) {
        super(topics);
        this.dao = dao;
    }

    /**
     * This function takes an id and returns a SpoolMessage from the db with the same id.
     * @param id : id assigned to MQTT message
     * @return payload of the MQTT message stored with id
     */
    @Override
    public SpoolMessage getMessageById(long id) {
        try {
            return dao.getSpoolMessageById(id);
        } catch (SQLException e) {
            logger.atError()
                    .kv(KV_MESSAGE_ID, id)
                    .cause(e)
                    .log("Failed to retrieve message by messageId");
            return null;
        }
    }


    /**
     * This function takes an id and removes the row in the db with the corresponding id.
     * @param id : id assigned to MQTT message
     */
    @Override
    public void removeMessageById(long id) {
        try {
            dao.removeSpoolMessageById(id);
            logger.atTrace().kv(KV_MESSAGE_ID, id).log("Removed message from Disk Spooler");
        } catch (SQLException e) {
            logger.atWarn()
                    .kv(KV_MESSAGE_ID, id)
                    .cause(e)
                    .log("Failed to delete message by messageId");
        }
    }

    /**
     * This function takes in an id and SpoolMessage and inserts them as a row into the spooler db.
     * @param id : id assigned to MQTT message
     * @param message :
     */
    @Override
    public void add(long id, SpoolMessage message) throws IOException {
        try {
            dao.insertSpoolMessage(message);
            logger.atTrace().kv(KV_MESSAGE_ID, id).log("Added message to Disk Spooler");
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Iterable<Long> getAllMessageIds() throws IOException {
        try {
            return dao.getAllSpoolMessageIds();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void initializeSpooler() throws IOException {
        try {
            dao.initialize();
            logger.atInfo().log("Finished setting up Database");
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void shutdown() throws InterruptedException {
        super.shutdown();
        dao.close();
    }
}
