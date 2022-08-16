package com.aws.greengrass.persistence.spool;

import java.nio.file.Path;
import java.util.List;

public class SpoolStorageDocumentDAO {
    private final String url;
    private static final String DATABASE_FORMAT = "jdbc:sqlite:%s/spooler.db";

    public SpoolStorageDocumentDAO(Path kernelWorkpath) {
        url = String.format(DATABASE_FORMAT, kernelWorkpath);
    }

    /**
     * This function will query the existing database for the existing queue of MQTT request Ids
     * and return them in order in an arraylist.
     * @return ordered arraylist of the existing ids in the persistent queue
     */
    public List<Long> getAllSpoolStorageDocumentIds() {
        return null;
    }

    /**
     * This function will query a SpoolStorageDocument and return it given an id.
     * @param messageID the id of the SpoolStorageDocument
     * @return SpoolStorageDocument
     */
    public SpoolStorageDocument getSpoolStorageDocumentById(Long messageID) {
        return null;
    }

    /**
     * This function will insert a SpoolStorageDocument into the database.
     * @param document instance of SpoolStorageDocument
     */
    public void insertSpoolStorageDocument(SpoolStorageDocument document) {

    }

    /**
     * This function will remove a SpoolStorageDocument from the database given its id.
     * @param messageID the id of the SpoolStorageDocument
     */
    public void removeSpoolStorageDocumentById(Long messageID) {

    }
}
