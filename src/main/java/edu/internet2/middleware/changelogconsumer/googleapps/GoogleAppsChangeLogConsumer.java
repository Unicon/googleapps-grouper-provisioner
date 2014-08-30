/*
 * Licensed to the University Corporation for Advanced Internet Development, 
 * Inc. (UCAID) under one or more contributor license agreements.  See the 
 * NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The UCAID licenses this file to You under the Apache 
 * License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.internet2.middleware.changelogconsumer.googleapps;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.Member;
import com.google.api.services.admin.directory.model.User;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.CacheManager;
import edu.internet2.middleware.grouper.changeLog.*;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.time.StopWatch;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;

/**
 * A {@link ChangeLogConsumer} which provisions via Google Apps API.
 *
 * @author John Gasper, Unicon
 **/
public class GoogleAppsChangeLogConsumer extends ChangeLogConsumerBase {

    /** Maps change log entry category and action (change log type) to methods. */
    enum EventType {

        /** Process the add attribute assign value change log entry type. */
        attributeAssignValue__addAttributeAssignValue {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processAttributeAssignValueAdd(consumer, changeLogEntry);
            }
        },

        /** Process the delete attribute assign value change log entry type. */
        attributeAssignValue__deleteAttributeAssignValue {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processAttributeAssignValueDelete(consumer, changeLogEntry);
            }
        },

        /** Process the add group change log entry type. */
        group__addGroup {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processGroupAdd(consumer, changeLogEntry);
            }
        },

        /** Process the delete group change log entry type. */
        group__deleteGroup {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processGroupDelete(consumer, changeLogEntry);
            }
        },

        /** Process the update group change log entry type. */
        group__updateGroup {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processGroupUpdate(consumer, changeLogEntry);
            }
        },

        /** Process the add membership change log entry type. */
        membership__addMembership {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processMembershipAdd(consumer, changeLogEntry);
            }
        },

        /** Process the delete membership change log entry type. */
        membership__deleteMembership {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processMembershipDelete(consumer, changeLogEntry);
            }
        },

        /** Process the add stem change log entry type. */
        stem__addStem {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processStemAdd(consumer, changeLogEntry);
            }
        },

        /** Process the delete stem change log entry type. */
        stem__deleteStem {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processStemDelete(consumer, changeLogEntry);
            }
        },

        /** Process the update stem change log entry type. */
        stem__updateStem {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processStemUpdate(consumer, changeLogEntry);
            }
        },

        ;

        /**
         * Process the change log entry.
         *
         * @param consumer the google change log consumer
         * @param changeLogEntry the change log entry
         * @throws Exception if any error occurs
         */
        public abstract void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception;

    }

    /** LDAP error returned when a stem/ou is renamed and the DSA does not support subtree renaming. */
    public static final String ERROR_SUBTREE_RENAME_NOT_SUPPORTED =
            "[LDAP: error code 66 - subtree rename not supported]";

    /** Boolean used to delay change log processing when a full sync is running. */
    private static boolean fullSyncIsRunning;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsChangeLogConsumer.class);

    /**
     * Gets a simple string representation of the change log entry.
     *
     * @param changeLogEntry the change log entry
     * @return the simple string representation of the change log entry
     */
    public static String toString(ChangeLogEntry changeLogEntry) {
        ToStringBuilder toStringBuilder = new ToStringBuilder(changeLogEntry, ToStringStyle.SHORT_PREFIX_STYLE);
        toStringBuilder.append("timestamp", changeLogEntry.getCreatedOn());
        toStringBuilder.append("sequence", changeLogEntry.getSequenceNumber());
        toStringBuilder.append("category", changeLogEntry.getChangeLogType().getChangeLogCategory());
        toStringBuilder.append("actionname", changeLogEntry.getChangeLogType().getActionName());
        toStringBuilder.append("contextId", changeLogEntry.getContextId());
        return toStringBuilder.toString();
    }

    /**
     * Gets a deep string representation of the change log entry.
     *
     * @param changeLogEntry the change log entry
     * @return the deep string representation of the change log entry
     */
    public static String toStringDeep(ChangeLogEntry changeLogEntry) {
        ToStringBuilder toStringBuilder = new ToStringBuilder(changeLogEntry, ToStringStyle.SHORT_PREFIX_STYLE);
        toStringBuilder.append("timestamp", changeLogEntry.getCreatedOn());
        toStringBuilder.append("sequence", changeLogEntry.getSequenceNumber());
        toStringBuilder.append("category", changeLogEntry.getChangeLogType().getChangeLogCategory());
        toStringBuilder.append("actionname", changeLogEntry.getChangeLogType().getActionName());
        toStringBuilder.append("contextId", changeLogEntry.getContextId());

        ChangeLogType changeLogType = changeLogEntry.getChangeLogType();

        for (String label : changeLogType.labels()) {
            toStringBuilder.append(label, changeLogEntry.retrieveValueForLabel(label));
        }

        return toStringBuilder.toString();
    }

    /** The change log consumer name from the processor metadata. */
    private String name;

    /** Whether or not to retry a change log entry if an error occurs. */
    private boolean retryOnError = false;

    /** Whether or not to omit diff responses in a bulk response. */
    private boolean omitDiffResponses = false;

    /** Whether or not to omit sync responses in a bulk response. */
    private boolean omitSyncResponses = false;

    /** Google Directory service*/
    private Directory directory;

    /** Global instance of the HTTP transport. */
    private static HttpTransport httpTransport;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     *
     * Constructor. Initializes the underlying {@link Directory}.
     *
     */
    public GoogleAppsChangeLogConsumer() throws GeneralSecurityException, IOException {
        initialize();
    }

    /**
     * Execute each {@link ModifyRequest}. If an error occurs executing a request, continue to execute requests, but
     * throw an exception upon completion.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     * @param modifyRequests the modify requests
     */
  /*
    protected void executeModifyRequests(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry,
                                      Collection<ModifyRequest> modifyRequests) {

        boolean isError = false;

        for (ModifyRequest modifyRequest : modifyRequests) {

            ModifyResponse modifyResponse = consumer.getPsp().execute(modifyRequest);

            if (modifyResponse.getStatus().equals(StatusCode.SUCCESS)) {
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Modify '{}'", new Object[] {name,
                        toString(changeLogEntry), PSPUtil.toString(modifyResponse),});
            } else {
                LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Modify failed '{}'", new Object[] {name,
                        toString(changeLogEntry), PSPUtil.toString(modifyResponse),});
                isError = true;
            }
        }

        if (isError) {
            String message =
                    "Google Apps Consumer '" + name + "' - Change log entry '" + toString(changeLogEntry) + "' Modify failed";
            LOG.error(message);
            throw new PspException(message);
        }
    }
*/
    /**
     * Create and execute a {@link SyncRequest}. The request identifier is retrieved from the {@link ChangeLogEntry}
     * using the {@link ChangeLogLabel}.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     * @param changeLogLabel the change log label used to determine the sync request identifier
     */
/*
    protected void executeSync(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry, ChangeLogLabel changeLogLabel) {

        // will throw a RuntimeException on error
        String principalName = changeLogEntry.retrieveValueForLabel(changeLogLabel);

        SyncRequest syncRequest = new SyncRequest();
        syncRequest.setId(principalName);
        syncRequest.setRequestID(PSPUtil.uniqueRequestId());

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Will attempt to sync '{}'", new Object[] {name,
                toString(changeLogEntry), PSPUtil.toString(syncRequest),});

        SyncResponse syncResponse = consumer.getPsp().execute(syncRequest);

        if (syncResponse.getStatus().equals(StatusCode.SUCCESS)) {
            LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Sync was successful '{}'", new Object[] {name,
                    toString(changeLogEntry), PSPUtil.toString(syncResponse),});
        } else if (syncResponse.getError().equals(ErrorCode.NO_SUCH_IDENTIFIER)) {
            LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Sync unable to calculate provisioning '{}'",
                    new Object[] {name, toString(changeLogEntry), PSPUtil.toString(syncResponse),});
        } else {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Sync failed '{}'", new Object[] {name,
                    toString(changeLogEntry), PSPUtil.toString(syncResponse),});
            throw new PspException(PSPUtil.toString(syncResponse));
        }
    }
*/
    /**
     * Run a full synchronization by executing a {@link BulkSyncRequest}.
     *
     * @return the response
     */
    /*
    public synchronized Response fullSync() {

        LOG.info("Google Apps Consumer '{}' - Starting full sync", name);

        if (fullSyncIsRunning) {
            LOG.info("Google Apps Consumer '{}' - Full sync is already running, will defer to next scheduled trigger.", name);
            return null;
        }

        fullSyncIsRunning = true;

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Perform bulk sync request without responses to conserve memory.
        BulkSyncRequest request = new BulkSyncRequest();
        if (omitDiffResponses) {
            request.setReturnDiffResponses(false);
        }
        if (omitSyncResponses) {
            request.setReturnSyncResponses(false);
        }
        BulkSyncResponse response = psp.execute(request);

        stopWatch.stop();

        fullSyncIsRunning = false;

        if (response.getStatus().equals(StatusCode.SUCCESS)) {
            LOG.info("Google Apps Consumer '{}' - Full sync was successful '{}'", name, PSPUtil.toString(response));
        } else {
            LOG.error("Google Apps Consumer '{}' - Full sync was not successful '{}'", name, PSPUtil.toString(response));
        }

        LOG.info("Google Apps Consumer '{}' - Finished full sync. Elapsed time {}", name, stopWatch);

        if (LOG.isDebugEnabled()) {
            for (String stats : PspCLI.getAllCacheStats()) {
                LOG.debug(stats);
            }
        }

        return response;
    }
*/
    /**
     * Return the {@link Directory}.
     *
     * @return the Google Apps Directory (service) object
     */

    protected Directory getDirectory() {
        return directory;
    }

    /**
     * If the underlying XX has not been initialized, instantiate the XX. Use the configuration
     * directory from the 'changeLog.consumer.ldappcng.confDir' property. If this property has not been set, then
     * configuration resources on the classpath will be used.
     */
    protected void initialize() throws GeneralSecurityException, IOException {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        if (directory == null) {
            final String serviceAccountPKCS12FilePath =
                    GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountPKCS12FilePath", true);
            LOG.debug("Google Apps Consumer - Setting Google serviceAccountPKCS12FilePath to {}", serviceAccountPKCS12FilePath);

            final String serviceAccountEmail =
                    GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountEmail", true);
            LOG.debug("Google Apps Consumer - Setting Google serviceAccountEmail on error to {}", serviceAccountEmail);

            final String serviceAccountUser =
                    GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountUser", true);
            LOG.debug("Google Apps Consumer - Setting Google serviceAccountUser to {}", serviceAccountUser);

            GoogleAppsUtils.googleDomain =
                    GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.domain", true);
            LOG.debug("Google Apps Consumer - Setting Google domain to {}", GoogleAppsUtils.googleDomain);


            final GoogleCredential googleCredential = GoogleAppsUtils.getGoogleCredential(serviceAccountEmail,
                    serviceAccountPKCS12FilePath, serviceAccountUser, httpTransport, JSON_FACTORY);

            directory = new Directory.Builder(httpTransport, JSON_FACTORY, googleCredential)
                    .setApplicationName("Google Apps Grouper Provisioner")
                    .build();

            //TODO: make the cache settings properties
            CacheManager.googleUsers().setCacheValidity(15);
            populateUserCache(directory);

            CacheManager.googleGroups().setCacheValidity(15);
            populateGroupCache(directory);
        }
            // retry on error
            retryOnError = GrouperLoaderConfig.getPropertyBoolean("changeLog.consumer.psp.retryOnError", false);
            LOG.debug("Google Apps Consumer - Setting retry on error to {}", retryOnError);

            // omit diff responses
            omitDiffResponses =
                    GrouperLoaderConfig.getPropertyBoolean("changeLog.psp.fullSync.omitDiffResponses", false);
            LOG.debug("Google Apps Consumer - Setting omit diff responses to {}", omitDiffResponses);

            // omit sync responses
            omitSyncResponses =
                    GrouperLoaderConfig.getPropertyBoolean("changeLog.psp.fullSync.omitSyncResponses", false);
            LOG.debug("Google Apps Consumer - Setting omit sync responses to {}", omitSyncResponses);
    }

    /**
     * Returns true if a change log entry should be retried upon error.
     *
     * @return Returns true if a change log entry should be retried upon error.
     */
    public boolean isRetryOnError() {
        return retryOnError;
    }

    /**
     * Add an attribute value.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */

    protected void processAttributeAssignValueAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
/*
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing add attribute assign value.", name,
                toString(changeLogEntry));

        List<ModifyRequest> modifyRequests =
                consumer.processModification(consumer, changeLogEntry, ModificationMode.ADD, ReturnData.DATA);

        executeModifyRequests(consumer, changeLogEntry, modifyRequests);
        */
    }

    /**
     * Delete an attribute value.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processAttributeAssignValueDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry)  {
/*
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing delete attribute assign value.", name,
                toString(changeLogEntry));

        List<ModifyRequest> modifyRequests =
                consumer.processModification(consumer, changeLogEntry, ModificationMode.DELETE, ReturnData.DATA);

        executeModifyRequests(consumer, changeLogEntry, modifyRequests);
        */
    }

    /** {@inheritDoc} */
    @Override
    public long processChangeLogEntries(final List<ChangeLogEntry> changeLogEntryList,
                                        ChangeLogProcessorMetadata changeLogProcessorMetadata) {

        //let's populate the caches, if necessary
        populateUserCache(getDirectory());
        populateGroupCache(getDirectory());

        // the change log sequence number to return
        long sequenceNumber = -1;

        // initialize this consumer's name from the change log metadata
        if (name == null) {
            name = changeLogProcessorMetadata.getConsumerName();
            LOG.trace("Google Apps Consumer '{}' - Setting name.", name);
        }

        // time context processing
        StopWatch stopWatch = new StopWatch();
        // the last change log sequence number processed
        String lastContextId = null;

        LOG.debug("Google Apps Consumer '{}' - Processing change log entry list size '{}'", name, changeLogEntryList.size());

        // process each change log entry
        for (ChangeLogEntry changeLogEntry : changeLogEntryList) {

            // return the current change log sequence number
            sequenceNumber = changeLogEntry.getSequenceNumber();

            // if full sync is running, return the previous sequence number to process this entry on the next run
            if (fullSyncIsRunning) {
                LOG.info("Google Apps Consumer '{}' - Full sync is running, returning sequence number '{}'", name,
                        sequenceNumber - 1);
                return sequenceNumber - 1;
            }

            // if first run, start the stop watch and store the last sequence number
            if (lastContextId == null) {
                stopWatch.start();
                lastContextId = changeLogEntry.getContextId();
            }

            // whether or not an exception was thrown during processing of the change log entry
            boolean errorOccurred = false;

            try {
                // process the change log entry
                processChangeLogEntry(changeLogEntry);

            } catch (Exception e) {
                errorOccurred = true;
                String message =
                        "Google Apps Consumer '" + name + "' - An error occurred processing sequence number " + sequenceNumber;
                LOG.error(message, e);
                changeLogProcessorMetadata.registerProblem(e, message, sequenceNumber);
                changeLogProcessorMetadata.setHadProblem(true);
                changeLogProcessorMetadata.setRecordException(e);
                changeLogProcessorMetadata.setRecordExceptionSequence(sequenceNumber);
            }

            // if the change log context id has changed, log and restart stop watch
            if (!lastContextId.equals(changeLogEntry.getContextId())) {
                stopWatch.stop();
                LOG.debug("Google Apps Consumer '{}' - Processed change log context '{}' Elapsed time {}", new Object[] {name,
                        lastContextId, stopWatch,});
                stopWatch.reset();
                stopWatch.start();
            }

            lastContextId = changeLogEntry.getContextId();

            // if an error occurs and retry on error is true, return the current sequence number minus 1
            if (errorOccurred && retryOnError) {
                sequenceNumber--;
                break;
            }
        }

        // stop the timer and log
        stopWatch.stop();
        LOG.debug("Google Apps Consumer '{}' - Processed change log context '{}' Elapsed time {}", new Object[] {name,
                lastContextId, stopWatch,});

        if (sequenceNumber == -1) {
            LOG.error("Google Apps Consumer '" + name + "' - Unable to process any records.");
            throw new RuntimeException("Google Apps Consumer '" + name + "' - Unable to process any records.");
        }

        LOG.debug("Google Apps Consumer '{}' - Finished processing change log entries. Last sequence number '{}'", name,
                sequenceNumber);

        // return the sequence number
        return sequenceNumber;
    }

    /**
     * Call the method of the {@link EventType} enum which matches the {@link ChangeLogEntry} category and action (the
     * change log type).
     *
     * @param changeLogEntry the change log entry
     * @throws Exception if an error occurs processing the change log entry
     */
    protected void processChangeLogEntry(ChangeLogEntry changeLogEntry) throws Exception {

        try {
            // find the method to run via the enum
            String enumKey =
                    changeLogEntry.getChangeLogType().getChangeLogCategory() + "__"
                            + changeLogEntry.getChangeLogType().getActionName();

            EventType ldappcEventType = EventType.valueOf(enumKey);

            if (ldappcEventType == null) {
                LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Unsupported category and action.", name,
                        toString(changeLogEntry));
            } else {
                // process the change log event
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}'", name, toStringDeep(changeLogEntry));
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                ldappcEventType.process(this, changeLogEntry);

                stopWatch.stop();
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Finished processing. Elapsed time {}",
                        new Object[] {name, toString(changeLogEntry), stopWatch,});

                if (LOG.isDebugEnabled()) {
                    //for (String stats : PspCLI.getAllCacheStats()) {
                    //    LOG.debug(stats);
                    //}
                }
            }
        } catch (IllegalArgumentException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Unsupported category and action.", name,
                    toString(changeLogEntry));
        }
    }

    /**
     * Delete an object. The object identifiers to be deleted are calculated from the change log entry. For every object
     * to be deleted, a lookup is performed on the object identifier to determine if the object exists. If the object
     * exists, it is deleted.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
/*
        // calculate the psoID to be deleted from the change log entry
        CalcRequest calcRequest = new CalcRequest();
        calcRequest.setReturnData(ReturnData.IDENTIFIER);
        calcRequest.setRequestID(PSPUtil.uniqueRequestId());

        CalcResponse calcResponse = consumer.getPsp().execute(calcRequest);

        if (!calcResponse.getStatus().equals(StatusCode.SUCCESS)) {
            LOG.error("Google Apps Consumer '{}' - Calc request '{}' failed {}", new Object[] {name, calcRequest.toString(),
                    PSPUtil.toString(calcResponse),});
            throw new PspException(PSPUtil.toString(calcResponse));
        }

        List<PSO> psos = calcResponse.getPSOs();

        if (psos.isEmpty()) {
            LOG.warn("Google Apps Consumer '{}' - Change log entry '{}' Unable to calculate identifier.", name,
                    toString(changeLogEntry));
            return;
        }

        for (PSO pso : psos) {
            // lookup object to see if it exists
            LookupRequest lookupRequest = new LookupRequest();
            lookupRequest.setPsoID(pso.getPsoID());
            lookupRequest.setRequestID(PSPUtil.uniqueRequestId());
            lookupRequest.setReturnData(ReturnData.IDENTIFIER);
            LookupResponse lookupResponse = consumer.getPsp().execute(lookupRequest);

            if (!lookupResponse.getStatus().equals(StatusCode.SUCCESS)) {
                LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Identifier '{}' does not exist.", new Object[] {
                        name, toString(changeLogEntry), PSPUtil.toString(pso.getPsoID()),});
                continue;
            }

            DeleteRequest deleteRequest = new DeleteRequest();
            deleteRequest.setPsoID(pso.getPsoID());
            deleteRequest.setRequestID(PSPUtil.uniqueRequestId());

            DeleteResponse deleteResponse = consumer.getPsp().execute(deleteRequest);

            if (deleteResponse.getStatus().equals(StatusCode.SUCCESS)) {
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Delete '{}'", new Object[] {name,
                        toString(changeLogEntry), PSPUtil.toString(deleteResponse),});
            } else {
                LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Delete failed '{}'", new Object[] {name,
                        toString(changeLogEntry), PSPUtil.toString(deleteResponse),});
                throw new PspException(PSPUtil.toString(deleteResponse));
            }
        }
        */
    }

    /**
     * Add a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group add.", name, toString(changeLogEntry));

        final Group group = new Group();
        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name);
        final String description = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.description);
        final String displayName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.displayName);

        group.setName(groupName);
        group.setEmail(GoogleAppsUtils.qualifyAddress(groupName, "-"));
        group.setDescription(description);

        try {
            GoogleAppsUtils.addGroup(directory, group);
        } catch (IOException e) {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add: {}", Arrays.asList(name, toString(changeLogEntry), e));
        }
    }

    /**
     * Delete a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
        //TODO: to archive or not to archive... that is the question!
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group delete.", name, toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name);

        try {
            final Group group = GoogleAppsUtils.retrieveGroup(directory, GoogleAppsUtils.qualifyAddress(groupName, "-"));
            GoogleAppsUtils.removeGroup(directory, group);
        } catch (IOException e) {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group delete: {}", Arrays.asList(name, toString(changeLogEntry), e.getMessage()));
        }
    }

    /**
     * Update a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupUpdate(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group update.", name, toString(changeLogEntry));

        final String propertyChanged = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyChanged);

        if (propertyChanged.equalsIgnoreCase("description")) {

        }

        //processUpdate(consumer, changeLogEntry, ChangeLogLabels.GROUP_UPDATE.name);
    }

    /**
     * Add a membership.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processMembershipAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing membership add.", name,
                toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName);
        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId);

        try {
            //TODO: A cache check should happen first
            Group group = GoogleAppsUtils.retrieveGroup(directory, GoogleAppsUtils.qualifyAddress(groupName, ":"));
            User user = GoogleAppsUtils.retrieveUser(directory, GoogleAppsUtils.qualifyAddress(subjectId));

            Member member = new Member();
            member.setEmail(user.getPrimaryEmail());
            //TODO: member.setRole();

            GoogleAppsUtils.addGroupMember(directory, group, member);
        } catch (IOException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership add failed: {}", Arrays.asList(name,
                    toString(changeLogEntry), e));
        }
/*
        List<ModifyRequest> modifyRequests =
                consumer.processModification(consumer, changeLogEntry, ModificationMode.ADD, ReturnData.EVERYTHING);

        executeModifyRequests(consumer, changeLogEntry, modifyRequests);
*/
    }

    /**
     * Delete a membership.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processMembershipDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing membership delete.", name,
                toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName);
        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.subjectId);

        try {
            //TODO: A cache check should happen first
            Group group = GoogleAppsUtils.retrieveGroup(directory, GoogleAppsUtils.qualifyAddress(groupName, ":"));
            GoogleAppsUtils.removeGroupMember(directory, group, GoogleAppsUtils.qualifyAddress(subjectId));
        } catch (IOException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership delete: {}", Arrays.asList(name,
                    toString(changeLogEntry), e));
        }

/*
        List<ModifyRequest> modifyRequests =
                consumer.processModification(consumer, changeLogEntry, ModificationMode.DELETE, ReturnData.EVERYTHING);

        executeModifyRequests(consumer, changeLogEntry, modifyRequests);
        */
    }

    /**
     * Return a {@link ModifyRequest} for the given {@link PSO} whose references or attributes need to be modified.
     *
     * @param pso the provisioned service object
     * @param modificationMode the modification mode
     * @param returnData spmlv2 return data
     * @return the modify request or null if there are no modifications to be performed
     */

    /*protected ModifyRequest processModification(PSO pso, ModificationMode modificationMode, ReturnData returnData) {

        List<DSMLAttr> attributes = processModificationData(pso, modificationMode);

        List<Reference> references = processModificationReferences(pso, modificationMode);

        if (references.isEmpty() && attributes.isEmpty()) {
            return null;
        }

        ModifyRequest modifyRequest = new ModifyRequest();
        modifyRequest.setRequestID(PSPUtil.uniqueRequestId());
        modifyRequest.setPsoID(pso.getPsoID());
        modifyRequest.addOpenContentAttr(Pso.ENTITY_NAME_ATTRIBUTE,
                pso.findOpenContentAttrValueByName(Pso.ENTITY_NAME_ATTRIBUTE));
        modifyRequest.setReturnData(ReturnData.IDENTIFIER);

        if (!attributes.isEmpty()) {
            for (DSMLAttr dsmlAttr : attributes) {
                Modification modification = new Modification();
                modification.setModificationMode(modificationMode);
                DSMLModification dsmlMod =
                        new DSMLModification(dsmlAttr.getName(), dsmlAttr.getValues(), modificationMode);
                modification.addOpenContentElement(dsmlMod);
                modifyRequest.addModification(modification);
            }
        }

        if (!references.isEmpty()) {
            Modification modification = new Modification();
            modification.setModificationMode(modificationMode);
            CapabilityData capabilityData = PSPUtil.fromReferences(references);
            modification.addCapabilityData(capabilityData);
            modifyRequest.addModification(modification);
        }

        return modifyRequest;
    }
*/
    /**
     * Return a {@link ModifyRequest} for every {@link PSO} whose references or attributes need to be modified.
     *
     * @param consumer the psp change log consumer
     * @param changeLogEntry the change log entry
     * @param modificationMode the modification mode
     * @param returnData spmlv2 return data
     * @return the possibly empty list of modify requests
     */
    /*
    public List<ModifyRequest> processModification(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry,
                                                   ModificationMode modificationMode, ReturnData returnData) {

        List<ModifyRequest> modifyRequests = new ArrayList<ModifyRequest>();

        CalcRequest calcRequest = new CalcRequest();
        calcRequest.setRequestID(PSPUtil.uniqueRequestId());
        if (returnData != null) {
            calcRequest.setReturnData(returnData);
        }

        CalcResponse calcResponse = consumer.getPsp().execute(calcRequest);

        for (PSO pso : calcResponse.getPSOs()) {

            ModifyRequest modifyRequest = processModification(pso, modificationMode, returnData);

            if (modifyRequest != null) {
                modifyRequests.add(modifyRequest);
            }
        }

        return modifyRequests;
    }
*/
    /**
     * Return the {@link DSMLAttr}s which need to be added or deleted to the {@link PSO}.
     *
     * @param pso the provisioned object
     * @param modificationMode the modification mode, either add or delete
     * @return the possibly empty list of attributes
     */
    /*
    public List<DSMLAttr> processModificationData(PSO pso, ModificationMode modificationMode) {

        // the attributes which need to be modified
        List<DSMLAttr> attributesToBeModified = new ArrayList<DSMLAttr>();

        // attributes from the pso
        Map<String, DSMLAttr> dsmlAttrMap = PSPUtil.getDSMLAttrMap(pso.getData());

        // for every attribute
        for (String dsmlAttrName : dsmlAttrMap.keySet()) {
            DSMLAttr dsmlAttr = dsmlAttrMap.get(dsmlAttrName);

            // the dsml values to be added or deleted
            List<DSMLValue> dsmlValuesToBeModified = new ArrayList<DSMLValue>();

            // for every attribute value
            for (DSMLValue dsmlValue : dsmlAttr.getValues()) {

                // if modification mode is delete, do not delete value if retain all values is true
                if (modificationMode.equals(ModificationMode.DELETE)) {
                    String entityName = pso.findOpenContentAttrValueByName(Pso.ENTITY_NAME_ATTRIBUTE);
                    if (entityName != null) {
                        Pso psoDefinition = psp.getPso(pso.getPsoID().getTargetID(), entityName);
                        if (psoDefinition != null) {
                            boolean retainAll = psoDefinition.getPsoAttribute(dsmlAttrName).isRetainAll();
                            if (retainAll) {
                                continue;
                            }
                        }
                    }
                }

                try {
                    // perform a search to determine if the attribute exists
                    boolean hasAttribute = psp.hasAttribute(pso.getPsoID(), dsmlAttr.getName(), dsmlValue.getValue());

                    // if adding attribute and it does not exist on target, modify
                    if (modificationMode.equals(ModificationMode.ADD) && !hasAttribute) {
                        dsmlValuesToBeModified.add(dsmlValue);
                    }

                    // if replacing attribute and it does not exist on target, modify
                    if (modificationMode.equals(ModificationMode.REPLACE) && !hasAttribute) {
                        dsmlValuesToBeModified.add(dsmlValue);
                    }

                    // if deleting attribute and it exists on target, modify
                    if (modificationMode.equals(ModificationMode.DELETE) && hasAttribute) {
                        dsmlValuesToBeModified.add(dsmlValue);
                    }
                } catch (PspNoSuchIdentifierException e) {
                    if (modificationMode.equals(ModificationMode.DELETE)) {
                        // ignore, must be already deleted, do not throw exception
                    } else {
                        throw new PspException(e);
                    }
                }
            }

            // return the dsml values to be added
            if (!dsmlValuesToBeModified.isEmpty()) {
                attributesToBeModified.add(new DSMLAttr(dsmlAttr.getName(), dsmlValuesToBeModified
                        .toArray(new DSMLValue[] {})));
            }
        }

        return attributesToBeModified;
    }
*/
    /**
     * Return the {@link Reference}s which need to be added or deleted to the {@link PSO}.
     *
     * A HasReference query is performed to determine if each {@link Reference} exists.
     *
     * @param pso the provisioned object
     * @param modificationMode the modification mode, either add or delete
     * @return the possibly empty list of references
     */
    /*
    public List<Reference> processModificationReferences(PSO pso, ModificationMode modificationMode) {

        // the references which need to be modified
        List<Reference> references = new ArrayList<Reference>();

        // references from the pso
        Map<String, List<Reference>> referenceMap = PSPUtil.getReferences(pso.getCapabilityData());

        // for every type of reference, i.e. the attribute name
        for (String typeOfReference : referenceMap.keySet()) {
            // for every reference
            for (Reference reference : referenceMap.get(typeOfReference)) {

                // perform a search to determine if the reference exists
                try {
                    boolean hasReference = psp.hasReference(pso.getPsoID(), reference);

                    // if adding reference and reference does not exist, modify
                    if (modificationMode.equals(ModificationMode.ADD) && !hasReference) {
                        references.add(reference);
                    }

                    // if replacing reference and reference does not exist, modify
                    if (modificationMode.equals(ModificationMode.REPLACE) && !hasReference) {
                        references.add(reference);
                    }

                    // if deleting reference and reference exists, modify
                    if (modificationMode.equals(ModificationMode.DELETE) && hasReference) {
                        references.add(reference);
                    }
                } catch (PspNoSuchIdentifierException e) {
                    if (modificationMode.equals(ModificationMode.DELETE)) {
                        // ignore, must be already deleted, do not throw exception
                    } else {
                        throw new PspException(e);
                    }
                }

            }
        }

        return references;
    }
*/
    /**
     * Add a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem add.", name, toString(changeLogEntry));
/*
        executeSync(consumer, changeLogEntry, ChangeLogLabels.STEM_ADD.name);
*/
    }

    /**
     * Delete a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem delete.", name, toString(changeLogEntry));

        processDelete(consumer, changeLogEntry);
    }

    /**
     * Update a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemUpdate(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem update.", name, toString(changeLogEntry));

        processUpdate(consumer, changeLogEntry, ChangeLogLabels.STEM_UPDATE.name);
    }

    /**
     * Process an object update. If the object should be renamed, attempt to rename, otherwise attempt to modify.
     *
     * If the attempt to rename the object fails with the {@link //ERROR_SUBTREE_RENAME_NOT_SUPPORTED} error, then attempt
     * to sync the object.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     * @param principalNameLabel the change log label used to determine the sync request identifier
     */
    public void processUpdate(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry,
                              ChangeLogLabel principalNameLabel) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing object update.", name, toString(changeLogEntry));
/*
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing object update was successful.", name,
                toString(changeLogEntry));
*/
    }

    /**
     * If true, retry a change log entry if an error occurs.
     *
     * @param retryOnError If true, retry a change log entry if an error occurs.
     */
    public void setRetryOnError(boolean retryOnError) {
        this.retryOnError = retryOnError;
    }

    private void populateUserCache(Directory directory) {
        LOG.debug("Google Apps Consumer '{}' - Populating the userCache.", name);

        if (CacheManager.googleUsers() == null || CacheManager.googleUsers().isExpired()) {
            try {
                List<User> list = GoogleAppsUtils.retrieveAllUsers(getDirectory());
                CacheManager.googleUsers().seed(list);

            } catch (GoogleJsonResponseException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            } catch (IOException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            }
        }
    }

    private void populateGroupCache(Directory directory) {
        LOG.debug("Google Apps Consumer '{}' - Populating the userCache.", name);

        if (CacheManager.googleGroups() == null || CacheManager.googleGroups().isExpired()) {
            try {
                List<Group> list = GoogleAppsUtils.retrieveAllGroups(getDirectory());
                CacheManager.googleGroups().seed(list);

            } catch (GoogleJsonResponseException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            } catch (IOException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            }
        }
    }
}
