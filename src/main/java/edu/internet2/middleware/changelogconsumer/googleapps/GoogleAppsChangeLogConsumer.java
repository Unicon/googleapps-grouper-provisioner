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
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
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
import com.google.api.services.admin.directory.model.UserName;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.CacheManager;
import edu.internet2.middleware.grouper.*;
import edu.internet2.middleware.grouper.attr.AttributeDef;
import edu.internet2.middleware.grouper.attr.AttributeDefName;
import edu.internet2.middleware.grouper.attr.AttributeDefType;
import edu.internet2.middleware.grouper.attr.finder.AttributeDefNameFinder;
import edu.internet2.middleware.grouper.changeLog.*;
import edu.internet2.middleware.subject.Subject;
import edu.internet2.middleware.subject.SubjectType;
import edu.internet2.middleware.subject.provider.SubjectTypeEnum;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;

/**
 * A {@link ChangeLogConsumer} which provisions via Google Apps API.
 *
 * @author John Gasper, Unicon
 **/
public class GoogleAppsChangeLogConsumer extends ChangeLogConsumerBase {

    public static final String SYNC_TO_GOOGLE = "syncToGoogle";
    public static final String GOOGLE_PROVISIONER = "googleProvisioner";
    public static final String ATTRIBUTE_CONFIG_STEM = "etc:attribute";
    public static final String GOOGLE_CONFIG_STEM = ATTRIBUTE_CONFIG_STEM + ":" + GOOGLE_PROVISIONER;
    public static final String SYNC_TO_GOOGLE_NAME = GOOGLE_CONFIG_STEM + ":" + SYNC_TO_GOOGLE;

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

    /** Boolean used to delay change log processing when a full sync is running. */
    private static boolean fullSyncIsRunning;

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsChangeLogConsumer.class);


    /** The change log consumer name from the processor metadata. */
    private String name;

    /** Whether or not to retry a change log entry if an error occurs. */
    private boolean retryOnError = false;

    /** Whether or not to provision users. */
    private boolean provisionUsers = false;

    /** Whether or not to de-provision users. */
    private boolean deprovisionUsers = false;

    /** Google Directory service*/
    private Directory directory;

    /** Global instance of the HTTP transport. */
    private static HttpTransport httpTransport;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();


    private AttributeDefName syncAttribute;
    /**
     *
     * Constructor. Initializes the underlying {@link Directory}.
     *
     */
    public GoogleAppsChangeLogConsumer() throws GeneralSecurityException, IOException {
        initialize();
    }

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
        }

        //TODO: make the cache settings properties
        CacheManager.googleUsers().setCacheValidity(15);
        populateGooUsersCache(directory);

        CacheManager.googleGroups().setCacheValidity(15);
        populateGooGroupsCache(directory);

        CacheManager.grouperSubjects().setCacheValidity(15);
        CacheManager.grouperSubjects().seed(CacheManager.googleUsers().size());

        CacheManager.grouperGroups().setCacheValidity(15);
        CacheManager.grouperGroups().seed(CacheManager.googleGroups().size());

        // retry on error
        retryOnError = GrouperLoaderConfig.getPropertyBoolean("changeLog.consumer.google.retryOnError", false);
        LOG.debug("Google Apps Consumer - Setting retry on error to {}", retryOnError);
    }

    /**
     * Returns true if a change log entry should be retried upon error.
     *
     * @return Returns true if a change log entry should be retried upon error.
     */
    public boolean isRetryOnError() {
        return retryOnError;
    }

    /** {@inheritDoc} */
    @Override
    public long processChangeLogEntries(final List<ChangeLogEntry> changeLogEntryList,
                                        ChangeLogProcessorMetadata changeLogProcessorMetadata) {

        GrouperSession grouperSession = null;

        //let's populate the caches, if necessary
        populateGooUsersCache(getDirectory());
        populateGooGroupsCache(getDirectory());

        // the change log sequence number to return
        long sequenceNumber = -1;

        try {
            // initialize this consumer's name from the change log metadata
            if (name == null) {
                name = changeLogProcessorMetadata.getConsumerName();
                LOG.trace("Google Apps Consumer '{}' - Setting name.", name);
            }

            grouperSession = GrouperSession.startRootSession();
            syncAttribute = getGoogleSyncAttribute();

            // time context processing
            final StopWatch stopWatch = new StopWatch();
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

        } finally {
            GrouperSession.stopQuietly(grouperSession);
        }

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
            final String enumKey =
                    changeLogEntry.getChangeLogType().getChangeLogCategory() + "__"
                            + changeLogEntry.getChangeLogType().getActionName();

            final EventType eventType = EventType.valueOf(enumKey);

            if (eventType == null) {
                LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Unsupported category and action.", name,
                        toString(changeLogEntry));
            } else {
                // process the change log event
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}'", name, toStringDeep(changeLogEntry));
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                eventType.process(this, changeLogEntry);

                stopWatch.stop();
                LOG.info("Google Apps Consumer '{}' - Change log entry '{}' Finished processing. Elapsed time {}",
                        new Object[] {name, toString(changeLogEntry), stopWatch,});

            }
        } catch (IllegalArgumentException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Unsupported category and action.", name,
                    toString(changeLogEntry));
        }
    }

    /**
     * Add an attribute value.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */

    protected void processAttributeAssignValueAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing add attribute assign value.", name,
                toString(changeLogEntry));

    }

    /**
     * Delete an attribute value.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processAttributeAssignValueDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry)  {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing delete attribute assign value.", name,
                toString(changeLogEntry));
    }


    /**
     * Add a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group add.", name, toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name);

        edu.internet2.middleware.grouper.Group  grouperGroup = fetchGrouperGroup(groupName);
        if (!shouldSyncGroup(grouperGroup)) {
            return;
        }

        final Group googleGroup = new Group();
        googleGroup.setName(grouperGroup.getDisplayExtension());
        googleGroup.setEmail(GoogleAppsUtils.qualifyAddress(groupName, true));
        googleGroup.setDescription(grouperGroup.getDescription());

        try {
            CacheManager.googleGroups().put(GoogleAppsUtils.addGroup(directory, googleGroup));
        } catch (IOException e) {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add: {}", Arrays.asList(name, toString(changeLogEntry), e));
        }

        //TODO: Apply other G Groups Config
    }

    /**
     * Delete a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group delete.", name, toString(changeLogEntry));

        //TODO: to archive or not to archive... that is the question!

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_DELETE.name);

        try {
            String groupKey = GoogleAppsUtils.qualifyAddress(groupName, true);
            GoogleAppsUtils.removeGroup(directory, groupKey);
            CacheManager.googleGroups().remove(groupKey);
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

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.name);
        final String propertyChanged = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyChanged);
        final String propertyOldValue = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyOldValue);
        final String propertyNewValue = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyNewValue);

        try {
            Group group = fetchGooGroup(GoogleAppsUtils.qualifyAddress(groupName, true));

            if (propertyChanged.equalsIgnoreCase("displayExtension")) {
                group.setName(propertyNewValue);

            } else if (propertyChanged.equalsIgnoreCase("description")) {
                group.setDescription(propertyNewValue);

            } else {
                LOG.warn("Google Apps Consumer '{}' - Change log entry '{}' Unmapped group property updated {}.",
                        Arrays.asList(name, toString(changeLogEntry)), propertyChanged);
            }

            CacheManager.googleGroups().put(GoogleAppsUtils.updateGroup(directory, GoogleAppsUtils.qualifyAddress(groupName, true), group));
        } catch (IOException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing group update.", name, toString(changeLogEntry));
        }
    }

    /**
     * Add a membership.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processMembershipAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
        final String ROLE = "MEMBER"; //Other types are ADMIN and OWNER. Neither makes sense for managed groups.

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing membership add.", name,
                toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName);
        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId);
        final String sourceId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.sourceId);

        final Subject lookupSubject = fetchGrouperSubject(sourceId, subjectId);
        final SubjectType subjectType = lookupSubject.getType();

        try {
            Group group = fetchGooGroup(GoogleAppsUtils.qualifyAddress(groupName, true));

            //For nested groups, ChangeLogEvents fire when the group is added, and also for each indirect user added,
            //so we only need to handle PERSON events.
            if (subjectType == SubjectTypeEnum.PERSON) {
                User user = fetchGooUser(GoogleAppsUtils.qualifyAddress(subjectId));
                if (user == null && provisionUsers) {
                    user = createUser(lookupSubject);
                }

                if (user != null) {
                    createMember(group, user, ROLE);
                }
            }

        } catch (IOException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership add failed: {}", Arrays.asList(name,
                    toString(changeLogEntry), e));
        }
    }

    /**
     * Delete a membership entry.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processMembershipDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {
        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing membership delete.", name,
                toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.groupName);
        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.subjectId);
        final String sourceId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.sourceId);

        final Subject lookupSubject = fetchGrouperSubject(sourceId, subjectId);
        final SubjectType subjectType = lookupSubject.getType();

        //For nested groups, ChangeLogEvents fire when the group is removed, and also for each indirect user added,
        //so we only need to handle PERSON events.
        if (subjectType == SubjectTypeEnum.PERSON) {
            try {
                Group group = fetchGooGroup(GoogleAppsUtils.qualifyAddress(groupName, true));
                GoogleAppsUtils.removeGroupMember(directory, group.getEmail(), GoogleAppsUtils.qualifyAddress(subjectId));
            } catch (IOException e) {
                LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership delete: {}", Arrays.asList(name,
                        toString(changeLogEntry), e));
            }
        }
    }

    /**
     * Add a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem add.", name, toString(changeLogEntry));

    }

    /**
     * Delete a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem delete.", name, toString(changeLogEntry));


    }

    /**
     * Update a stem.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    public void processStemUpdate(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem update.", name, toString(changeLogEntry));

    }

    /**
     * If true, retry a change log entry if an error occurs.
     *
     * @param retryOnError If true, retry a change log entry if an error occurs.
     */
    public void setRetryOnError(boolean retryOnError) {
        this.retryOnError = retryOnError;
    }

    private void populateGooUsersCache(Directory directory) {
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

    /**
     * Indicates if users should be provisioned if they aren't found in Google.
     *
     * @param provisionUsers If true, create missing users in Google
     */
    public void setProvisionUsers(boolean provisionUsers) {
        this.provisionUsers = provisionUsers;
    }


    private void populateGooGroupsCache(Directory directory) {
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


    private Group fetchGooGroup(String groupKey) throws IOException {
        Group group = CacheManager.googleGroups().get(groupKey);
        if (group == null) {
            group = GoogleAppsUtils.retrieveGroup(directory, groupKey);

            if (group != null) {
                CacheManager.googleGroups().put(group);
            }
        }

        return group;
    }

    private User fetchGooUser(String userKey) throws IOException {
        User user = CacheManager.googleUsers().get(userKey);
        if (user == null) {
            user = GoogleAppsUtils.retrieveUser(directory, userKey);

            if (user != null) {
                CacheManager.googleUsers().put(user);
            }
        }

        return user;
    }

    private edu.internet2.middleware.grouper.Group fetchGrouperGroup(String groupName) {
        edu.internet2.middleware.grouper.Group group = CacheManager.grouperGroups().get(groupName);
        if (group == null) {
            group = GroupFinder.findByName(GrouperSession.staticGrouperSession(false), groupName, false);

            if (group != null) {
                CacheManager.grouperGroups().put(group);
            }
        }

        return group;
    }

    private Subject fetchGrouperSubject(String sourceId, String subjectId) {
        Subject subject = CacheManager.grouperSubjects().get(sourceId + "__" + subjectId);
        if (subject == null) {
            subject = SubjectFinder.findByIdAndSource(subjectId, sourceId, false);

            if (subject != null) {
                CacheManager.grouperSubjects().put(subject);
            }
        }

        return subject;
    }

    private User createUser(Subject subject) throws IOException {
        User newUser = new User();
        newUser.setName(new UserName());
        //TODO: Parameterize the attributes
        String email = subject.getAttributeValue("email");
        newUser.setPrimaryEmail(email != null? email : GoogleAppsUtils.qualifyAddress(subject.getId()));
        //TODO: Configure Subject API to individual name components
        newUser.getName().setFamilyName(subject.getName().split(" ")[1]);
        newUser.getName().setGivenName(subject.getName().split(" ")[0]);
        newUser.getName().setFullName(subject.getName());
        newUser.setPassword(new BigInteger(130, new SecureRandom()).toString(32));

        newUser = GoogleAppsUtils.addUser(directory, newUser);
        CacheManager.googleUsers().put(newUser);
        return newUser;
    }

    private void createMember(Group group, User user, String role) throws IOException {
        final Member gMember = new Member();
        gMember.setEmail(user.getPrimaryEmail());
        gMember.setRole(role);

        GoogleAppsUtils.addGroupMember(directory, group, gMember);
    }

    private AttributeDefName getGoogleSyncAttribute() {
        LOG.debug("Google Apps Consumer '{}' - {} attribute not found, creating it now", name, SYNC_TO_GOOGLE_NAME);

        AttributeDefName attrDefName = AttributeDefNameFinder.findByName(SYNC_TO_GOOGLE_NAME, false);

        if (attrDefName == null) {
            LOG.info("Google Apps Consumer '{}' - {} attribute not found, creating it now", name, SYNC_TO_GOOGLE_NAME);
            Stem googleStem = StemFinder.findByName(GrouperSession.staticGrouperSession(), GOOGLE_CONFIG_STEM, false);

            if (googleStem == null) {
                LOG.info("Google Apps Consumer '{}' - {} stem not found, creating it now", name, GOOGLE_CONFIG_STEM);
                Stem etcAttributeStem = StemFinder.findByName(GrouperSession.staticGrouperSession(), ATTRIBUTE_CONFIG_STEM, false);
                googleStem = etcAttributeStem.addChildStem(GOOGLE_PROVISIONER, GOOGLE_PROVISIONER);
            }

            AttributeDef syncAttrDef = googleStem.addChildAttributeDef(SYNC_TO_GOOGLE + "Def", AttributeDefType.attr);
            syncAttrDef.setAssignToGroup(true);
            syncAttrDef.setAssignToStem(true);
            syncAttrDef.setMultiAssignable(true);
            syncAttrDef.store();

            attrDefName = googleStem.addChildAttributeDefName(syncAttrDef, SYNC_TO_GOOGLE, SYNC_TO_GOOGLE);
            LOG.info("Google Apps Consumer '{}' - {} attribute name created", name, SYNC_TO_GOOGLE_NAME);
        }

        return attrDefName;
    }

    private boolean shouldSyncGroup(edu.internet2.middleware.grouper.Group group) {
        boolean result;

        //TODO: Check Cache

        if (group.getAttributeDelegate().retrieveAssignments(syncAttribute).size() > 0) {
            result = true;
        } else {
            result = shouldSyncStem(group.getParentStem());
        }

        //TODO: Cache Result
        return result;
    }

    private boolean shouldSyncStem(Stem stem) {
        boolean result;

        //TODO: Check Cache

        if (stem.getAttributeDelegate().retrieveAssignments(syncAttribute).size() > 0) {
            result = true;
        } else if (stem.isRootStem()) {
            result = false;
        } else {
            result = shouldSyncStem(stem.getParentStem());
        }

        //TODO: Cache Result
        return result;
    }

    /**
     * Gets a simple string representation of the change log entry.
     *
     * @param changeLogEntry the change log entry
     * @return the simple string representation of the change log entry
     */
    private static String toString(ChangeLogEntry changeLogEntry) {
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
    private static String toStringDeep(ChangeLogEntry changeLogEntry) {
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

}
