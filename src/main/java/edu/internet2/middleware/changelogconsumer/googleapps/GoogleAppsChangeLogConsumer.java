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
import java.util.*;

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
import com.google.api.services.groupssettings.Groupssettings;
import com.google.api.services.groupssettings.model.Groups;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.Cache;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.GoogleCacheManager;
import edu.internet2.middleware.grouper.*;
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;
import edu.internet2.middleware.grouper.attr.AttributeDef;
import edu.internet2.middleware.grouper.attr.AttributeDefName;
import edu.internet2.middleware.grouper.attr.AttributeDefType;
import edu.internet2.middleware.grouper.attr.assign.AttributeAssign;
import edu.internet2.middleware.grouper.attr.assign.AttributeAssignType;
import edu.internet2.middleware.grouper.attr.finder.AttributeDefFinder;
import edu.internet2.middleware.grouper.attr.finder.AttributeDefNameFinder;
import edu.internet2.middleware.grouper.changeLog.*;
import edu.internet2.middleware.grouper.internal.dao.QueryOptions;
import edu.internet2.middleware.grouper.misc.GrouperDAOFactory;
import edu.internet2.middleware.grouper.util.GrouperUtil;
import edu.internet2.middleware.subject.Subject;
import edu.internet2.middleware.subject.SubjectType;
import edu.internet2.middleware.subject.provider.SubjectTypeEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.internet2.middleware.grouper.Stem.Scope;


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
    public static final String PARAMETER_NAMESPACE = "changeLog.consumer.";

    /** Maps change log entry category and action (change log type) to methods. */
    enum EventType {

        /** Process the add attribute assign value change log entry type. */
        attributeAssign__addAttributeAssign {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processAttributeAssignAdd(consumer, changeLogEntry);
            }
        },

        /** Process the delete attribute assign value change log entry type. */
        attributeAssign__deleteAttributeAssign {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processAttributeAssignDelete(consumer, changeLogEntry);
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

        /** Process the delete stem change log entry type. */
        stem__deleteStem {
            /** {@inheritDoc} */
            public void process(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) throws Exception {
                consumer.processStemDelete(consumer, changeLogEntry);
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

    /** "Boolean" used to delay change log processing when a full sync is running. */
    private static final HashMap<String, String> fullSyncIsRunning = new HashMap<String, String>();
    private static final Object fullSyncIsRunningLock = new Object();

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsChangeLogConsumer.class);


    /** The change log consumer name from the processor metadata. */
    private String name;

    /** Whether or not to retry a change log entry if an error occurs. */
    private boolean retryOnError = false;

    /** Whether or not to provision users. */
    private boolean provisionUsers;

    /** Whether or not to de-provision users. */
    private boolean deprovisionUsers;

    /** Whether to not use "split" to parse name or the subject API is used to get the name, see subjectGivenNameField and subjectSurnameField */
    private boolean simpleSubjectNaming;

    /** The givenName field to lookup with the Subject API */
    private String subjectGivenNameField;

    /** The surname field to lookup with the Subject API */
    private String subjectSurnameField;

    /** should the provisioned users be in the GAL*/
    private boolean includeUserInGlobalAddressList;

    /** should the provisioned groups be in the GAL*/
    private boolean includeGroupInGlobalAddressList;

    /** What to do with deleted Groups: archive, delete, ignore (default) */
    private String handleDeletedGroup;

    /** Google Directory services client*/
    private Directory directoryClient;

    /** Google Groupssettings services client*/
    private Groupssettings groupssettingsClient;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private AttributeDefName syncAttribute;

    private AddressFormatter addressFormatter = new AddressFormatter();

    //The Google Objects hang around a lot longer due to Google API constraints, so they are stored in a static GoogleCacheManager class.
    //Grouper ones are easier to refresh.
    private Cache<Subject> grouperSubjects = new Cache<Subject>();
    private Cache<edu.internet2.middleware.grouper.Group> grouperGroups = new Cache<edu.internet2.middleware.grouper.Group>();
    private HashMap<String, String> syncedObjects = new HashMap<String, String>();

    public GoogleAppsChangeLogConsumer() {
        LOG.debug("Google Apps Consumer - new");
    }

    protected void initialize() throws GeneralSecurityException, IOException {
        final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        final String qualifiedParameterNamespace = PARAMETER_NAMESPACE + this.name + ".";

        final String serviceAccountPKCS12FilePath =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "serviceAccountPKCS12FilePath");
        LOG.debug("Google Apps Consumer - Setting Google serviceAccountPKCS12FilePath to {}", serviceAccountPKCS12FilePath);

        final String serviceAccountEmail =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "serviceAccountEmail");
        LOG.debug("Google Apps Consumer - Setting Google serviceAccountEmail on error to {}", serviceAccountEmail);

        final String serviceImpersonationUser =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "serviceImpersonationUser");
        LOG.debug("Google Apps Consumer - Setting Google serviceImpersonationUser to {}", serviceImpersonationUser);
        final GoogleCredential googleDirectoryCredential = GoogleAppsSdkUtils.getGoogleDirectoryCredential(serviceAccountEmail,
                serviceAccountPKCS12FilePath, serviceImpersonationUser, httpTransport, JSON_FACTORY);

        final GoogleCredential googleGroupssettingsCredential = GoogleAppsSdkUtils.getGoogleGroupssettingsCredential(serviceAccountEmail,
                serviceAccountPKCS12FilePath, serviceImpersonationUser, httpTransport, JSON_FACTORY);

        directoryClient = new Directory.Builder(httpTransport, JSON_FACTORY, googleDirectoryCredential)
                .setApplicationName("Google Apps Grouper Provisioner")
                .build();

        groupssettingsClient = new Groupssettings.Builder(httpTransport, JSON_FACTORY, googleGroupssettingsCredential)
                .setApplicationName("Google Apps Grouper Provisioner")
                .build();

        String googleDomain =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "domain");
        LOG.debug("Google Apps Consumer - Setting Google domain to {}", googleDomain);

        final String groupIdentifierExpression =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "groupIdentifierExpression");
        LOG.debug("Google Apps Consumer - Setting groupIdentifierExpression to {}", groupIdentifierExpression);

        final String subjectIdentifierExpression =
                GrouperLoaderConfig.retrieveConfig().propertyValueStringRequired(qualifiedParameterNamespace + "subjectIdentifierExpression");
        LOG.debug("Google Apps Consumer - Setting subjectIdentifierExpression to {}", subjectIdentifierExpression);

        addressFormatter.setGroupIdentifierExpression(groupIdentifierExpression)
                .setSubjectIdentifierExpression(subjectIdentifierExpression)
                .setDomain(googleDomain);

        provisionUsers =
                GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(qualifiedParameterNamespace + "provisionUsers", false);
        LOG.debug("Google Apps Consumer - Setting provisionUser to {}", provisionUsers);

        deprovisionUsers =
                GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(qualifiedParameterNamespace + "deprovisionUsers", false);
        LOG.debug("Google Apps Consumer - Setting deprovisionUser to {}", deprovisionUsers);

        includeUserInGlobalAddressList =
                GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(qualifiedParameterNamespace + "includeUserInGlobalAddressList", true);
        LOG.debug("Google Apps Consumer - Setting includeUserInGlobalAddressList to {}", includeUserInGlobalAddressList);

        includeGroupInGlobalAddressList =
                GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(qualifiedParameterNamespace + "includeGroupInGlobalAddressList", true);
        LOG.debug("Google Apps Consumer - Setting includeGroupInGlobalAddressList to {}", includeGroupInGlobalAddressList);

        simpleSubjectNaming =
                GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(qualifiedParameterNamespace + "simpleSubjectNaming", true);
        LOG.debug("Google Apps Consumer - Setting simpleSubjectNaming to {}", simpleSubjectNaming);

        subjectGivenNameField =
                GrouperLoaderConfig.retrieveConfig().propertyValueString(qualifiedParameterNamespace + "subjectGivenNameField", "givenName");
        LOG.debug("Google Apps Consumer - Setting subjectGivenNameField to {}", subjectGivenNameField);

        subjectSurnameField =
                GrouperLoaderConfig.retrieveConfig().propertyValueString(qualifiedParameterNamespace + "subjectSurnameField" ,"sn");
        LOG.debug("Google Apps Consumer - Setting subjectSurnameField to {}", subjectSurnameField);

        final int googleUserCacheValidity =
                GrouperLoaderConfig.retrieveConfig().propertyValueInt(qualifiedParameterNamespace + "googleUserCacheValidityPeriod", 30);
        LOG.debug("Google Apps Consumer - Setting googleUserCacheValidityPeriod to {}", googleUserCacheValidity);

        final int googleGroupCacheValidity =
                GrouperLoaderConfig.retrieveConfig().propertyValueInt(qualifiedParameterNamespace + "googleGroupCacheValidityPeriod", 30);
        LOG.debug("Google Apps Consumer - Setting googleGroupCacheValidityPeriod to {}", googleGroupCacheValidity);

        handleDeletedGroup =
                GrouperLoaderConfig.retrieveConfig().propertyValueString(qualifiedParameterNamespace + "handleDeletedGroup", "ignore");
        LOG.debug("Google Apps Consumer - Setting handleDeletedGroup to {}", handleDeletedGroup);

        GoogleCacheManager.googleUsers().setCacheValidity(googleUserCacheValidity);
        populateGooUsersCache(directoryClient);

        GoogleCacheManager.googleGroups().setCacheValidity(googleGroupCacheValidity);
        populateGooGroupsCache(directoryClient);

        grouperSubjects.setCacheValidity(5);
        grouperSubjects.seed(1000);

        grouperGroups.setCacheValidity(5);
        grouperGroups.seed(100);

        // retry on error
        retryOnError = GrouperLoaderConfig.retrieveConfig().propertyValueBoolean(PARAMETER_NAMESPACE + "retryOnError", false);
        LOG.debug("Google Apps Consumer - Setting retry on error to {}", retryOnError);
    }

    /**
     * Runs a fullSync.
     * @param consumerName the consumerName
     * @param dryRun indicates that this is dryRun
     */
    public void fullSync(String consumerName, boolean dryRun) {
        this.name = consumerName;

        synchronized (fullSyncIsRunningLock) {
            fullSyncIsRunning.put(name, Boolean.toString(true));
        }

        //Start with a clean cache
        GoogleCacheManager.googleGroups().clear();
        GoogleCacheManager.googleGroups().clear();

        try {
            initialize();
        } catch (GeneralSecurityException e) {
            LOG.error("Google Apps Consume '{}' Full Sync - This consumer failed to initialize: {}", name, e.getMessage());
        } catch (IOException e) {
            LOG.error("Google Apps Consume '{}' Full Sync - This consumer failed to initialize: {}", name, e.getMessage());
        }

        GrouperSession grouperSession = null;
            try {
                grouperSession = GrouperSession.startRootSession();
                syncAttribute = getGoogleSyncAttribute();
                cacheSynedObjects(true);

                // time context processing
                final StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                //Populate a normalized list (google naming) of Grouper groups
                ArrayList<ComparableGroupItem> grouperGroups = new ArrayList<ComparableGroupItem>();
                for (String groupKey : syncedObjects.keySet()) {
                    if (syncedObjects.get(groupKey).equalsIgnoreCase("yes")) {
                        edu.internet2.middleware.grouper.Group group = fetchGrouperGroup(groupKey);

                        if (group != null) {
                            grouperGroups.add(new ComparableGroupItem(addressFormatter.qualifyGroupAddress(group.getName()), group));
                        }
                    }
                }

                //Populate a comparable list of Google groups
                ArrayList<ComparableGroupItem> googleGroups = new ArrayList<ComparableGroupItem>();
                for (String groupName : GoogleCacheManager.googleGroups().getKeySet()) {
                    googleGroups.add(new ComparableGroupItem(groupName));
                }

                //Get our sets
                Collection<ComparableGroupItem> extraGroups = CollectionUtils.subtract(googleGroups, grouperGroups);
                for (ComparableGroupItem item : extraGroups) {
                    LOG.info("Google Apps Consumer '{}' Full Sync - extra Google group: {}", name, item);

                    if (!dryRun) {
                        try {
                            deleteGroupByEmail(item.getName());
                        } catch (IOException e) {
                            LOG.error("Google Apps Consume '{}' Full Sync - Error removing extra group ({}): {}", new Object[]{name, item.getName(), e.getMessage()});
                        }
                    }
                }

                Collection<ComparableGroupItem> missingGroups = CollectionUtils.subtract(grouperGroups, googleGroups);
                for (ComparableGroupItem item : missingGroups) {
                    LOG.info("Google Apps Consumer '{}' Full Sync - missing Google group: {} ({})", new Object[] {name, item.getGrouperGroup().getName(), item});

                    if (!dryRun) {
                        try {
                            createGroupIfNecessary(item.getGrouperGroup());
                        } catch (IOException e) {
                            LOG.error("Google Apps Consume '{}' Full Sync - Error adding missing group ({}): {}", new Object[]{name, item.getName(), e.getMessage()});
                        }
                    }
                }

                Collection<ComparableGroupItem> matchedGroups = CollectionUtils.intersection(grouperGroups, googleGroups);
                for (ComparableGroupItem item : matchedGroups) {
                    LOG.info("Google Apps Consumer '{}' Full Sync - matched group: {} ({})", new Object[] {name, item.getGrouperGroup().getName(), item});

                    Group gooGroup = null;
                    try {
                        gooGroup = fetchGooGroup(item.getName());
                    } catch (IOException e) {
                        LOG.error("Google Apps Consume '{}' Full Sync - Error fetching matched group ({}): {}", new Object[]{name, item.getName(), e.getMessage()});
                    }
                    boolean updated = false;

                    if (item.getGrouperGroup().getDescription().equalsIgnoreCase(gooGroup.getDescription())) {
                        if (!dryRun) {
                            gooGroup.setDescription(item.getGrouperGroup().getDescription());
                            updated = true;
                        }
                    }

                    if (item.getGrouperGroup().getDisplayExtension().equalsIgnoreCase(gooGroup.getName())) {
                        if (!dryRun) {
                            gooGroup.setName(item.getGrouperGroup().getDisplayExtension());
                            updated = true;
                        }
                    }

                    if (updated) {
                        try {
                            GoogleAppsSdkUtils.updateGroup(directoryClient, item.getName(), gooGroup);
                        } catch (IOException e) {
                            LOG.error("Google Apps Consume '{}' Full Sync - Error updating matched group ({}): {}", new Object[]{name, item.getName(), e.getMessage()});
                        }
                    }

                    //Retrieve & Examine Membership
                    ArrayList<ComparableMemberItem> grouperMembers = new ArrayList<ComparableMemberItem>();
                    for (edu.internet2.middleware.grouper.Member member : item.getGrouperGroup().getEffectiveMembers()) {
                        grouperMembers.add(new ComparableMemberItem(addressFormatter.qualifySubjectAddress(member.getSubjectId()), member));
                    }

                    ArrayList<ComparableMemberItem> googleMembers = new ArrayList<ComparableMemberItem>();
                    List<Member> memberList = null;

                    try {
                        memberList = GoogleAppsSdkUtils.retrieveGroupMembers(directoryClient, item.getName());
                    } catch (IOException e) {
                        LOG.error("Google Apps Consume '{}' Full Sync - Error fetching membership list for group({}): {}", new Object[]{name, item.getName(), e.getMessage()});
                    }

                    for (Member member : memberList) {
                        googleMembers.add(new ComparableMemberItem(member.getEmail()));
                    }

                    Collection<ComparableMemberItem> extraMembers = CollectionUtils.subtract(googleMembers, grouperMembers);
                    for (ComparableMemberItem member : extraMembers) {
                        if (!dryRun) {
                            try {
                                GoogleAppsSdkUtils.removeGroupMember(directoryClient, item.getName(), member.getEmail());
                            } catch (IOException e) {
                                LOG.error("Google Apps Consume '{}' Full Sync - Error removing member ({}) from matched group ({}): {}", new Object[]{name, member.getEmail(), item.getName(), e.getMessage()});
                            }
                        }
                    }

                    Collection<ComparableMemberItem> missingMembers = CollectionUtils.subtract(grouperMembers, googleMembers);
                    for (ComparableMemberItem member : missingMembers) {
                        if (!dryRun) {
                            Subject subject = fetchGrouperSubject(member.getGrouperMember().getSubjectSourceId(), member.getGrouperMember().getSubjectId());
                            User user = fetchGooUser(member.getEmail());

                            if (user == null) {
                                try {
                                    user = createUser(subject);
                                } catch (IOException e) {
                                    LOG.error("Google Apps Consume '{}' Full Sync - Error creating missing user ({}) from extra group ({}): {}", new Object[]{name, member.getEmail(), item.getName(), e.getMessage()});
                                }
                            }

                            if (user != null) {
                                try {
                                    createMember(gooGroup, user, "MEMBER");
                                } catch (IOException e) {
                                    LOG.error("Google Apps Consume '{}' Full Sync - Error creating missing member ({}) from extra group ({}): {}", new Object[]{name, member.getEmail(), item.getName(), e.getMessage()});
                                }
                            }
                        }
                    }

                    Collection<ComparableMemberItem> matchedMembers = CollectionUtils.intersection(grouperMembers, googleMembers);
                    for (ComparableMemberItem member : matchedMembers) {
                        if (!dryRun) {
                            //check the privilege level when implemented
                        }
                    }

                }

                // stop the timer and log
                stopWatch.stop();
                LOG.debug("Google Apps Consumer '{}' Full Sync - Processed, Elapsed time {}", new Object[] {name, stopWatch});

            } finally {
                GrouperSession.stopQuietly(grouperSession);

                synchronized (fullSyncIsRunningLock) {
                    fullSyncIsRunning.put(name, Boolean.toString(true));
                }
            }

       /* } catch (GeneralSecurityException e) {
            LOG.error("Google Apps Consumer '{}' FullSync - This consumer failed to initialize: {}", name, e.getMessage());
        } catch (IOException e) {
            LOG.error("Google Apps Consume '{}' Full Sync - This consumer failed to initialize: {}", name, e.getMessage());
        }*/
    }

    /** {@inheritDoc} */
    @Override
    public long processChangeLogEntries(final List<ChangeLogEntry> changeLogEntryList,
                                        ChangeLogProcessorMetadata changeLogProcessorMetadata) {

        LOG.debug("Google Apps Consumer - waking up");

        // the change log sequence number to return
        long sequenceNumber = -1;

        // initialize this consumer's name from the change log metadata
        if (name == null) {
            name = changeLogProcessorMetadata.getConsumerName();
            LOG.trace("Google Apps Consumer '{}' - Setting name.", name);
        }

        try {
            initialize();
        } catch (Exception e) {
            LOG.error("Google Apps Consumer '{}' - This consumer failed to initialize: {}", name, e.getMessage());
            return changeLogEntryList.get(0).getSequenceNumber() - 1;
        }

        GrouperSession grouperSession = null;
        try {

            grouperSession = GrouperSession.startRootSession();
            syncAttribute = getGoogleSyncAttribute();
            cacheSynedObjects();

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
                boolean isFullSyncRunning;
                synchronized (fullSyncIsRunningLock) {
                    isFullSyncRunning = fullSyncIsRunning.get(name) != null && Boolean.valueOf(fullSyncIsRunning.get(name));
                }
                if (isFullSyncRunning) {
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
            final String enumKey = changeLogEntry.getChangeLogType().getChangeLogCategory() + "__"
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
     * Add an attribute.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processAttributeAssignAdd(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing add attribute assign value.", name,
                toString(changeLogEntry));

        final String attributeDefNameId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_ADD.attributeDefNameId);
        final String assignType = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_ADD.assignType);
        final String ownerId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_ADD.ownerId1);

        if (syncAttribute.getId().equalsIgnoreCase(attributeDefNameId)) {

            if (AttributeAssignType.valueOf(assignType) == AttributeAssignType.group) {
                final edu.internet2.middleware.grouper.Group group = GroupFinder.findByUuid(GrouperSession.staticGrouperSession(), ownerId, false);

                try {
                    createGroupIfNecessary(group);
                } catch (IOException e) {
                    LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add: {}", new Object[] {name, toString(changeLogEntry), e});
                }

            } else if (AttributeAssignType.valueOf(assignType) == AttributeAssignType.stem) {
                final Stem stem = StemFinder.findByUuid(GrouperSession.staticGrouperSession(), ownerId, false);
                final Set<edu.internet2.middleware.grouper.Group> groups = stem.getChildGroups(Scope.SUB);

                for (edu.internet2.middleware.grouper.Group group : groups) {
                    try {
                        createGroupIfNecessary(group);
                    } catch (IOException e) {
                        LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add, continuing: {}", new Object[] {name, toString(changeLogEntry), e});
                    }
                }
            }
        }
    }

    /**
     * Delete an attribute.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processAttributeAssignDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry)  {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing delete attribute assign value.", name,
                toString(changeLogEntry));

        final String attributeDefNameId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_DELETE.attributeDefNameId);
        final String assignType = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_DELETE.assignType);
        final String ownerId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.ATTRIBUTE_ASSIGN_DELETE.ownerId1);

        if (syncAttribute.getId().equalsIgnoreCase(attributeDefNameId)) {

            if (AttributeAssignType.valueOf(assignType) == AttributeAssignType.group) {
                final edu.internet2.middleware.grouper.Group group = GroupFinder.findByUuid(GrouperSession.staticGrouperSession(), ownerId, false);

                try {
                    deleteGroup(group);
                } catch (IOException e) {
                    LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add: {}", new Object[] {name, toString(changeLogEntry), e});
                }

            } else if (AttributeAssignType.valueOf(assignType) == AttributeAssignType.stem) {
                final Stem stem = StemFinder.findByUuid(GrouperSession.staticGrouperSession(), ownerId, false);
                final Set<edu.internet2.middleware.grouper.Group> groups = stem.getChildGroups(Scope.SUB);

                for (edu.internet2.middleware.grouper.Group group : groups) {
                    try {
                        deleteGroup(group);
                    } catch (IOException e) {
                        LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add, continuing: {}", new Object[] {name, toString(changeLogEntry), e});
                    }
                }
            }
        }
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
        final edu.internet2.middleware.grouper.Group group = fetchGrouperGroup(groupName);

        if (!shouldSyncGroup(group)) {
            return;
        }

        try {
            createGroupIfNecessary(group);
        } catch (IOException e) {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group add: {}",  new Object[] {name, toString(changeLogEntry), e});
        }

    }

    /**
     * Delete a group.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processGroupDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing group delete.", name, toString(changeLogEntry));

        final String groupName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_DELETE.name);
        final edu.internet2.middleware.grouper.Group grouperGroup = fetchGrouperGroup(groupName);

        if (!shouldSyncGroup(grouperGroup)) {
            return;
        }

        try {
            deleteGroup(grouperGroup);
        } catch (IOException e) {
            LOG.error("Google Apps Consumer '{}' - Change log entry '{}' Error processing group delete: {}", new Object[] {name, toString(changeLogEntry), e.getMessage()});
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

        Group group;
        edu.internet2.middleware.grouper.Group grouperGroup;

        try {
            //Group moves are a bit different than just a property change, let's take care of it now.
            if (propertyChanged.equalsIgnoreCase("name")) {
                String oldAddress = addressFormatter.qualifyGroupAddress(propertyOldValue);
                String newAddress = addressFormatter.qualifyGroupAddress(propertyNewValue);

                grouperGroup = fetchGrouperGroup(groupName);
                group = fetchGooGroup(oldAddress);

                if (group != null && shouldSyncGroup(grouperGroup)) {
                    group.setEmail(newAddress);

                    if (group.getAliases() == null) {
                        group.setAliases(new ArrayList<String>(1));
                    }

                    group.getAliases().add(oldAddress);

                    syncedObjects.remove(groupName);
                    GoogleCacheManager.googleGroups().remove(oldAddress);
                    GoogleCacheManager.googleGroups().put(GoogleAppsSdkUtils.updateGroup(directoryClient, oldAddress, group));
                }

                return;
            }

            grouperGroup = fetchGrouperGroup(groupName);
            if (!shouldSyncGroup(grouperGroup)) {
                return;
            }

            group = fetchGooGroup(addressFormatter.qualifyGroupAddress(groupName));

            if (propertyChanged.equalsIgnoreCase("displayExtension")) {
                group.setName(propertyNewValue);

            } else if (propertyChanged.equalsIgnoreCase("description")) {
                group.setDescription(propertyNewValue);

            } else {
                LOG.warn("Google Apps Consumer '{}' - Change log entry '{}' Unmapped group property updated {}.",
                        new Object[] {name, toString(changeLogEntry), propertyChanged});
            }

            GoogleCacheManager.googleGroups().put(GoogleAppsSdkUtils.updateGroup(directoryClient, addressFormatter.qualifyGroupAddress(groupName), group));

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
        final edu.internet2.middleware.grouper.Group grouperGroup = fetchGrouperGroup(groupName);

        if (!shouldSyncGroup(grouperGroup)) {
            return;
        }

        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId);
        final String sourceId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.sourceId);
        final Subject lookupSubject = fetchGrouperSubject(sourceId, subjectId);
        final SubjectType subjectType = lookupSubject.getType();

        try {
            Group group = fetchGooGroup(addressFormatter.qualifyGroupAddress(groupName));
            if (group == null) {
                createGroupIfNecessary(grouperGroup);
                group = fetchGooGroup(addressFormatter.qualifyGroupAddress(groupName));
            }

            //For nested groups, ChangeLogEvents fire when the group is added, and also for each indirect user added,
            //so we only need to handle PERSON events.
            if (subjectType == SubjectTypeEnum.PERSON) {
                User user = fetchGooUser(addressFormatter.qualifySubjectAddress(subjectId));
                if (user == null) {
                    user = createUser(lookupSubject);
                }

                if (user != null) {
                    createMember(group, user, ROLE);
                }
            }

        } catch (IOException e) {
            LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership add failed: {}", new Object[] {name,
                    toString(changeLogEntry), e});
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
        final edu.internet2.middleware.grouper.Group grouperGroup = fetchGrouperGroup(groupName);

        if (!shouldSyncGroup(grouperGroup)) {
            return;
        }

        final String subjectId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.subjectId);
        final String sourceId = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.sourceId);
        final Subject lookupSubject = fetchGrouperSubject(sourceId, subjectId);
        final SubjectType subjectType = lookupSubject.getType();

        //For nested groups, ChangeLogEvents fire when the group is removed, and also for each indirect user added,
        //so we only need to handle PERSON events.
        if (subjectType == SubjectTypeEnum.PERSON) {
            try {
                GoogleAppsSdkUtils.removeGroupMember(directoryClient, addressFormatter.qualifyGroupAddress(groupName), addressFormatter.qualifySubjectAddress(subjectId));

                if (deprovisionUsers) {
                    //FUTURE: check if the user has other memberships and if not, initiate the removal here.
                }
            } catch (IOException e) {
                LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Error processing membership delete: {}", new Object[] {name,
                        toString(changeLogEntry), e});
            }
        }
    }

    /**
     * Delete a stem, but we generally don't care since the stem has to be empty before it can be deleted.
     *
     * @param consumer the change log consumer
     * @param changeLogEntry the change log entry
     */
    protected void processStemDelete(GoogleAppsChangeLogConsumer consumer, ChangeLogEntry changeLogEntry) {

        LOG.debug("Google Apps Consumer '{}' - Change log entry '{}' Processing stem delete.", name, toString(changeLogEntry));

        final String stemName = changeLogEntry.retrieveValueForLabel(ChangeLogLabels.STEM_DELETE.name);

        syncedObjects.remove(stemName);
    }

    private void populateGooUsersCache(Directory directory) {
        LOG.debug("Google Apps Consumer '{}' - Populating the userCache.", name);

        if (GoogleCacheManager.googleUsers() == null || GoogleCacheManager.googleUsers().isExpired()) {
            try {
                final List<User> list = GoogleAppsSdkUtils.retrieveAllUsers(directoryClient);
                GoogleCacheManager.googleUsers().seed(list);

            } catch (GoogleJsonResponseException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            } catch (IOException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            }
        }
    }

    private void populateGooGroupsCache(Directory directory) {
        LOG.debug("Google Apps Consumer '{}' - Populating the userCache.", name);

        if (GoogleCacheManager.googleGroups() == null || GoogleCacheManager.googleGroups().isExpired()) {
            try {
                final List<Group> list = GoogleAppsSdkUtils.retrieveAllGroups(directoryClient);
                GoogleCacheManager.googleGroups().seed(list);

            } catch (GoogleJsonResponseException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            } catch (IOException e) {
                LOG.error("Google Apps Consumer '{}' - Something bad happened when populating the UserCache: {}", name, e);
            }
        }
    }

    private Group fetchGooGroup(String groupKey) throws IOException {
        Group group = GoogleCacheManager.googleGroups().get(groupKey);
        if (group == null) {
            group = GoogleAppsSdkUtils.retrieveGroup(directoryClient, groupKey);

            if (group != null) {
                GoogleCacheManager.googleGroups().put(group);
            }
        }

        return group;
    }

    private User fetchGooUser(String userKey) {
        User user = GoogleCacheManager.googleUsers().get(userKey);
        if (user == null) {
            try {
                user = GoogleAppsSdkUtils.retrieveUser(directoryClient, userKey);
            } catch (IOException e) {
                LOG.warn("Google Apps Consume '{}' - Error fetching user ({}) from Google: {}", new Object[]{name, userKey, e.getMessage()});
            }

            if (user != null) {
                GoogleCacheManager.googleUsers().put(user);
            }
        }

        return user;
    }

    private edu.internet2.middleware.grouper.Group fetchGrouperGroup(String groupName) {
        edu.internet2.middleware.grouper.Group group = grouperGroups.get(groupName);
        if (group == null) {
            group = GroupFinder.findByName(GrouperSession.staticGrouperSession(false), groupName, false);

            if (group != null) {
                grouperGroups.put(group);
            }
        }

        return group;
    }

    private Subject fetchGrouperSubject(String sourceId, String subjectId) {
        Subject subject = grouperSubjects.get(sourceId + "__" + subjectId);
        if (subject == null) {
            subject = SubjectFinder.findByIdAndSource(subjectId, sourceId, false);

            if (subject != null) {
                grouperSubjects.put(subject);
            }
        }

        return subject;
    }

    private User createUser(Subject subject) throws IOException {
        final String email = subject.getAttributeValue("email");
        final String subjectName = subject.getName();

        User newUser = null;
        if (provisionUsers) {
            newUser = new User();
            newUser.setPassword(new BigInteger(130, new SecureRandom()).toString(32));
            newUser.setPrimaryEmail(email != null ? email : addressFormatter.qualifySubjectAddress(subject.getId()));
            newUser.setIncludeInGlobalAddressList(includeUserInGlobalAddressList);
            newUser.setName(new UserName());
            newUser.getName().setFullName(subjectName);

            if (simpleSubjectNaming) {
                final String[] subjectNameSplit = subjectName.split(" ");
                newUser.getName().setFamilyName(subjectNameSplit[subjectNameSplit.length - 1]);
                newUser.getName().setGivenName(subjectNameSplit[0]);

            } else {
                newUser.getName().setFamilyName(subject.getAttributeValue(subjectSurnameField));
                newUser.getName().setGivenName(subject.getAttributeValue(subjectGivenNameField));
            }

            newUser = GoogleAppsSdkUtils.addUser(directoryClient, newUser);
            GoogleCacheManager.googleUsers().put(newUser);
        }

        return newUser;
    }

    private void createMember(Group group, User user, String role) throws IOException {
        final Member gMember = new Member();
        gMember.setEmail(user.getPrimaryEmail());
        gMember.setRole(role);

        GoogleAppsSdkUtils.addGroupMember(directoryClient, group, gMember);
    }

    private void createGroupIfNecessary(edu.internet2.middleware.grouper.Group grouperGroup) throws IOException {
        final String groupKey = addressFormatter.qualifyGroupAddress(grouperGroup.getName());

        Group googleGroup = fetchGooGroup(groupKey);
        if (googleGroup == null) {
            googleGroup = new Group();
            googleGroup.setName(grouperGroup.getDisplayExtension());
            googleGroup.setEmail(groupKey);
            googleGroup.setDescription(grouperGroup.getDescription());

            GoogleCacheManager.googleGroups().put(GoogleAppsSdkUtils.addGroup(directoryClient, googleGroup));

            Set<edu.internet2.middleware.grouper.Member> members = grouperGroup.getEffectiveMembers();
            for (edu.internet2.middleware.grouper.Member member : members) {
                Subject subject = fetchGrouperSubject(member.getSubjectId(), member.getSubjectSourceId());
                String userKey = addressFormatter.qualifySubjectAddress(subject.getId());
                User user = fetchGooUser(userKey);

                if (user == null) {
                    user = createUser(subject);
                }

                if (user != null) {
                    createMember(googleGroup, user, "MEMBER");
                }
            }
        } else {
            Groups groupssettings = GoogleAppsSdkUtils.retrieveGroupSettings(groupssettingsClient, groupKey);

            if (groupssettings.getArchiveOnly().equalsIgnoreCase("true")) {
                groupssettings.setArchiveOnly("false");
                GoogleAppsSdkUtils.updateGroupSettings(groupssettingsClient, groupKey, groupssettings);
            }
        }
    }

    private void deleteGroup(edu.internet2.middleware.grouper.Group group) throws IOException {
        deleteGroupByName(group.getName());
    }

    private void deleteGroupByName(String groupName) throws IOException {
        final String groupKey = addressFormatter.qualifyGroupAddress(groupName);
        deleteGroupByEmail(groupKey);

        grouperGroups.remove(groupName);
        syncedObjects.remove(groupName);
    }

    private void deleteGroupByEmail(String groupKey) throws IOException {
        if (handleDeletedGroup.equalsIgnoreCase("archive")) {
            Groups gs = GoogleAppsSdkUtils.retrieveGroupSettings(groupssettingsClient, groupKey);
            gs.setArchiveOnly("true");
            GoogleAppsSdkUtils.updateGroupSettings(groupssettingsClient, groupKey, gs);

        } else if (handleDeletedGroup.equalsIgnoreCase("delete")) {
            GoogleAppsSdkUtils.removeGroup(directoryClient, groupKey);
            GoogleCacheManager.googleGroups().remove(groupKey);
        }
        //else "ignore" (we do nothing)

    }


    /**
     * Finds the AttributeDefName specific to this GoogleApps ChangeLog Consumer instance.
     * @return The AttributeDefName for this GoogleApps ChangeLog Consumer
     */
    private AttributeDefName getGoogleSyncAttribute() {
        LOG.debug("Google Apps Consumer '{}' - looking for attribute: {}", name, SYNC_TO_GOOGLE_NAME + name);

        AttributeDefName attrDefName = AttributeDefNameFinder.findByName(SYNC_TO_GOOGLE_NAME + name, false);

        if (attrDefName == null) {
            Stem googleStem = StemFinder.findByName(GrouperSession.staticGrouperSession(), GOOGLE_CONFIG_STEM, false);

            if (googleStem == null) {
                LOG.info("Google Apps Consumer '{}' - {} stem not found, creating it now", name, GOOGLE_CONFIG_STEM);
                final Stem etcAttributeStem = StemFinder.findByName(GrouperSession.staticGrouperSession(), ATTRIBUTE_CONFIG_STEM, false);
                googleStem = etcAttributeStem.addChildStem(GOOGLE_PROVISIONER, GOOGLE_PROVISIONER);
            }

            AttributeDef syncAttrDef = AttributeDefFinder.findByName(SYNC_TO_GOOGLE_NAME + "Def", false);
            if (syncAttrDef == null) {
                LOG.info("Google Apps Consumer '{}' - {} AttributeDef not found, creating it now", name, SYNC_TO_GOOGLE + "Def");
                syncAttrDef = googleStem.addChildAttributeDef(SYNC_TO_GOOGLE + "Def", AttributeDefType.attr);
                syncAttrDef.setAssignToGroup(true);
                syncAttrDef.setAssignToStem(true);
                syncAttrDef.setMultiAssignable(true);
                syncAttrDef.store();
            }

            LOG.info("Google Apps Consumer '{}' - {} attribute not found, creating it now", name, SYNC_TO_GOOGLE_NAME + name);
            attrDefName = googleStem.addChildAttributeDefName(syncAttrDef, SYNC_TO_GOOGLE + name, SYNC_TO_GOOGLE + name);
        }

        return attrDefName;
    }

    private boolean shouldSyncGroup(edu.internet2.middleware.grouper.Group group) {
        boolean result;

        final String groupName = group.getName();

        if (syncedObjects.containsKey(groupName)) {
            result = syncedObjects.get(groupName).equalsIgnoreCase("yes");

        } else {
            result = group.getAttributeDelegate().retrieveAssignments(syncAttribute).size() > 0 || shouldSyncStem(group.getParentStem());
            syncedObjects.put(groupName, result ? "yes" : "no");
        }

        return result;
    }

    private boolean shouldSyncStem(Stem stem) {
        boolean result;

        final String stemName = stem.getName();

        if (syncedObjects.containsKey(stemName)) {
            result = syncedObjects.get(stemName).equalsIgnoreCase("yes");

        } else {
            result = stem.getAttributeDelegate().retrieveAssignments(syncAttribute).size() > 0 || !stem.isRootStem() && shouldSyncStem(stem.getParentStem());

            syncedObjects.put(stemName, result ? "yes" : "no");
        }

        return result;
    }

    private void cacheSynedObjects() {
       cacheSynedObjects(false);
    }
    private void cacheSynedObjects(boolean fullyPopulate) {
        /* Future: API 2.3.0 has support for getting a list of stems and groups using the Finder objects. */

        final ArrayList<String> ids = new ArrayList<String>();

        //First the users
        Set<AttributeAssign> attributeAssigns = GrouperDAOFactory.getFactory()
                .getAttributeAssign().findStemAttributeAssignments(null, null, GrouperUtil.toSet(syncAttribute.getId()), null, null, true, false);

        for (AttributeAssign attributeAssign : attributeAssigns) {
            ids.add(attributeAssign.getOwnerStemId());
        }
        final Set<Stem> stems = StemFinder.findByUuids(GrouperSession.staticGrouperSession(), ids, new QueryOptions());
        for (Stem stem : stems) {
            syncedObjects.put(stem.getName(), "yes");

            if (fullyPopulate) {
                for (edu.internet2.middleware.grouper.Group group : stem.getChildGroups(Scope.SUB)) {
                    syncedObjects.put(group.getName(), "yes");
                }
            }
        }

        //Now for the Groups
        attributeAssigns = GrouperDAOFactory.getFactory()
                .getAttributeAssign().findGroupAttributeAssignments(null, null, GrouperUtil.toSet(syncAttribute.getId()), null, null, true, false);

        for (AttributeAssign attributeAssign : attributeAssigns) {
            final edu.internet2.middleware.grouper.Group group = GroupFinder.findByUuid(GrouperSession.staticGrouperSession(), attributeAssign.getOwnerGroupId(), false);
            syncedObjects.put(group.getName(), "yes");
        }
    }

    /**
     * Gets a simple string representation of the change log entry.
     *
     * @param changeLogEntry the change log entry
     * @return the simple string representation of the change log entry
     */
    private static String toString(ChangeLogEntry changeLogEntry) {
        final ToStringBuilder toStringBuilder = new ToStringBuilder(changeLogEntry, ToStringStyle.SHORT_PREFIX_STYLE);
        toStringBuilder.append("timestamp", changeLogEntry.getCreatedOn());
        toStringBuilder.append("sequence", changeLogEntry.getSequenceNumber());
        toStringBuilder.append("category", changeLogEntry.getChangeLogType().getChangeLogCategory());
        toStringBuilder.append("actionName", changeLogEntry.getChangeLogType().getActionName());
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
        final ToStringBuilder toStringBuilder = new ToStringBuilder(changeLogEntry, ToStringStyle.SHORT_PREFIX_STYLE);
        toStringBuilder.append("timestamp", changeLogEntry.getCreatedOn());
        toStringBuilder.append("sequence", changeLogEntry.getSequenceNumber());
        toStringBuilder.append("category", changeLogEntry.getChangeLogType().getChangeLogCategory());
        toStringBuilder.append("actionName", changeLogEntry.getChangeLogType().getActionName());
        toStringBuilder.append("contextId", changeLogEntry.getContextId());

        final ChangeLogType changeLogType = changeLogEntry.getChangeLogType();

        for (String label : changeLogType.labels()) {
            toStringBuilder.append(label, changeLogEntry.retrieveValueForLabel(label));
        }

        return toStringBuilder.toString();
    }

}
