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

import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.Member;
import com.google.api.services.admin.directory.model.User;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.GoogleCacheManager;
import edu.internet2.middleware.changelogconsumer.googleapps.utils.ComparableGroupItem;
import edu.internet2.middleware.changelogconsumer.googleapps.utils.ComparableMemberItem;
import edu.internet2.middleware.changelogconsumer.googleapps.utils.GoogleAppsSyncProperties;
import edu.internet2.middleware.grouper.GrouperSession;
import edu.internet2.middleware.grouper.attr.AttributeDefName;
import edu.internet2.middleware.subject.Subject;
import edu.internet2.middleware.subject.provider.SubjectTypeEnum;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initiates a GoogleAppsFullSync from command-line
 *
 * @author John Gasper, Unicon
 */
public class GoogleAppsFullSync {

    public static void main(String[] args) {
        if (args.length == 0 ) {
            System.console().printf("Google Change Log Consumer Name must be provided\n");
            System.console().printf("*nix: googleAppsFullSync.sh consumerName [--dry-run]\n");
            System.console().printf("Windows: googleAppsFullSync.bat consumerName [--dry-run]\n");

            System.exit(-1);
        }

        GoogleAppsFullSync fullSync = new GoogleAppsFullSync(args[0], args.length > 1 && args[1].equalsIgnoreCase("--dry-run"));
    }

    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsFullSync.class);

    /** "Boolean" used to delay change log processing when a full sync is running. */
    private static final HashMap<String, String> fullSyncIsRunning = new HashMap<String, String>();
    private static final Object fullSyncIsRunningLock = new Object();

    private AttributeDefName syncAttribute;

    private GoogleGrouperConnector connector;

    private String consumerName;

    public GoogleAppsFullSync(String consumerName, boolean dryRun) {
        this.consumerName = consumerName;

        try {
            fullSync(dryRun);

        } catch (Exception e) {
            System.console().printf(e.toString() + ": \n");
            e.printStackTrace();
        }

        System.exit(0);
    }

    /**
     * Runs a fullSync.
     * @param dryRun indicates that this is dryRun
     */
    public void fullSync(boolean dryRun) {

        synchronized (fullSyncIsRunningLock) {
            fullSyncIsRunning.put(consumerName, Boolean.toString(true));
        }

        connector = new GoogleGrouperConnector();

        //Start with a clean cache
        GoogleCacheManager.googleGroups().clear();
        GoogleCacheManager.googleGroups().clear();

        GoogleAppsSyncProperties properties = new GoogleAppsSyncProperties(consumerName);

        try {
            connector.initialize(consumerName, properties);

        } catch (GeneralSecurityException e) {
            LOG.error("Google Apps Consume '{}' Full Sync - This consumer failed to initialize: {}", consumerName, e.getMessage());
        } catch (IOException e) {
            LOG.error("Google Apps Consume '{}' Full Sync - This consumer failed to initialize: {}", consumerName, e.getMessage());
        }

        GrouperSession grouperSession = null;

        try {
            grouperSession = GrouperSession.startRootSession();
            syncAttribute = connector.getGoogleSyncAttribute();
            connector.cacheSynedObjects(true);

            // time context processing
            final StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            //Populate a normalized list (google naming) of Grouper groups
            ArrayList<ComparableGroupItem> grouperGroups = new ArrayList<ComparableGroupItem>();
            for (String groupKey : connector.getSyncedGroupsAndStems().keySet()) {
                if (connector.getSyncedGroupsAndStems().get(groupKey).equalsIgnoreCase("yes")) {
                    edu.internet2.middleware.grouper.Group group = connector.fetchGrouperGroup(groupKey);

                    if (group != null) {
                        grouperGroups.add(new ComparableGroupItem(connector.getAddressFormatter().qualifyGroupAddress(group.getName()), group));
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
                LOG.info("Google Apps Consumer '{}' Full Sync - removing extra Google group: {}", consumerName, item);

                if (!dryRun) {
                    try {
                        connector.deleteGooGroupByEmail(item.getName());
                    } catch (IOException e) {
                        LOG.error("Google Apps Consume '{}' Full Sync - Error removing extra group ({}): {}", new Object[]{consumerName, item.getName(), e.getMessage()});
                    }
                }
            }

            Collection<ComparableGroupItem> missingGroups = CollectionUtils.subtract(grouperGroups, googleGroups);
            for (ComparableGroupItem item : missingGroups) {
                LOG.info("Google Apps Consumer '{}' Full Sync - adding missing Google group: {} ({})", new Object[] {consumerName, item.getGrouperGroup().getName(), item});

                if (!dryRun) {
                    try {
                        connector.createGooGroupIfNecessary(item.getGrouperGroup());
                    } catch (IOException e) {
                        LOG.error("Google Apps Consume '{}' Full Sync - Error adding missing group ({}): {}", new Object[]{consumerName, item.getName(), e.getMessage()});
                    }
                }
            }

            Collection<ComparableGroupItem> matchedGroups = CollectionUtils.intersection(grouperGroups, googleGroups);
            for (ComparableGroupItem item : matchedGroups) {
                LOG.info("Google Apps Consumer '{}' Full Sync - examining matched group: {} ({})", new Object[] {consumerName, item.getGrouperGroup().getName(), item});

                Group gooGroup = null;
                try {
                    gooGroup = connector.fetchGooGroup(item.getName());
                } catch (IOException e) {
                    LOG.error("Google Apps Consume '{}' Full Sync - Error fetching matched group ({}): {}", new Object[]{consumerName, item.getName(), e.getMessage()});
                }
                boolean updated = false;

                if (!item.getGrouperGroup().getDescription().equalsIgnoreCase(gooGroup.getDescription())) {
                    if (!dryRun) {
                        gooGroup.setDescription(item.getGrouperGroup().getDescription());
                        updated = true;
                    }
                }

                if (!item.getGrouperGroup().getDisplayExtension().equalsIgnoreCase(gooGroup.getName())) {
                    if (!dryRun) {
                        gooGroup.setName(item.getGrouperGroup().getDisplayExtension());
                        updated = true;
                    }
                }

                if (updated) {
                    try {
                        connector.updateGooGroup(item.getName(), gooGroup);
                    } catch (IOException e) {
                        LOG.error("Google Apps Consume '{}' Full Sync - Error updating matched group ({}): {}", new Object[]{consumerName, item.getName(), e.getMessage()});
                    }
                }

                //Retrieve & Examine Membership
                ArrayList<ComparableMemberItem> grouperMembers = new ArrayList<ComparableMemberItem>();
                for (edu.internet2.middleware.grouper.Member member : item.getGrouperGroup().getMembers()) {
                    if (member.getSubjectType() == SubjectTypeEnum.PERSON) {
                        grouperMembers.add(new ComparableMemberItem(connector.getAddressFormatter().qualifySubjectAddress(member.getSubjectId()), member));
                    }
                }

                ArrayList<ComparableMemberItem> googleMembers = new ArrayList<ComparableMemberItem>();
                List<Member> memberList = null;

                try {
                    memberList = connector.getGooMembership(item.getName());
                } catch (IOException e) {
                    LOG.error("Google Apps Consume '{}' Full Sync - Error fetching membership list for group({}): {}", new Object[]{consumerName, item.getName(), e.getMessage()});
                }

                for (Member member : memberList) {
                    googleMembers.add(new ComparableMemberItem(member.getEmail()));
                }

                Collection<ComparableMemberItem> extraMembers = CollectionUtils.subtract(googleMembers, grouperMembers);
                for (ComparableMemberItem member : extraMembers) {
                    LOG.info("Google Apps Consume '{}' Full Sync - Removing extra member ({}) from matched group ({})", new Object[]{consumerName, member.getEmail(), item.getName()});
                    if (!dryRun) {
                        try {
                            connector.removeGooMembership(item.getName(), member.getGrouperMember().getSubject());
                        } catch (IOException e) {
                            LOG.warn("Google Apps Consume '{}' - Error removing membership ({}) from Google Group ({}): {}", new Object[]{consumerName, member.getEmail(), item.getName(), e.getMessage()});
                        }
                    }
                }

                Collection<ComparableMemberItem> missingMembers = CollectionUtils.subtract(grouperMembers, googleMembers);
                for (ComparableMemberItem member : missingMembers) {
                    LOG.info("Google Apps Consume '{}' Full Sync - Creating missing user/member ({}) from extra group ({}).", new Object[]{consumerName, member.getEmail(), item.getName()});
                    if (!dryRun) {
                        Subject subject = connector.fetchGrouperSubject(member.getGrouperMember().getSubjectSourceId(), member.getGrouperMember().getSubjectId());
                        User user = connector.fetchGooUser(member.getEmail());

                        if (user == null) {
                            try {
                                user = connector.createGooUser(subject);
                            } catch (IOException e) {
                                LOG.error("Google Apps Consume '{}' Full Sync - Error creating missing user ({}) from extra group ({}): {}", new Object[]{consumerName, member.getEmail(), item.getName(), e.getMessage()});
                            }
                        }

                        if (user != null) {
                            try {
                                connector.createGooMember(gooGroup, user, "MEMBER");
                            } catch (IOException e) {
                                LOG.error("Google Apps Consume '{}' Full Sync - Error creating missing member ({}) from extra group ({}): {}", new Object[]{consumerName, member.getEmail(), item.getName(), e.getMessage()});
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
            LOG.debug("Google Apps Consumer '{}' Full Sync - Processed, Elapsed time {}", new Object[] {consumerName, stopWatch});

        } finally {
            GrouperSession.stopQuietly(grouperSession);

            synchronized (fullSyncIsRunningLock) {
                fullSyncIsRunning.put(consumerName, Boolean.toString(true));
            }
        }

    }

    public static boolean isFullSyncRunning(String consumerName) {
        synchronized (fullSyncIsRunningLock) {
            return fullSyncIsRunning.get(consumerName) != null && Boolean.valueOf(fullSyncIsRunning.get(consumerName));
        }
    }

}
