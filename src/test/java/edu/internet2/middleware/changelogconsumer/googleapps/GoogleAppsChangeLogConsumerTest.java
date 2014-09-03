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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.Member;
import com.google.api.services.admin.directory.model.User;
import com.google.api.services.admin.directory.model.UserName;
import edu.internet2.middleware.changelogconsumer.googleapps.cache.CacheManager;
import edu.internet2.middleware.grouper.SubjectFinder;
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;
import edu.internet2.middleware.grouper.changeLog.*;
import edu.internet2.middleware.subject.Subject;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "org.apache.log4j.*", "javax.xml.parsers.*", "org.apache.xerces.jaxp.*",  "org.xml.*", "net.sf.ehcache.*"})
@PrepareForTest(value = { GrouperLoaderConfig.class, GrouperLoaderConfig.class, Subject.class, SubjectFinder.class})
public class GoogleAppsChangeLogConsumerTest {

    private static final String groupName = "edu:internet2:grouper:provisioned:testGroup";

    private static final String subjectId = "testSubjectId";

    private static final String sourceId = "unitTest";

    private ChangeLogProcessorMetadata metadata;

    private static GoogleAppsChangeLogConsumer consumer;
    private static String googleDomain;

    @BeforeClass
    public static void setupClass() {
        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream("unit-test.properties");
        try {
            props.load(is);
        }
        catch (IOException e) {
            System.out.println("test.properties configuration not found. Try again! Love, Grumpy Cat");
        }

        googleDomain = props.getProperty("DOMAIN");

        mockStatic(GrouperLoaderConfig.class);
        when(GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountPKCS12FilePath", true))
                .thenReturn(props.getProperty("SERVICE_ACCOUNT_PKCS_12_FILE_PATH"));
        when(GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountEmail", true))
                .thenReturn(props.getProperty("SERVICE_ACCOUNT_EMAIL"));
        when(GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.serviceAccountUser", true))
                .thenReturn(props.getProperty("SERVICE_ACCOUNT_USER"));
        when(GrouperLoaderConfig.getPropertyString("changeLog.consumer.google.domain", true))
                .thenReturn(googleDomain);

        mockStatic(SubjectFinder.class);

        try {
            consumer = new GoogleAppsChangeLogConsumer();
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() throws GeneralSecurityException, IOException {
        metadata = mock(ChangeLogProcessorMetadata.class);
        when(metadata.getConsumerName()).thenReturn("UnitTestConsumer");

    }

    @After
    public void teardown() throws GeneralSecurityException, IOException {
        try {
            GoogleAppsUtils.removeGroup(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-"));
        } catch (IOException e) {

        }

        try {
            GoogleAppsUtils.removeUser(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(subjectId));
        } catch (IOException e) {

        }

        CacheManager.googleGroups().clear();
        CacheManager.googleUsers().clear();
    }

    @Test
    public void testProcessGroupAdd() throws GeneralSecurityException, IOException {
        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("group", "addGroup", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name)).thenReturn(groupName);
        when(addEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(addEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);
        Group group = GoogleAppsUtils.retrieveGroup(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-"));
        assertNotNull(group);
        assertTrue(group.getName().equalsIgnoreCase(groupName));
    }


    @Test
    public void testProcessGroupUpdate() throws GeneralSecurityException, IOException {
        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("group", "updateGroup", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.name)).thenReturn(groupName);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyOldValue)).thenReturn(groupName);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_UPDATE.propertyNewValue)).thenReturn("test");
        when(addEntry.getContextId()).thenReturn("123456789");

        //TODO: Name Change
        //TODO: ID Change
        //TODO: Description Change
        //TODO: Privilege Change
    }


    @Test
    public void testProcessGroupMemberAddExisting() throws GeneralSecurityException, IOException {
        //User already exists in Google
        createTestGroup(groupName);
        createTestUser(GoogleAppsUtils.qualifyAddress(subjectId), "testgn", "testfm");

        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("membership", "addMembership", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName)).thenReturn(groupName);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId)).thenReturn(subjectId);
        when(addEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(addEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);

        List<Member> members = GoogleAppsUtils.retrieveGroupMembers(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-"));
        assertNotNull(members);
        assertTrue(members.size() == 1);
        assertTrue(members.get(0).getEmail().equalsIgnoreCase(GoogleAppsUtils.qualifyAddress(subjectId)));
    }

    @Test
    public void testProcessGroupMemberAddNew() throws GeneralSecurityException, IOException {
        //User doesn't exists in Google
        createTestGroup(groupName);

        Subject subject = mock(Subject.class);
        when(subject.getAttributeValue("givenName")).thenReturn("testgn2");
        when(subject.getAttributeValue("sn")).thenReturn("testfn2");
        when(subject.getAttributeValue("displayName")).thenReturn("testgn2, testfn2");
        when(subject.getAttributeValue("mail")).thenReturn(GoogleAppsUtils.qualifyAddress(subjectId));

        mockStatic(SubjectFinder.class);
        when(SubjectFinder.findByIdAndSource(subjectId, sourceId, true)).thenReturn(subject);

        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("membership", "addMembership", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.groupName)).thenReturn(groupName);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.subjectId)).thenReturn(subjectId);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_ADD.sourceId)).thenReturn(sourceId);
        when(addEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(addEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);

        List<Member> members = GoogleAppsUtils.retrieveGroupMembers(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-"));
        assertNotNull(members);
        assertTrue(members.size() == 1);
        assertTrue(members.get(0).getEmail().equalsIgnoreCase(GoogleAppsUtils.qualifyAddress(subjectId)));
    }

    @Test
    public void testProcessGroupMemberRemove() throws GeneralSecurityException, IOException {
        //User already exists in Google
        Group group = createTestGroup(groupName);
        createTestUser(GoogleAppsUtils.qualifyAddress(subjectId), "testgn3", "testfm3");

        Member member = new Member()
                .setEmail(GoogleAppsUtils.qualifyAddress(subjectId))
                .setRole("MEMBER");
        GoogleAppsUtils.addGroupMember(consumer.getDirectory(), group, member);

        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("membership", "deleteMembership", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.groupName)).thenReturn(groupName);
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.MEMBERSHIP_DELETE.subjectId)).thenReturn(subjectId);
        when(addEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(addEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);

        List<Member> members = GoogleAppsUtils.retrieveGroupMembers(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-"));
        assertNotNull(members);
        assertTrue(members.size() == 0);
    }

    @Test
    public void testProcessGroupMembershipSet() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
        //TODO: Some should already exists and some shouldn't yet.
    }

    @Test
    public void testProcessGroupsStemChange() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessSyncAttributeAddedDirectly() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessSyncAttributeAddedToParent() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessSyncAttributeRemovedDirectly() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessSyncAttributeRemovedFromParent() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessPrivilegeAdded() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessPrivilegeRemoved() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessPrivilegeChange() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
    }

    @Test
    public void testProcessGroupDelete() throws GeneralSecurityException, IOException {
        createTestGroup(groupName);

        ChangeLogEntry deleteEntry = mock(ChangeLogEntry.class);
        when(deleteEntry.getChangeLogType()).thenReturn(new ChangeLogType("group", "deleteGroup", ""));
        when(deleteEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name)).thenReturn(groupName);
        when(deleteEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(deleteEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);
        assertTrue(GoogleAppsUtils.retrieveGroup(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(groupName, "-")) == null);
    }

    private Group createTestGroup(String groupName) throws IOException {
        Group group = new Group();
        group.setName(groupName);
        group.setEmail(GoogleAppsUtils.qualifyAddress(groupName, "-"));
        return GoogleAppsUtils.addGroup(consumer.getDirectory(), group);
    }

    private User createTestUser(String email, String givenName, String surname) throws IOException {
        User user = new User();
        user.setPrimaryEmail(email);
        user.setName(new UserName());
        user.getName().setFamilyName(surname);
        user.getName().setGivenName(givenName);
        user.setPassword(new BigInteger(130, new SecureRandom()).toString(32));
        return GoogleAppsUtils.addUser(consumer.getDirectory(), user);
    }
}
