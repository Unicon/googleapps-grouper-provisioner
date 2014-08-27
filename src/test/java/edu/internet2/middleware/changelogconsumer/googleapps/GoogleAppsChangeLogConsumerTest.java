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
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderConfig;
import edu.internet2.middleware.grouper.changeLog.*;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "org.apache.log4j.*", "javax.xml.parsers.*", "org.apache.xerces.jaxp.*",  "org.xml.*", "net.sf.ehcache.*"})
@PrepareForTest(value = { GrouperLoaderConfig.class, GrouperLoaderConfig.class, })
public class GoogleAppsChangeLogConsumerTest {

    private static final String grouperName = "edu:internet2:grouper:provisioned:testGroup";

    private static final String subjectId = "testSubjectId";

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

    @Test
    public void testProcessGroupAdd() throws GeneralSecurityException, IOException {
        ChangeLogEntry addEntry = mock(ChangeLogEntry.class);
        when(addEntry.getChangeLogType()).thenReturn(new ChangeLogType("group", "addGroup", ""));
        when(addEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name)).thenReturn(grouperName);
        when(addEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(addEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);
        Group group = GoogleAppsUtils.retrieveGroup(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(grouperName, "-"));
        assertNotNull(group);
        assertTrue(group.getName().equalsIgnoreCase(grouperName));
    }

    @Test
    public void testProcessGroupDelete() throws GeneralSecurityException, IOException {
        ChangeLogEntry deleteEntry = mock(ChangeLogEntry.class);
        when(deleteEntry.getChangeLogType()).thenReturn(new ChangeLogType("group", "deleteGroup", ""));
        when(deleteEntry.retrieveValueForLabel(ChangeLogLabels.GROUP_ADD.name)).thenReturn(grouperName);
        when(deleteEntry.getContextId()).thenReturn("123456789");

        ArrayList changeLogEntryList = new ArrayList<ChangeLogEntry>(Arrays.asList(deleteEntry));

        consumer.processChangeLogEntries(changeLogEntryList, metadata);
        assertTrue(GoogleAppsUtils.retrieveGroup(consumer.getDirectory(), GoogleAppsUtils.qualifyAddress(grouperName, "-")) == null);
    }

    @Test
    public void testProcessGroupUpdate() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
        //TODO: Name Change
        //TODO: ID Change
        //TODO: Description Change
        //TODO: Privilege Change
    }

    @Test
    public void testProcessGroupMemberAdd() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
        //TODO: Member already exists
        //TODO: Member doesn't exists yet
    }

    @Test
    public void testProcessGroupMemberRemove() throws GeneralSecurityException, IOException {
        fail("Not Implemented");
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


}
