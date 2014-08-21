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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.model.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Properties;

public class GoogleAppsUtilsTest {
    private static String TEST_USER;
    private static String TEST_GROUP;

    private static String SERVICE_ACCOUNT_EMAIL;
    private static String SERVICE_ACCOUNT_PKCS_12_FILE_PATH;
    private static String SERVICE_ACCOUNT_USER;

    /** Global instance of the HTTP transport. */
    private static HttpTransport httpTransport;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    GoogleCredential googleCredential = null;

    @BeforeClass
    public static void setup() {
        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream("test.properties");
        try {
            props.load(is);
            TEST_USER = props.getProperty("TEST_USER");
            TEST_GROUP = props.getProperty("TEST_GROUP");

            SERVICE_ACCOUNT_EMAIL = props.getProperty("SERVICE_ACCOUNT_EMAIL");
            SERVICE_ACCOUNT_PKCS_12_FILE_PATH = props.getProperty("SERVICE_ACCOUNT_PKCS_12_FILE_PATH");
            SERVICE_ACCOUNT_USER = props.getProperty("SERVICE_ACCOUNT_USER");
        }
        catch (IOException e) {
            System.out.println("test.properties configuration not found. Try again! Love, Grumpy Cat");
        }
    }

    private Directory getDirectoryService() throws GeneralSecurityException, IOException {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        if (googleCredential == null) {
            googleCredential = GoogleAppsUtils.getGoogleCredential(SERVICE_ACCOUNT_EMAIL,
                    SERVICE_ACCOUNT_PKCS_12_FILE_PATH, SERVICE_ACCOUNT_USER,
                    httpTransport, JSON_FACTORY);
        }

        return  new Directory.Builder(httpTransport, JSON_FACTORY, googleCredential)
                .setApplicationName("Google Apps Grouper Provisioner")
                .build();
    }

    @Test
    public void testGetGoogleCredential() throws Exception {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredential googleCredential = GoogleAppsUtils.getGoogleCredential(SERVICE_ACCOUNT_EMAIL,
                SERVICE_ACCOUNT_PKCS_12_FILE_PATH, SERVICE_ACCOUNT_USER,
                httpTransport, JSON_FACTORY);

        Directory service = new Directory.Builder(httpTransport, JSON_FACTORY, googleCredential)
                .setApplicationName("Google Apps Grouper Provisioner")
                .build();

        Directory.Users.List request = service.users().list().setCustomer("my_customer");
        request.execute();
    }

    @Test
    public void testCreateUser() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        User user = new User();
        user.setName(new UserName().setFamilyName("Gasper").setGivenName("Test"))
                .setPrimaryEmail(TEST_USER)
                .setPassword(new BigInteger(130, new SecureRandom()).toString(32));
        try {
            User currentUser = GoogleAppsUtils.addUser(directory, user);
            Assert.assertEquals("Boom", currentUser.getName().getGivenName(), user.getName().getGivenName());

        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409 ) {
                throw ex;
            }
        }
    }

    @Test
    public void testCreateGroup() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        Group group = new Group();
        group.setName("Test Group");
        group.setEmail(TEST_GROUP);

        try {
            Group currentGroup = GoogleAppsUtils.addGroup(directory, group);
            Assert.assertEquals("Boom", currentGroup.getName(), group.getName());

        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409 ) {
                throw ex;
            }
        }
    }

    @Test
    public void testRetrieveAllUsers() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        List<User> allUsers = GoogleAppsUtils.retrieveAllUsers(directory);
        Assert.assertTrue(allUsers.size() > 0);
    }

    @Test
    public void testRetrieveUser() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        User user = GoogleAppsUtils.retrieveUser(directory, TEST_USER);
        Assert.assertTrue(user.getName().getGivenName().equalsIgnoreCase("Test"));
    }

    @Test
    public void testRetrieveMissingUser() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        User user = GoogleAppsUtils.retrieveUser(directory, "missing-" + TEST_USER);
        Assert.assertTrue(user == null);
    }

    @Test
    public void testRetrieveAllGroups() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        List<Group> allGroups = GoogleAppsUtils.retrieveAllGroups(directory);
        Assert.assertTrue(allGroups.size() > 0);
    }

    @Test
    public void testRetrieveGroup() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        Group group = GoogleAppsUtils.retrieveGroup(directory, TEST_GROUP);
        Assert.assertTrue(group.getName().equalsIgnoreCase("Test Group"));
    }

    @Test
    public void testRetrieveMissingGroup() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        Group group = GoogleAppsUtils.retrieveGroup(directory, "missing-" + TEST_GROUP);
        Assert.assertTrue(group == null);
    }

    @Test
    public void testAddMember() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        Member member = new Member();
        member.setRole("MEMBER");
        member.setEmail(TEST_USER);

        Group group = GoogleAppsUtils.retrieveGroup(directory, TEST_GROUP);

        try {
            Member currentMember = GoogleAppsUtils.addGroupMember(directory, group, member);
            Assert.assertEquals("Boom", currentMember.getEmail(), member.getEmail());

        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409 ) {
                throw ex;
            }
        }
    }

    @Test
    public void testRetrieveGroupMembers() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        Group group = GoogleAppsUtils.retrieveGroup(directory, TEST_GROUP);
        List<Member> members = GoogleAppsUtils.retrieveGroupMembers(directory, group);
        Assert.assertTrue(members.size() > 0);
    }

    @Test
    public void testRemoveMember() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();
        User user = GoogleAppsUtils.retrieveUser(directory, TEST_USER);
        Group group = GoogleAppsUtils.retrieveGroup(directory, TEST_GROUP);

        try {
            GoogleAppsUtils.removeGroupMember(directory, group, user);

        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409 ) {
                throw ex;
            }
        }
    }

    @Test
    public void testRemoveGroup() throws GeneralSecurityException, IOException {
        Directory directory = getDirectoryService();

        Group group = GoogleAppsUtils.retrieveGroup(directory, TEST_GROUP);

        try {
            GoogleAppsUtils.removeGroup(directory, group);

        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() != 409 ) {
                throw ex;
            }
        }
    }

}
