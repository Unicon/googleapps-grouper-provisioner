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
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryRequest;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.admin.directory.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * GoogleAppsSdkUtils is a helper class that interfaces with the Google SDK Admin API and handles exponential back-off.
 * see https://developers.google.com/admin-sdk/directory/v1/guides/delegation
 *
 * @author John Gasper, Unicon
 */
public class GoogleAppsSdkUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsChangeLogConsumer.class);

    private static final String[] scope = {DirectoryScopes.ADMIN_DIRECTORY_USER, DirectoryScopes.ADMIN_DIRECTORY_GROUP};

    private static final Random randomGenerator = new Random();

    /**
     * getGoogleCredential creates a credential object that authenticates the REST API calls.
     * @param serviceAccountEmail
     * @param serviceAccountPKCS12FilePath path of a private key (.p12) file provided by Google
     * @param serviceAccountUser a impersonation user account
     * @param httpTransport a httpTransport object
     * @param jsonFactory a jsonFactory object
     * @return a Google Credential
     * @throws GeneralSecurityException
     * @throws IOException
     */
    public static GoogleCredential getGoogleCredential(String serviceAccountEmail, String serviceAccountPKCS12FilePath,
                                        String serviceAccountUser, HttpTransport httpTransport, JsonFactory jsonFactory)
            throws GeneralSecurityException, IOException {

        return new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setServiceAccountId(serviceAccountEmail)
                .setServiceAccountScopes(Arrays.asList(scope))
                .setServiceAccountUser(serviceAccountUser)
                .setServiceAccountPrivateKeyFromP12File(new File(serviceAccountPKCS12FilePath))
                .build();
    }

    /**
     * addUser creates a user to Google.
     * @param directory a Directory (service) object
     * @param user a populated User object
     * @return the new User object created/returned by Google
     * @throws IOException
     */
    public static User addUser(Directory directory, User user) throws IOException {
        LOG.debug("addUser() - {}", user);

        Directory.Users.Insert request = null;

        try {
            request = directory.users().insert(user);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        return (User) execute(request);
    }

    /**
     * removeGroup removes a group from Google.
     * @param directory a Directory (service) object
     * @param userKey an identifier for a user (e-mail address is the most popular)
     * @throws IOException
     */
    public static void removeUser(Directory directory, String userKey) throws IOException {
        LOG.debug("removeUser() - {}", userKey);

        Directory.Users.Delete request = null;

        try {
            request = directory.users().delete(userKey);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        execute(request);
    }


    /**
     * addGroup adds a group to Google.
     * @param directory a Directory (service) object
     * @param group a populated Group object
     * @return the new Group object created/returned by Google
     * @throws IOException
     */
    public static Group addGroup(Directory directory, Group group) throws IOException {
        LOG.debug("addGroup() - {}", group);

        Directory.Groups.Insert request = null;

        try {
            request = directory.groups().insert(group);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        return (Group) execute(request);
    }

    /**
     * removeGroup removes a group from Google.
     * @param directory a Directory (service) object
     * @param groupKey
     * @throws IOException
     */
    public static void removeGroup(Directory directory, String groupKey) throws IOException {
        LOG.debug("removeGroup() - {}", groupKey);

        Directory.Groups.Delete request = null;

        try {
            request = directory.groups().delete(groupKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        execute(request);
    }

    /**
     * addGroup adds a group to Google.
     * @param directory a Directory (service) object
     * @param group a populated Group object
     * @return the new Group object created/returned by Google
     * @throws IOException
     */
    public static Group updateGroup(Directory directory, String groupKey, Group group) throws IOException {
        LOG.debug("updateGroup() - {}", group);

        Directory.Groups.Update request = null;

        try {
            request = directory.groups().update(groupKey, group);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        return (Group) execute(request);
    }

    /**
     * retrieveAllUsers returns all of the users from Google.
     * @param directory a Directory (service) object
     * @return a list of all the users in the directory
     * @throws IOException
     */
    public static List<User> retrieveAllUsers(Directory directory) throws IOException {
        LOG.debug("retrieveAllUsers()");

        List<User> allUsers = new ArrayList<User>();

        Directory.Users.List request = null;
        try {
            request = directory.users().list().setCustomer("my_customer");
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        do { //continue until we have all the pages read in.
            Users currentPage = (Users)execute(request);

            allUsers.addAll(currentPage.getUsers());
            request.setPageToken(currentPage.getNextPageToken());

        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return allUsers;
    }

    /**
     *
     * @param directory a Directory (service) object
     * @param userKey an identifier for a user (e-mail address is the most popular)
     * @return the User object returned by Google.
     * @throws IOException
     */
    public static User retrieveUser(Directory directory, String userKey) throws IOException {
        LOG.debug("retrieveUser() - {}", userKey);

        Directory.Users.Get request = null;

        try {
            request = directory.users().get(userKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (User) execute(request);
    }

    /**
     *
     * @param directory a Directory (service) object
     * @return a list of all the groups in the directory
     * @throws IOException
     */
    public static List<Group> retrieveAllGroups(Directory directory) throws IOException {
        LOG.debug("retrieveAllGroups()");

        final List<Group> allGroups = new ArrayList<Group>();

        Directory.Groups.List request = null;
        try {
            request = directory.groups().list().setCustomer("my_customer");
        } catch (IOException e) {
            e.printStackTrace();
        }

        do { //continue until we have all the pages read in.
            final Groups currentPage = (Groups)execute(request);

            allGroups.addAll(currentPage.getGroups());
            request.setPageToken(currentPage.getNextPageToken());

        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return allGroups;
    }

    /**
     * retrieveGroup returns a requested group.
     * @param directory a Directory (service) object
     * @param groupKey an identifier for a group (e-mail address is the most popular)
     * @return the Group object from Google
     * @throws IOException
     */
    public static Group retrieveGroup(Directory directory, String groupKey) throws IOException {
        LOG.debug("retrieveGroup() - {}", groupKey);

        Directory.Groups.Get request = null;

        try {
            request = directory.groups().get(groupKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (Group) execute(request);
    }

    /**
     * retrieveGroupMembers returns a list of members of a group.
     * @param directory a Directory (service) object
     * @param groupKey an identifier for a group (e-mail address is the most popular)
     * @return a list of Members in the Group
     * @throws IOException
     */
    public static List<Member> retrieveGroupMembers(Directory directory, String groupKey) throws IOException {
        LOG.debug("retrieveGroupMembers() - {}", groupKey);

        final List<Member> members = new ArrayList<Member>();

        Directory.Members.List request = null;
        try {
            request = directory.members().list(groupKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        do { //continue until we have all the pages read in.
            try {
                final Members currentPage = (Members) execute(request);

                members.addAll(currentPage.getMembers());
                request.setPageToken(currentPage.getNextPageToken());
            } catch (NullPointerException ex) {

            }

        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return members;
    }

    /**
     * addGroupMember add an additional member to a group.
     * @param directory a Directory (service) object
     * @param group a Group object
     * @param member a Member object
     * @return a Member object stored on Google.
     * @throws IOException
     */
    public static Member addGroupMember(Directory directory, Group group, Member member) throws IOException {
        LOG.debug("addGroupMember() - add {} to {}", member, group);

        Directory.Members.Insert request = null;

        try {
            request = directory.members().insert(group.getId(), member);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return (Member) execute(request);
    }

    /**
     * removeGroupMember removes a member of a group.
     * @param directory a Directory (service) object
     * @param groupKey an identifier for a user (e-mail address is the most popular)
     * @param memberKey an identifier for a user (e-mail address is the most popular)
     * @throws GoogleJsonResponseException
     */
    public static void removeGroupMember(Directory directory, String groupKey, String memberKey) throws IOException {
        LOG.debug("removeGroupMember() - remove {} from {}", memberKey, groupKey);

        Directory.Members.Delete request = null;

        try {
            request = directory.members().delete(groupKey, memberKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        execute(request);
    }

    /**
     * handleGoogleJsonResponseException makes the handling of exponential back-off easy.
     * @param ex the GoogleJsonResponseException being handled
     * @param interval the exponential back-off interval
     * @return true=no record found, false=everything was handled properly
     * @throws GoogleJsonResponseException
     */
    private static boolean handleGoogleJsonResponseException(GoogleJsonResponseException ex, int interval)
            throws GoogleJsonResponseException {

        final GoogleJsonError e = ex.getDetails();

        switch (e.getCode()) {
            case 403:
                if (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                    || e.getErrors().get(0).getReason().equals("userRateLimitExceeded")) {

                    try {
                        LOG.warn("handleGoogleJsonResponseException() - we've exceeded a rate limit ({}) so taking a nap. (You should see if you can get the rate limit increased by Google.)", e.getErrors().get(0).getReason());
                        Thread.sleep((1 << interval) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        LOG.debug("handleGoogleJsonResponseException() - {}", ie);
                    }
                }
                break;

            case 404: //Not found
                return true;

            case 503:
                if (e.getErrors().get(0).getReason().equals("backendError")) {
                    try {
                        LOG.warn("handleGoogleJsonResponseException() - service unavailable/backend error so taking a nap.");
                        Thread.sleep((1 << interval) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        LOG.debug("handleGoogleJsonResponseException() - {}", ie);
                    }

                }
                break;

            default:
                // Other error, re-throw.
                throw ex;
        }
        return false;
    }

    /**
     * execute takes a DirectoryRequest and calls the execute() method and handles exponential back-off, etc.
     * @param request a populated DirectoryRequest object
     * @return an output Object that should be cast in the calling method
     * @throws IOException
     */
    private static Object execute(DirectoryRequest request) throws IOException {
        return execute(1, request);
    }

    /**
     * execute takes a DirectoryRequest and calls the execute() method and handles exponential back-off, etc.
     * @param interval the count of attempts that this request has had.
     * @param request a populated DirectoryRequest object
     * @return an output Object that should be cast in the calling method
     * @throws IOException
     */
    private static Object execute(int interval, DirectoryRequest request) throws IOException {
        LOG.trace("execute() - {} request attempt #{}",request.getClass().getName().replace(request.getClass().getPackage().getName(), ""), interval);

        try {
            return request.execute();
        } catch (GoogleJsonResponseException ex) {
            if (interval == 7) {
                LOG.error("execute() - Retried attempt 7 times, failing request");
                throw ex;

            } else {
                if (handleGoogleJsonResponseException(ex, interval)) { //404's return true
                    return null;
                } else {
                    return execute(++interval, request);
                }
            }
        } catch(IOException e) {
            LOG.error("execute() - An unknown IO error occurred: " + e);

            if (interval == 7) {
                LOG.error("Retried attempt 7 times, failing request");
                throw e;

            } else {
                return execute(++interval, request);
            }
        }

    }

}
