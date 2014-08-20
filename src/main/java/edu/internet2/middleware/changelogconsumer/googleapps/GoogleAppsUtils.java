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
 * GoogleAppsUtils is a helper class that interfaces with the Google Admin SDK/API and handles exponential back-off.
 *
 * @author John Gasper, Unicon
 */
public class GoogleAppsUtils {
    private static final Logger LOG = LoggerFactory.getLogger(GoogleAppsChangeLogConsumer.class);

    private final static String[] scope = {DirectoryScopes.ADMIN_DIRECTORY_USER, DirectoryScopes.ADMIN_DIRECTORY_GROUP};

    private final static Random randomGenerator = new Random();

    //https://developers.google.com/admin-sdk/directory/v1/guides/delegation
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


    public static User addUser(Directory directory, User user) throws GoogleJsonResponseException {
        Directory.Users.Insert request = null;

        try {
            request = directory.users().insert(user);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                return request.execute();

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }

        return null;
    }


    public static Group addGroup(Directory directory, Group group) throws GoogleJsonResponseException {
        Directory.Groups.Insert request = null;

        try {
            request = directory.groups().insert(group);
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                return request.execute();

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }

        return null;
    }

    public static void removeGroup(Directory directory, Group group) throws GoogleJsonResponseException {
        Directory.Groups.Delete request = null;

        try {
            request = directory.groups().delete(group.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                request.execute();
                break;

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }
    }


    public static List<User> retrieveAllUsers(Directory directory) throws GoogleJsonResponseException {
        List<User> allUsers = new ArrayList<User>();

        Directory.Users.List request = null;
        try {
            request = directory.users().list().setCustomer("my_customer");
        } catch (IOException e) {
            LOG.error("An unknown error occurred: " + e);
        }

        do { //continue until we have all the pages read in.
            for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
                try {
                    Users currentPage = request.execute();

                    allUsers.addAll(currentPage.getUsers());
                    request.setPageToken(currentPage.getNextPageToken());
                    break; //success, break out of the for loop.

                } catch (GoogleJsonResponseException ex){
                    GoogleJsonError e = ex.getDetails();

                    if (e.getCode() == 403
                            && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                            || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                        try {
                            Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    } else {
                        // Other error, re-throw.
                        throw ex;
                    }

                } catch(IOException e) {
                    LOG.debug("We are probably just out of pages to go through, but maybe not: " + e);
                    request.setPageToken(null);
                }
            }
        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return allUsers;
    }


    public static User retrieveUser(Directory directory, String userKey) throws GoogleJsonResponseException {
        Directory.Users.Get request = null;

        try {
            request = directory.users().get(userKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                return request.execute();

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else if (e.getCode() == 404) {//Not found
                    return null;

                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }

        return null;
    }

    public static List<Group> retrieveAllGroups(Directory directory) throws GoogleJsonResponseException {
        List<Group> allGroups = new ArrayList<Group>();

        Directory.Groups.List request = null;
        try {
            request = directory.groups().list().setCustomer("my_customer");
        } catch (IOException e) {
            e.printStackTrace();
        }

        do { //continue until we have all the pages read in.
            for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
                try {
                    Groups currentPage = request.execute();

                    allGroups.addAll(currentPage.getGroups());
                    request.setPageToken(currentPage.getNextPageToken());
                    break; //success, break out of the for loop.

                } catch (GoogleJsonResponseException ex){
                    GoogleJsonError e = ex.getDetails();

                    if (e.getCode() == 403
                            && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                            || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                        try {
                            Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    } else {
                        // Other error, re-throw.
                        throw ex;
                    }

                } catch(IOException e) {
                    LOG.debug("We are probably just out of pages to go through, but maybe not: " + e);
                    request.setPageToken(null);
                }
            }
        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return allGroups;
    }


    public static Group retrieveGroup(Directory directory, String groupKey) throws GoogleJsonResponseException {
        Directory.Groups.Get request = null;

        try {
            request = directory.groups().get(groupKey);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                return request.execute();

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }

                } else if (e.getCode() == 404) { //Not found
                    return null;

                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }

        return null;
    }

    public static List<Member> retrieveGroupMembers(Directory directory, Group group) throws GoogleJsonResponseException {
        List<Member> members = new ArrayList<Member>();

        Directory.Members.List request = null;
        try {
            request = directory.members().list(group.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        do { //continue until we have all the pages read in.
            for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
                try {
                    Members currentPage = request.execute();

                    members.addAll(currentPage.getMembers());
                    request.setPageToken(currentPage.getNextPageToken());
                    break; //success, break out of the for loop.

                } catch (GoogleJsonResponseException ex){
                    GoogleJsonError e = ex.getDetails();

                    if (e.getCode() == 403
                            && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                            || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                        try {
                            Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                        } catch (InterruptedException ie) {
                            ie.printStackTrace();
                        }
                    } else {
                        // Other error, re-throw.
                        throw ex;
                    }

                } catch(IOException e) {
                    LOG.debug("We are probably just out of pages to go through, but maybe not: " + e);
                    request.setPageToken(null);
                }
            }
        } while (request.getPageToken() != null && request.getPageToken().length() > 0);

        return members;
    }

    public static Member addGroupMember(Directory directory, Group group, Member member) throws GoogleJsonResponseException {
        Directory.Members.Insert request = null;

        try {
            request = directory.members().insert(group.getId(), member);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {
                return request.execute();

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }

        return null;
    }

    public static void removeGroupMember(Directory directory, Group group, User user) throws GoogleJsonResponseException {
        Directory.Members.Delete request = null;

        try {
            request = directory.members().delete(group.getId(), user.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int n = 0; n < 7; ++n) { //try exponential back-off 7 times
            try {

                request.execute();
                break;

            } catch (GoogleJsonResponseException ex){
                GoogleJsonError e = ex.getDetails();

                if (e.getCode() == 403
                        && (e.getErrors().get(0).getReason().equals("rateLimitExceeded")
                        || e.getErrors().get(0).getReason().equals("userRateLimitExceeded"))) {

                    try {
                        Thread.sleep((1 << n) * 1000 + randomGenerator.nextInt(1001));
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                } else {
                    // Other error, re-throw.
                    throw ex;
                }

            } catch(IOException e) {
                LOG.error("An unknown error occurred: " + e);
            }
        }
    }

}
