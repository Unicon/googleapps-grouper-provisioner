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

/**
 * Initiates a GoogleAppsFullSync from command-line
 *
 * @author John Gasper, Unicon
 */
public class GoogleAppsFullSync {

    public static void main(String[] args) {
        if (args.length == 0 ) {
            System.console().printf("a Google Change Log Consumer Name must be provided");
            System.console().printf("consumerName [--dry-run]");
            System.exit(-1);
        }

        GoogleAppsFullSync fullSync = new GoogleAppsFullSync(args[0], args[1] != null && args[1].equalsIgnoreCase("--dry-run"));
    }

    public GoogleAppsFullSync(String googleChangeLogConsumerName, boolean dryRun) {
        GoogleAppsChangeLogConsumer consumer = new GoogleAppsChangeLogConsumer();
        consumer.fullSync(googleChangeLogConsumerName, dryRun);
    }


}
