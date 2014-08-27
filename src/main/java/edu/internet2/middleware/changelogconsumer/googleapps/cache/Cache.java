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
package edu.internet2.middleware.changelogconsumer.googleapps.cache;

import com.google.api.services.admin.directory.model.Group;
import com.google.api.services.admin.directory.model.User;
import org.joda.time.DateTime;

import java.util.Hashtable;
import java.util.List;

/**
 * CacheObject supports Google User & Group object
 *
 * * @author John Gasper, Unicon
 */
public class Cache<T> {
    private Hashtable<String, T> cache;
    private DateTime cachePopulatedTime;
    private int cacheValidity = 30;

    public T get(String id) {
        return cache.get(id);
    }

    public void clear() {
        cache.clear();
    }

    public void put(String id, T item) {
        cache.put(id, item);
    }

    public void seed(List<T> items) {
        cache = new Hashtable<String, T>(items.size() + 100);

        for (T item : items) {
            if (item.getClass().equals(User.class)) {
                cache.put(((User) item).getPrimaryEmail(), item);
            } else if (item.getClass().equals(Group.class)) {
                cache.put(((Group) item).getEmail(), item);
            }
        }

        cachePopulatedTime = new DateTime();
    }

    public void setCacheValidity(int minutes){
        cacheValidity = minutes;
    }

    public DateTime getExpiration() {
        return cachePopulatedTime != null ? cachePopulatedTime.plusMinutes(cacheValidity) : null;
    }

    public boolean isExpired() {
        return cachePopulatedTime == null ? true : getExpiration().isBeforeNow();
    }
}