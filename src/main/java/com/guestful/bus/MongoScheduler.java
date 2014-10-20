/**
 * Copyright (C) 2013 Guestful (info@guestful.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.guestful.bus;

import com.guestful.json.JsonMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class MongoScheduler implements Scheduler {

    private static final Logger LOGGER = Logger.getLogger(MongoScheduler.class.getName());
    private static final long MINS_5 = 5 * 60 * 1000;

    private final DBCollection collection;
    private final JsonMapper jsonMapper;
    private final EventBus eventBus;

    public MongoScheduler(DBCollection collection, JsonMapper jsonMapper) {
        this(collection, jsonMapper, null);
    }

    public MongoScheduler(DBCollection collection, JsonMapper jsonMapper, EventBus eventBus) {
        this.collection = collection;
        this.jsonMapper = jsonMapper;
        this.eventBus = eventBus;
    }

    @Override
    public void schedule(Stream<ScheduledEvent> scheduledEvents) {
        LOGGER.finest("schedule() " + scheduledEvents);
        collection.insert(scheduledEvents.map(scheduledEvent ->
            new BasicDBObject()
                .append("id", scheduledEvent.getId())
                .append("scheduledTime", scheduledEvent.getScheduledTime())
                .append("name", scheduledEvent.getScheduledEvent().getClass().getName())
                .append("status", "PENDING")
                .append("createdDate", scheduledEvent.getTime())
                .append("updatedDate", scheduledEvent.getTime())
                .append("data", jsonMapper.toJson(scheduledEvent.getScheduledEvent())))
            .collect(Collectors.toList()));
    }

    public void produce() {
        produce((event, err) -> LOGGER.log(Level.SEVERE, "Error firing event: " + err.getMessage() + ". Event: " + event, err));
    }

    public void produce(BiConsumer<Map, Exception> onError) {
        if (eventBus == null) throw new UnsupportedOperationException();
        Map scheduledEvent;
        while ((scheduledEvent = next()) != null) {
            LOGGER.finest("produce() " + scheduledEvent);
            try {
                Class<?> eventClass = Thread.currentThread().getContextClassLoader().loadClass((String) scheduledEvent.get("name"));
                if (!Event.class.isAssignableFrom(eventClass)) {
                    throw new IllegalStateException("Not an event: " + eventClass.getName());
                }
                eventBus.post((Event) jsonMapper.fromJson((String) scheduledEvent.get("data"), eventClass));
                collection.update(new BasicDBObject("id", scheduledEvent.get("id")), new BasicDBObject("$set", new BasicDBObject()
                    .append("updatedDate", new Date())
                    .append("status", "FIRED")));
            } catch (Exception err) {
                try {
                    collection.update(
                        new BasicDBObject("id", scheduledEvent.get("id")),
                        new BasicDBObject("$set", new BasicDBObject()
                            .append("updatedDate", new Date())
                            .append("status", "ERROR")
                            .append("error", err.getMessage())));
                } finally {
                    onError.accept(scheduledEvent, err);
                }
            }
        }
    }

    private Map next() {
        long now = System.currentTimeMillis();
        DBObject object = collection.findAndModify(
            new BasicDBObject()
                .append("scheduledTime", new BasicDBObject("$lte", new Date(now)))
                .append("$or", Arrays.asList(
                    new BasicDBObject("status", "PENDING"),
                    new BasicDBObject("status", "LOADING").append("updatedDate", new BasicDBObject("$lte", new Date(now - MINS_5))))),
            null,
            null,
            false,
            new BasicDBObject("$set", new BasicDBObject()
                .append("updatedDate", new Date(now))
                .append("status", "LOADING")),
            true,
            false);
        return object == null ? null : object.toMap();
    }

}
