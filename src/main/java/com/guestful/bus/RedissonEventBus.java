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

import org.redisson.Redisson;
import org.redisson.core.RTopic;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class RedissonEventBus implements EventBus {

    private static final Logger LOGGER = Logger.getLogger(RedissonEventBus.class.getName());

    private final Redisson redisson;
    private final EventBus delegate;
    private final ConcurrentMap<String, Object> sentEvents = new ConcurrentHashMap<>();
    private RTopic<Event> topic;
    private int listenerId;

    public RedissonEventBus(EventBus delegate, Redisson redisson) {
        this.delegate = delegate;
        this.redisson = redisson;
    }

    @PostConstruct
    public void init() {
        if (topic == null) {
            topic = redisson.getTopic("eventbus");
            listenerId = topic.addListener(e -> {
                if (sentEvents.remove(e.getId()) == null) {
                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.finest("Received event " + e.getId() + " " + e.getClass().getSimpleName());
                    }
                    e.setLocal(false);
                    delegate.post(e);
                }
            });
        }
    }

    @PreDestroy
    public void close() {
        if (topic != null) {
            topic.removeListener(listenerId);
            topic = null;
        }
    }

    @Override
    public void post(Stream<? extends Event> eventStream) {
        eventStream.forEach(event -> {
            if (event.isLocal() && event.getClass().isAnnotationPresent(Cluster.class)) {
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest("Posting event " + event.getId() + " " + event.getClass().getSimpleName());
                }
                sentEvents.put(event.getId(), Void.class);
                topic.publish(event);
            }
        });
    }

}
