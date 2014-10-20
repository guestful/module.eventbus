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

import com.guestful.client.ironio.mq.IronProject;
import com.guestful.json.JsonMapper;

import javax.json.JsonObject;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class IronMQEventBus implements EventBus {

    private static final Logger LOGGER = Logger.getLogger(IronMQEventBus.class.getName());

    private final IronProject project;
    private final JsonMapper mapper;

    public IronMQEventBus(IronProject project, JsonMapper mapper) {
        this.project = project;
        this.mapper = mapper;
    }

    @Override
    public void post(Stream<? extends Event> events) {
        events
            .filter(event -> event.isLocal() && event.getClass().isAnnotationPresent(Queue.class))
            .map(Message::new)
            .collect(groupingBy(Message::getFromQueue))
            .forEach((queue, messages) -> {
                Collection<JsonObject> objs = messages.stream().map(mapper::toJsonObject).collect(toList());
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest("Posting events " + messages.stream().map(Message::getEventType).collect(toList()) + " to queue " + queue + ": " + objs);
                }
                project.getQueue(queue).offer(objs);
            });
    }

    public static class Message {

        private final Event event;
        private final String fromQueue;
        private final String eventType;

        public Message(Event event) {
            this.event = event;
            Queue queue = event.getClass().getAnnotation(Queue.class);
            String name = queue.name() == null ? "" : queue.name().trim();
            if (name.length() == 0) {
                name = "events." + event.getClass().getSimpleName();
            }
            this.fromQueue = name;
            this.eventType = event.getClass().getSimpleName();
        }

        public Event getEvent() {
            return event;
        }

        public String getFromQueue() {
            return fromQueue;
        }

        public String getEventType() {
            return eventType;
        }

        @Override
        public String toString() {
            return "Message{" +
                "event=" + event +
                ", fromQueue='" + fromQueue + '\'' +
                ", eventType='" + eventType + '\'' +
                '}';
        }
    }

}
