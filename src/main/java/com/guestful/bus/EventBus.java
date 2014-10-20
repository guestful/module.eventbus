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

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Dispatcher inyerface used to send events
 *
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public interface EventBus {

    void post(Stream<? extends Event> eventStream);

    default void post(Event event) {
        post(Stream.of(event));
    }

    default void post(Event... events) {
        post(Stream.of(events));
    }

    default void post(Collection<? extends Event> events) {
        post(events.stream());
    }

}
