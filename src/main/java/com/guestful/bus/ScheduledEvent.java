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

import java.util.Date;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class ScheduledEvent extends Event {

    private Date scheduledTime = new Date();
    private Event scheduledEvent;

    public Date getScheduledTime() {
        return scheduledTime;
    }

    public void setScheduledTime(Date scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    public Event getScheduledEvent() {
        return scheduledEvent;
    }

    public void setScheduledEvent(Event scheduledEvent) {
        this.scheduledEvent = scheduledEvent;
    }

    @Override
    public String toString() {
        return getScheduledEvent().getClass().getSimpleName() + " at " + getScheduledTime();
    }

}
