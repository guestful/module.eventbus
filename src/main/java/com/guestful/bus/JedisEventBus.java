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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.guestful.simplepool.ObjectPool;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * @author Mathieu Carbou (mathieu.carbou@gmail.com)
 */
public class JedisEventBus implements EventBus {

    private static final Logger LOGGER = Logger.getLogger(JedisEventBus.class.getName());
    private static final byte[] DEFAULT_CHANNEL = "eventbus".getBytes(StandardCharsets.UTF_8);

    private final JedisPool jedisPool;
    private final ObjectPool<Kryo> kryoPool;
    private final EventBus delegate;
    private final ConcurrentMap<String, Object> sentEvents = new ConcurrentHashMap<>();
    private final BinaryJedisPubSub subscriber = new BinaryJedisPubSub() {
        @Override
        public void onMessage(byte[] channel, byte[] message) {
            try {
                Event e = (Event) decode(message);
                if (sentEvents.remove(e.getId()) == null) {
                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.finest("Received event " + e.getId() + " " + e.getClass().getSimpleName());
                    }
                    e.setLocal(false);
                    delegate.post(e);
                }
            } catch (TimeoutException | InterruptedException e) {
                throw new Error(e);
            }
        }

        @Override
        public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {

        }

        @Override
        public void onSubscribe(byte[] channel, int subscribedChannels) {

        }

        @Override
        public void onUnsubscribe(byte[] channel, int subscribedChannels) {

        }

        @Override
        public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {

        }

        @Override
        public void onPSubscribe(byte[] pattern, int subscribedChannels) {

        }
    };

    private Thread poller;

    public JedisEventBus(EventBus delegate, JedisPool jedisPool, ObjectPool<Kryo> kryoPool) {
        this.delegate = delegate;
        this.jedisPool = jedisPool;
        this.kryoPool = kryoPool;
    }

    @PostConstruct
    public void init() {
        if (!subscriber.isSubscribed()) {
            poller = new Thread(JedisEventBus.class.getSimpleName() + "-Poller") {
                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted() && poller != null) {
                        Jedis jedis = null;
                        try {
                            jedis = jedisPool.getResource();
                            jedis.subscribe(subscriber, DEFAULT_CHANNEL);
                        } catch (Exception e) {
                            Throwable t = e;
                            if (e instanceof Error) {
                                t = e.getCause();
                            }
                            if (t instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            LOGGER.log(Level.SEVERE, "Subscribe error: " + e.getMessage(), e);
                            if (e instanceof JedisConnectionException) {
                                jedisPool.returnBrokenResource(jedis);
                                jedis = null;
                            }
                        } finally {
                            subscriber.unsubscribe();
                            jedisPool.returnResource(jedis);
                        }
                    }
                }
            };
            poller.start();
        }
    }

    @PreDestroy
    public void close() {
        if (subscriber.isSubscribed()) {
            poller.interrupt();
            poller = null;
            subscriber.unsubscribe();
        }
    }

    @Override
    public void post(Stream<? extends Event> events) {
        final Jedis jedis = jedisPool.getResource();
        try {
            events
                .filter(event -> event.isLocal() && event.getClass().isAnnotationPresent(Cluster.class))
                .forEach(event -> {
                    if (LOGGER.isLoggable(Level.FINEST)) {
                        LOGGER.finest("Posting event " + event.getId() + " " + event.getClass().getSimpleName());
                    }
                    try {
                        jedis.publish(DEFAULT_CHANNEL, encode(event));
                    } catch (TimeoutException | InterruptedException e) {
                        throw new RuntimeException("Unable to send event: " + e.getMessage(), e);
                    }
                    sentEvents.put(event.getId(), Void.class);
                });
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    private Object decode(byte[] bytes) throws TimeoutException, InterruptedException {
        if (bytes == null) return null;
        Kryo kryo = null;
        try {
            kryo = kryoPool.borrow();
            return kryo.readClassAndObject(new Input(bytes));
        } finally {
            if (kryo != null) {
                kryoPool.yield(kryo);
            }
        }
    }

    private byte[] encode(Object value) throws TimeoutException, InterruptedException {
        Kryo kryo = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output output = new Output(baos);
            kryo = kryoPool.borrow();
            kryo.writeClassAndObject(output, value);
            output.close();
            return baos.toByteArray();
        } finally {
            if (kryo != null) {
                kryoPool.yield(kryo);
            }
        }
    }

    private static class Error extends RuntimeException {
        private Error(Throwable cause) {
            super(cause);
        }
    }

}
