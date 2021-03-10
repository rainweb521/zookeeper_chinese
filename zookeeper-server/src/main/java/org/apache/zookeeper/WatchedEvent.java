/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.proto.WatcherEvent;

/**
 *  A WatchedEvent represents a change on the ZooKeeper that a Watcher
 *  is able to respond to.  The WatchedEvent includes exactly what happened,
 *  the current state of the ZooKeeper, and the path of the znode that
 *  was involved in the event.
 *  标准的事件处理器，定义了事件通知相关的逻辑
 *  这里包装了Watcher通知的状态和时间类型枚举类
 */
@InterfaceAudience.Public
public class WatchedEvent {

    //通知状态
    private final KeeperState keeperState;
    //事件类型
    private final EventType eventType;
    //节点路径
    private String path;

    /**
     * Create a WatchedEvent with specified type, state and path
     */
    public WatchedEvent(EventType eventType, KeeperState keeperState, String path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
    }

    /**
     * Convert a WatcherEvent sent over the wire into a full-fledged WatcherEvent
     */
    public WatchedEvent(WatcherEvent eventMessage) {
        keeperState = KeeperState.fromInt(eventMessage.getState());
        eventType = EventType.fromInt(eventMessage.getType());
        path = eventMessage.getPath();
    }

    public KeeperState getState() {
        return keeperState;
    }

    public EventType getType() {
        return eventType;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "WatchedEvent state:" + keeperState + " type:" + eventType + " path:" + path;
    }

    /**
     *  Convert WatchedEvent to type that can be sent over network
     *  这里调用方法将包装成一个可序列化的WatcherEvent事件，以便通过网络传输到客户端
     *  客户端接收到事件对象后，将WatcherEvent还原成WatchedEvent事件，并传递给process处理
     */
    public WatcherEvent getWrapper() {
        return new WatcherEvent(eventType.getIntValue(), keeperState.getIntValue(), path);
    }

}
