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
package org.apache.zookeeper.audit;

import static org.apache.zookeeper.audit.AuditEvent.FieldName;
import java.lang.reflect.Constructor;
import org.apache.zookeeper.audit.AuditEvent.Result;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zookeeper的审核日志，仅在记录连接了客户端的服务器记录
 *
 * 会议:客户会话ID
 *
 * 用户:与客户端会话相关联的用户的逗号分隔列表
 *
 * ip:客户端IP地址
 *
 * 操作:所选的任何一项审核操作。可能的值为（serverStart，serverStop，create，delete，setData，setAcl，multiOperation，reconfig，ephemeralZNodeDeleteOnSessionClose）
 *
 * znode: znode的路径
 *
 * znode类型:创建操作时znode的类型
 *
 * ACL:znode ACL的字符串表示形式，如cdrwa（创建，删除，读取，写入，管理）。仅记录setAcl操作
 *
 * 结果:操作结果。可能的值为（成功/失败/调用）。结果“ invoked”用于serverStop操作，因为在确保服务器实际停止之前已记录了stop。
 */
public class ZKAuditProvider {
    static final String AUDIT_ENABLE = "zookeeper.audit.enable";
    static final String AUDIT_IMPL_CLASS = "zookeeper.audit.impl.class";
    private static final Logger LOG = LoggerFactory.getLogger(ZKAuditProvider.class);
    // By default audit logging is disabled
    private static boolean auditEnabled;
    private static AuditLogger auditLogger;

    static {
        auditEnabled = Boolean.getBoolean(AUDIT_ENABLE);
        if (auditEnabled) {
            //initialise only when audit logging is enabled
            auditLogger = getAuditLogger();
            LOG.info("ZooKeeper audit is enabled.");
        } else {
            LOG.info("ZooKeeper audit is disabled.");
        }
    }

    private static AuditLogger getAuditLogger() {
        String auditLoggerClass = System.getProperty(AUDIT_IMPL_CLASS);
        if (auditLoggerClass == null) {
            auditLoggerClass = Log4jAuditLogger.class.getName();
        }
        try {
            Constructor<?> clientCxnConstructor = Class.forName(auditLoggerClass)
                    .getDeclaredConstructor();
            AuditLogger auditLogger = (AuditLogger) clientCxnConstructor.newInstance();
            auditLogger.initialize();
            return auditLogger;
        } catch (Exception e) {
            throw new RuntimeException("Couldn't instantiate " + auditLoggerClass, e);
        }
    }

    /**
     * @return true if audit log is enabled
     */
    public static boolean isAuditEnabled() {
        return auditEnabled;
    }

    public static void log(String user, String operation, String znode, String acl,
                           String createMode, String session, String ip, Result result) {
        auditLogger.logAuditEvent(createLogEvent(user, operation, znode, acl, createMode, session, ip, result));
    }

    /**
     * A helper api for creating an AuditEvent object.
     */
    static AuditEvent createLogEvent(String user, String operation, Result result) {
        AuditEvent event = new AuditEvent(result);
        event.addEntry(FieldName.USER, user);
        event.addEntry(FieldName.OPERATION, operation);
        return event;
    }

    /**
     * A helper api for creating an AuditEvent object.
     */
    static AuditEvent createLogEvent(String user, String operation, String znode, String acl,
                                     String createMode, String session, String ip, Result result) {
        AuditEvent event = new AuditEvent(result);
        event.addEntry(FieldName.SESSION, session);
        event.addEntry(FieldName.USER, user);
        event.addEntry(FieldName.IP, ip);
        event.addEntry(FieldName.OPERATION, operation);
        event.addEntry(FieldName.ZNODE, znode);
        event.addEntry(FieldName.ZNODE_TYPE, createMode);
        event.addEntry(FieldName.ACL, acl);
        return event;
    }

    /**
     * Add audit log for server start and register server stop log.
     */
    public static void addZKStartStopAuditLog() {
        if (isAuditEnabled()) {
            log(getZKUser(), AuditConstants.OP_START, Result.SUCCESS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log(getZKUser(), AuditConstants.OP_STOP, Result.INVOKED);
            }));
        }
    }

    /**
     * Add audit log for server start fail.
     */
    public static void addServerStartFailureAuditLog() {
        if (isAuditEnabled()) {
            log(ZKAuditProvider.getZKUser(), AuditConstants.OP_START, Result.FAILURE);
        }
    }

    private static void log(String user, String operation, Result result) {
        auditLogger.logAuditEvent(createLogEvent(user, operation, result));
    }

    /**
     * User who has started the ZooKeeper server user, it will be the logged-in
     * user. If no user logged-in then system user.
     */
    public static String getZKUser() {
        return ServerCnxnFactory.getUserName();
    }

}
