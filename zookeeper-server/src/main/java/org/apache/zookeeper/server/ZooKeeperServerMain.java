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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.audit.ZKAuditProvider;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 *
 * 这个类用于在独立模式启动zookeeper
 */
@InterfaceAudience.Public
public class ZooKeeperServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperServerMain.class);

    private static final String USAGE = "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    // ZooKeeper server supports two kinds of connection: unencrypted and encrypted.
    private ServerCnxnFactory cnxnFactory;//管理客户端连接
    private ServerCnxnFactory secureCnxnFactory; //管理加密的客户端连接
    private ContainerManager containerManager; // zookeeper整体服务
    private MetricsProvider metricsProvider; //监控服务
    private AdminServer adminServer; //管理server状态

    /*
     * Start up the ZooKeeper server.
     *
     * @param args the configfile or the port datadir [ticktime]
     * 启动过程与QuorumPeer的一样
     */
    public static void main(String[] args) {
        ZooKeeperServerMain main = new ZooKeeperServerMain();
        try {
            //加载配置文件
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {//参数无效
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            //这里打印审核日志
            ZKAuditProvider.addServerStartFailureAuditLog();
            //调用系统退出，并添加错误值
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        //正常退出
        LOG.info("Exiting normally");
        ServiceUtils.requestSystemExit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    protected void initializeAndRun(String[] args) throws ConfigException, IOException, AdminServerException {
        try {
            //注册log4j来实现远程日志功能
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        //新建一个空的配置文件类
        ServerConfig config = new ServerConfig();
        //使用启动参数中的配置文件地址
        if (args.length == 1) {
            config.parse(args[0]);
        } else {
            config.parse(args);
        }

        //通过配置文件启动
        runFromConfig(config);
    }

    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     * @throws AdminServerException
     */
    public void runFromConfig(ServerConfig config) throws IOException, AdminServerException {
        LOG.info("Starting server");
        //清理事务和快照文件
        FileTxnSnapLog txnLog = null;
        try {
            try {
                //配置监控服务
                metricsProvider = MetricsProviderBootstrap.startMetricsProvider(
                    config.getMetricsProviderClassName(),
                    config.getMetricsProviderConfiguration());
            } catch (MetricsProviderLifeCycleException error) {
                throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(), error);
            }
            //初始化监控
            ServerMetrics.metricsProviderInitialized(metricsProvider);
            ProviderRegistry.initialize();
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            //配置路径
            txnLog = new FileTxnSnapLog(config.dataLogDir, config.dataDir);
            //创建jvm监控类
            JvmPauseMonitor jvmPauseMonitor = null;
            if (config.jvmPauseMonitorToRun) {
                jvmPauseMonitor = new JvmPauseMonitor(config);
            }
            //创建server类
            final ZooKeeperServer zkServer = new ZooKeeperServer(jvmPauseMonitor, txnLog, config.tickTime, config.minSessionTimeout, config.maxSessionTimeout, config.listenBacklog, null, config.initialConfig);
            txnLog.setServerStats(zkServer.serverStats());

            // Registers shutdown handler which will be used to know the
            // server error or shutdown state changes.
            //因为当前为独立模式，当有一个线程完成后，表示程序结束
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            zkServer.registerServerShutdownHandler(new ZooKeeperServerShutdownHandler(shutdownLatch));

            // Start Admin server
            //adminServer配置，并启动
            adminServer = AdminServerFactory.createAdminServer();
            adminServer.setZooKeeperServer(zkServer);
            adminServer.start();

            //needStartZKServer用来判断在普通会话管理器还是加密会话管理器来启动zk
            boolean needStartZKServer = true;
            if (config.getClientPortAddress() != null) {
                //配置启动会话管理器
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), false);
                //此方法除了启动ServerCnxnFactory,还会启动ZooKeeper
                cnxnFactory.startup(zkServer);
                // zkServer has been started. So we don't need to start it again in secureCnxnFactory.
                needStartZKServer = false;
            }
            if (config.getSecureClientPortAddress() != null) {
                //启动加密类型的会话管理器
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), true);
                secureCnxnFactory.startup(zkServer, needStartZKServer);
            }

            //创建容器节点，类似于管理持久节点,持久顺序节点,临时节点,临时顺序节点
            containerManager = new ContainerManager(
                zkServer.getZKDatabase(),
                zkServer.firstProcessor,
                Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1)),//执行两次检查任务之间的时间间隔,单位:ms,默认1min
                Integer.getInteger("znode.container.maxPerMinute", 10000), //	一分钟内最多删除多少个容器节点,即删除两个容器节点之间的最少时间间隔为60000/10000=6ms
                Long.getLong("znode.container.maxNeverUsedIntervalMs", 0)
            );
            containerManager.start();
            ZKAuditProvider.addZKStartStopAuditLog();

            // Watch status of ZooKeeper server. It will do a graceful shutdown
            // if the server is not running or hits an internal error.
            //这里启动后等待主线程的执行，服务器正常启动时,运行到此处阻塞,只有server的state变为ERROR或SHUTDOWN时继续运行后面的代码
            shutdownLatch.await();

            shutdown();

            if (cnxnFactory != null) {
                cnxnFactory.join();
            }
            if (secureCnxnFactory != null) {
                secureCnxnFactory.join();
            }
            if (zkServer.canShutdown()) {
                zkServer.shutdown(true);
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
            if (txnLog != null) {
                txnLog.close();
            }
            if (metricsProvider != null) {
                try {
                    metricsProvider.stop();
                } catch (Throwable error) {
                    LOG.warn("Error while stopping metrics", error);
                }
            }
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        if (containerManager != null) {
            containerManager.stop();
        }
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
        try {
            if (adminServer != null) {
                adminServer.shutdown();
            }
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }
    }

    // VisibleForTesting
    ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }

    // VisibleForTesting
    ServerCnxnFactory getSecureCnxnFactory() {
        return secureCnxnFactory;
    }

    /**
     * Shutdowns properly the service, this method is not a public API.
     */
    public void close() {
        ServerCnxnFactory primaryCnxnFactory = this.cnxnFactory;
        if (primaryCnxnFactory == null) {
            // in case of pure TLS we can hook into secureCnxnFactory
            primaryCnxnFactory = secureCnxnFactory;
        }
        if (primaryCnxnFactory == null || primaryCnxnFactory.getZooKeeperServer() == null) {
            return;
        }
        ZooKeeperServerShutdownHandler zkShutdownHandler = primaryCnxnFactory.getZooKeeperServer().getZkShutdownHandler();
        zkShutdownHandler.handle(ZooKeeperServer.State.SHUTDOWN);
        try {
            // ServerCnxnFactory will call the shutdown
            primaryCnxnFactory.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

}
