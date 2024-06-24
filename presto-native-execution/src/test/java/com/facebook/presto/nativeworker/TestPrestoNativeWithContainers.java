/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestPrestoNativeWithContainers
{
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final int NATIVE_SESSION_PROPERTY_COUNT = 17;
    private static final int NATIVE_FUNCTION_COUNT = 1113;
    private static final String BASE_DIR = System.getProperty("user.dir");
    private static final Network network = Network.newNetwork();
    private GenericContainer<?> coordinator;
    private GenericContainer<?> sidecar;
    private GenericContainer<?> worker;

    @BeforeClass
    public void setUp()
            throws InterruptedException
    {
        coordinator = new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(8081)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(120));

        sidecar = new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(7778)
                .withNetwork(network).withNetworkAliases("presto-sidecar")
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/sidecar-etc", "/opt/presto-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));

        worker = new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(7777)
                .withNetwork(network).withNetworkAliases("presto-worker")
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/velox-etc", "/opt/presto-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));

        coordinator.start();
        sidecar.start();
        worker.start();

        // Wait for sidecar to announce itself.
        TimeUnit.SECONDS.sleep(20);
    }

    @AfterClass
    public void tearDown()
    {
        coordinator.stop();
        sidecar.stop();
        worker.stop();
    }

    private Container.ExecResult executeQuery(String sql)
            throws IOException, InterruptedException
    {
        // Command to run inside the coordinator container using the presto-cli.
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:8081",
                "--execute",
                sql
        };

        Container.ExecResult execResult = coordinator.execInContainer(command);
        if (execResult.getExitCode() != 0) {
            String errorDetails = "Stdout: " + execResult.getStdout() + "\nStderr: " + execResult.getStderr();
            fail("Presto CLI exited with error code: " + execResult.getExitCode() + "\n" + errorDetails);
        }
        return execResult;
    }

    private Container.ExecResult executeQueryFail(String sql)
            throws IOException, InterruptedException
    {
        // Command to run inside the coordinator container using the presto-cli.
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:8081",
                "--execute",
                sql
        };

        Container.ExecResult execResult = coordinator.execInContainer(command);
        assertTrue(execResult.getExitCode() != 0, "Query succeeded, expected failure");
        return execResult;
    }

    @Test
    public void testShowSession()
            throws IOException, InterruptedException
    {
        String showSession = "SHOW SESSION";
        Container.ExecResult execResult = executeQuery(showSession);

        // Validate number of native session properties.
        String[] lines = execResult.getStdout().split("\n");
        int nativeSessionPropertyCount = 0;
        for (String line : lines) {
            if (line.contains("Native Execution only")) {
                nativeSessionPropertyCount++;
            }
        }
        assertEquals(nativeSessionPropertyCount, NATIVE_SESSION_PROPERTY_COUNT, "Mismatch in the number of native session properties");
    }

    @Test
    public void testShowFunctions()
            throws IOException, InterruptedException
    {
        String showFunctions = "SHOW FUNCTIONS";
        Container.ExecResult execResult = executeQuery(showFunctions);

        // Validate number of native session properties.
        String[] lines = execResult.getStdout().split("\n");
        int nativeFunctionsCount = 0;
        for (String line : lines) {
            if (line.contains("native.default.")) {
                nativeFunctionsCount++;
            }
            else {
                fail(format("no namespace match found for function: %s", line));
            }
        }
        assertEquals(nativeFunctionsCount, NATIVE_FUNCTION_COUNT, "Mismatch in the number of native functions");
    }
}
