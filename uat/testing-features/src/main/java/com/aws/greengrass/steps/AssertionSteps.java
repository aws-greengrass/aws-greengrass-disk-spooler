/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package com.aws.greengrass.steps;

import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.aws.greengrass.testing.model.TestContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.spi.HttpServerProvider;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;


@Log4j2
@ScenarioScoped
@SuppressWarnings("MissingJavadocMethod")
public class AssertionSteps implements Closeable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LogManager.getLogger(AssertionSteps.class);
    @Getter
    private final List<Assertion> assertionList = new CopyOnWriteArrayList<>();
    private final ScenarioContext scenarioContext;
    private final TestContext testContext;
    private final WaitSteps waits;
    private HttpServer server;
    @Getter
    private int port;

    @Inject
    @SuppressWarnings("MissingJavadocMethod")
    public AssertionSteps(TestContext testContext,
                          ScenarioContext scenarioContext,
                          WaitSteps waits) {

        this.testContext = testContext;
        this.scenarioContext = scenarioContext;
        this.waits = waits;
    }

    private void assertionHandler(HttpExchange httpExchange) {
        try {
            Assertion val = MAPPER.readValue(httpExchange.getRequestBody(), Assertion.class);
            assertionList.add(val);
            LOGGER.debug("Got assertion value {}", val);
            httpExchange.sendResponseHeaders(200, 0);
            httpExchange.getResponseBody().flush();
        } catch (IOException e) {
            LOGGER.error("Error decoding assertion", e);
        } finally {
            httpExchange.close();
        }
    }

    @Given("I start an assertion server")
    @SuppressWarnings("MissingJavadocMethod")
    public void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("Server already exists");
        }

        server = HttpServerProvider.provider().createHttpServer(new InetSocketAddress("localhost", 0),
                0);
        server.createContext("/assert", this::assertionHandler);
        LOGGER.debug("Starting HTTP assertion server");
        server.setExecutor(Executors.newCachedThreadPool(runnable -> {
            // Daemon-ize to allow main thread to end
            final Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName(String.format("AssertionServer-%d-%s", server.getAddress().getPort(),
                    testContext.testId().id()));
            thread.setDaemon(true);
            return thread;
        }));
        server.start();
        port = server.getAddress().getPort();
        LOGGER.info("Started HTTP assertion server at port {}", port);
        scenarioContext.put("assertionServerPort", Integer.toString(port));
    }


    @Then("I get {int} assertion(s) with context {string}")
    public void assertionsWithContext(int assertionCount, String context) throws Throwable {
        assertionsWithContextTimeout(assertionCount, scenarioContext.applyInline(context), null,
                100, true);
    }


    @Then("I get at least {int} assertion(s) with context {string}")
    public void atLeastNAssertionsWithContext(int assertionCount, String context) throws Throwable {
        assertionsWithContextTimeout(assertionCount, scenarioContext.applyInline(context), null,
                100, false);
    }


    @Then("I get at least {int} assertion(s) with context {string} within {long} seconds")
    public void atLeastNAssertionsWithContext(int assertionCount, String context, long timeoutSeconds)
            throws Throwable {
        assertionsWithContextTimeout(assertionCount, scenarioContext.applyInline(context),
                null, timeoutSeconds, false);
    }

    @Then("I get {int} assertion(s) with context {string} within {long} seconds")
    public void assertionsWithContextTimeout(int assertionCount, String context, long timeoutSeconds)
            throws Throwable {
        assertionsWithContextTimeout(assertionCount, scenarioContext.applyInline(context),
                null, timeoutSeconds, true);
    }


    @Then("I get {int} assertion(s) with context {string} and message {string} within {long} seconds")
    public void assertionsWithContextTimeout(int assertionCount, String context, String message,
                                                 long timeoutSeconds) throws Throwable {

        assertionsWithContextTimeout(assertionCount, scenarioContext.applyInline(context),
                message, timeoutSeconds, true);
    }

    @SuppressWarnings("PMD.AvoidCatchingThrowable")
    private void assertionsWithContextTimeout(int assertionCount, String context, String message,
                                              long timeoutSeconds, boolean failOnTooManyAssertions)
            throws Throwable {
        String exceptionMessage = (message == null)
                ? String.format("did not receive %d assertions for '%s' in time (%ds)", assertionCount,
                        context, timeoutSeconds)
                : String.format("did not receive %d assertions for '%s' with message '%s' in time (%ds)",
                assertionCount, context, message, timeoutSeconds);

        class Wrapper<T> {
            @Getter
            @Setter
            private T wrapped;
        }

        Wrapper<Throwable> wrapper = new Wrapper<>();
        boolean finished = waits.untilTrue(() -> {
            try {
                return assertionsReceived(assertionCount, context, message, failOnTooManyAssertions, exceptionMessage);
            } catch (Throwable e) {
                wrapper.setWrapped(e);
                return true;
            }
        }, (int) timeoutSeconds, TimeUnit.SECONDS);

        if (!finished) {
            throw new TimeoutException("Timeout: " + exceptionMessage);
        }

        Throwable e = wrapper.getWrapped();
        if (e != null) {
            throw e;
        }
    }

    private boolean assertionsReceived(int assertionCount, String context, String message,
                                          boolean failOnTooManyAssertions, String exceptionMessage) {
        List<Assertion> assertions = assertionList.stream()
                .filter(assertion -> assertion.getContext() != null)
                .filter(assertion -> context.equalsIgnoreCase(assertion.getContext()))
                .filter(assertion -> message == null || message.equalsIgnoreCase(assertion.getMessage()))
                .collect(Collectors.toList());
        for (Assertion assertion : assertions) {
            if (!assertion.isSuccess()) {
                fail(String.format("Assertion with message %s and context %s FAILED", assertion.getMessage(),
                        assertion.getContext()));
            }
        }
        if (failOnTooManyAssertions && assertions.size() > assertionCount) {
            fail(exceptionMessage);
        }
        return assertions.size() >= assertionCount;
    }

    @After
    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
            ((ExecutorService) server.getExecutor()).shutdownNow();
            LOGGER.info("Shut down HTTP assertion server");
        }
    }

    @When("I clear the assertions")
    public void clearTheAssertions() {
        assertionList.clear();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    static class Assertion {
        private boolean success;
        private String context;
        private String message;
    }
}
