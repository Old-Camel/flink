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

package org.apache.flink.table.client.cli;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.util.CollectionUtil;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** SQL CLI client. */
public class CliClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);

    private  Executor executor;

    private final String sessionId;

    private final Terminal terminal;

    private final LineReader lineReader = null;

    private final String prompt = null;

    private boolean isRunning;

    private static final int PLAIN_TERMINAL_WIDTH = 80;

    private static final int PLAIN_TERMINAL_HEIGHT = 30;

    private static final int SOURCE_MAX_SIZE = 50_000;

    /**
     * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance afterwards
     * using {@link #close()}.
     */
    @VisibleForTesting
    public CliClient(Terminal terminal, String sessionId, Executor executor, Path historyFilePath) {
        this.terminal = terminal;
        this.sessionId = sessionId;
        this.executor = executor;

    }

    /**
     * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
     * afterwards using {@link #close()}.
     */
    public CliClient(String sessionId, Executor executor) {
        //this(createDefaultTerminal(), sessionId, executor, historyFilePath);
        this(null, sessionId, executor, null);
    }

    public Terminal getTerminal() {
        return terminal;
    }

    public String getSessionId() {
        return this.sessionId;
    }

    public void clearTerminal() {
        if (isPlainTerminal()) {
            for (int i = 0; i < 200; i++) { // large number of empty lines
                terminal.writer().println();
            }
        } else {
            terminal.puts(InfoCmp.Capability.clear_screen);
        }
    }

    public boolean isPlainTerminal() {
        // check if terminal width can be determined
        // e.g. IntelliJ IDEA terminal supports only a plain terminal
        return terminal.getWidth() == 0 && terminal.getHeight() == 0;
    }

    public int getWidth() {
        if (isPlainTerminal()) {
            return PLAIN_TERMINAL_WIDTH;
        }
        return terminal.getWidth();
    }

    public int getHeight() {
        if (isPlainTerminal()) {
            return PLAIN_TERMINAL_HEIGHT;
        }
        return terminal.getHeight();
    }

    public Executor getExecutor() {
        return executor;
    }
    public void setExecutor(Executor executor) {
       this. executor=executor;
    }

    /** Opens the interactive CLI shell. */
    public void open() {
        isRunning = true;

        // print welcome
        terminal.writer().append(CliStrings.MESSAGE_WELCOME);

        // begin reading loop
        while (isRunning) {
            // make some space to previous command
            terminal.writer().append("\n");
            terminal.flush();

            final String line;
            try {
                line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
            } catch (UserInterruptException e) {
                // user cancelled line with Ctrl+C
                continue;
            } catch (EndOfFileException | IOError e) {
                // user cancelled application with Ctrl+D or kill
                break;
            } catch (Throwable t) {
                throw new SqlClientException("Could not read from command line.", t);
            }
            if (line == null) {
                continue;
            }
            final Optional<SqlCommandCall> cmdCall = parseCommand(line);
            cmdCall.ifPresent(this::callCommand);
        }
    }


    /** Closes the CLI instance. */
    @Override
    public void close() {
       /* try {
            terminal.close();
        } catch (IOException e) {
            throw new SqlClientException("Unable to close terminal.", e);
        }*/
    }

    /**
     * Submits a SQL update statement and prints status information and/or errors on the terminal.
     *
     * @param statement SQL update statement
     *
     * @return flag to indicate if the submission was successful or not
     */
    public boolean submitUpdate(String statement) {
        LOG.info(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE));
        terminal.writer().println(new AttributedString(statement).toString());
        terminal.flush();

        final Optional<SqlCommandCall> parsedStatement = parseCommand(statement);
        // only support INSERT INTO/OVERWRITE
        return parsedStatement
                .map(
                        cmdCall -> {
                            switch (cmdCall.command) {
                                case INSERT_INTO:
                                case INSERT_OVERWRITE:
                                    String str = callInsert(cmdCall);
                                    if (str.contains(CliStrings.MESSAGE_SQL_EXECUTION_ERROR)) {
                                        return false;
                                    } else {
                                        return true;
                                    }
                                default:
                                    printError(CliStrings.MESSAGE_UNSUPPORTED_SQL);
                                    return false;
                            }
                        })
                .orElse(false);
    }

    // --------------------------------------------------------------------------------------------

    public Optional<SqlCommandCall> parseCommand(String line) {
        final SqlCommandCall parsedLine;
        try {
            parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return Optional.empty();
        }
        return Optional.of(parsedLine);
    }

    public String callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case QUIT:
                //callQuit();
                break;
            case CLEAR:
                //callClear();
                break;
            case RESET:
               return  callReset();
            case SET:
                return  callSet(cmdCall);
            case HELP:
                //callHelp();
                break;
            case SHOW_CATALOGS:
                return callShowCatalogs();
            case SHOW_CURRENT_CATALOG:
                return callShowCurrentCatalog();
            case SHOW_DATABASES:
                return callShowDatabases();
            case SHOW_CURRENT_DATABASE:
                return callShowCurrentDatabase();
            case SHOW_TABLES:
                return  callShowTables();
            case SHOW_FUNCTIONS:
                return callShowFunctions();
            case SHOW_MODULES:
                return callShowModules();
            case SHOW_PARTITIONS:
                return   callShowPartitions(cmdCall);
            case USE_CATALOG:
                return  callUseCatalog(cmdCall);
            case USE:
                return  callUseDatabase(cmdCall);
            case DESC:
            case DESCRIBE:
                return  callDescribe(cmdCall);
            case EXPLAIN:
                return   callExplain(cmdCall);
            case SELECT:
                //callSelect(cmdCall);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                return    callInsert(cmdCall);
            case CREATE_TABLE:
                return    callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
            case DROP_TABLE:
                return    callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_REMOVED);
            case CREATE_VIEW:
                return   callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_CREATED);
            case DROP_VIEW:
                return    callDdl(cmdCall.operands[0], CliStrings.MESSAGE_VIEW_REMOVED);
            case ALTER_VIEW:
                return    callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_VIEW_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_VIEW_FAILED);
            case CREATE_FUNCTION:
                return   callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_CREATED);
            case DROP_FUNCTION:
                return    callDdl(cmdCall.operands[0], CliStrings.MESSAGE_FUNCTION_REMOVED);
            case ALTER_FUNCTION:
                return    callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_FUNCTION_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_FUNCTION_FAILED);
            case SOURCE:
                callSource(cmdCall);
                return null;
            case CREATE_DATABASE:
                return callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_CREATED);
            case DROP_DATABASE:
                return  callDdl(cmdCall.operands[0], CliStrings.MESSAGE_DATABASE_REMOVED);
            case ALTER_DATABASE:
                return callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_DATABASE_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_DATABASE_FAILED);
            case ALTER_TABLE:
                return  callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED,
                        CliStrings.MESSAGE_ALTER_TABLE_FAILED);
            case CREATE_CATALOG:
                return  callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_CREATED);
            case DROP_CATALOG:
                return callDdl(cmdCall.operands[0], CliStrings.MESSAGE_CATALOG_REMOVED);
            case LOAD_MODULE:
                return  callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_LOAD_MODULE_SUCCEEDED,
                        CliStrings.MESSAGE_LOAD_MODULE_FAILED);
            case UNLOAD_MODULE:
                return  callDdl(
                        cmdCall.operands[0],
                        CliStrings.MESSAGE_UNLOAD_MODULE_SUCCEEDED,
                        CliStrings.MESSAGE_UNLOAD_MODULE_FAILED);
            default:
                throw new SqlClientException("Unsupported command: " + cmdCall.command);
        }
        return null;
    }

    private void callQuit() {
        printInfo(CliStrings.MESSAGE_QUIT);
        isRunning = false;
    }

    private void callClear() {
        clearTerminal();
    }

    private String callReset() {
        try {
            executor.resetSessionProperties(sessionId);
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return CliStrings.MESSAGE_RESET;
    }

    private String callSet(SqlCommandCall cmdCall) {
        // show all properties
        if (cmdCall.operands.length == 0) {
            final Map<String, String> properties;
            try {
                properties = executor.getSessionProperties(sessionId);
            } catch (SqlExecutionException e) {
                return printExecutionException(e);
            }
            if (properties.isEmpty()) {
                return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
            } else {
                return properties.entrySet().stream()
                        .map((e) -> e.getKey() + "=" + e.getValue())
                        .sorted()
                        .collect(Collectors.joining("\n"));
            }
        }
        // set a property
        else {
            try {
                executor.setSessionProperty(
                        sessionId, cmdCall.operands[0], cmdCall.operands[1].trim());
            } catch (SqlExecutionException e) {
                return printExecutionException(e);
            }
            return CliStrings.messageInfo(CliStrings.MESSAGE_SET);
        }
    }

    private void callHelp() {
        terminal.writer().println(CliStrings.MESSAGE_HELP);
        terminal.flush();
    }

    public String callShowCatalogs() {
        final List<String> catalogs;
        try {
            catalogs = getShowResult("CATALOGS");
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (catalogs.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            return catalogs.stream().collect(Collectors.joining("\n"));
        }
    }

    public String callShowCurrentCatalog() {
        String currentCatalog;
        try {
            currentCatalog =
                    executor.executeSql(sessionId, "SHOW CURRENT CATALOG")
                            .collect()
                            .next()
                            .toString();
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return currentCatalog;
    }

    private String callShowDatabases() {
        final List<String> dbs;
        try {
            dbs = getShowResult("DATABASES");
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (dbs.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            return dbs.stream().collect(Collectors.joining("\n"));
        }
    }

    private String callShowCurrentDatabase() {
        String currentDatabase;
        try {
            currentDatabase =
                    executor.executeSql(sessionId, "SHOW CURRENT DATABASE")
                            .collect()
                            .next()
                            .toString();
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return currentDatabase;
    }

    private String callShowTables() {
        final List<String> tables;
        try {
            tables = getShowResult("TABLES");
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (tables.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            return tables.stream().collect(Collectors.joining("\n"));
        }
    }

    private String callShowFunctions() {
        final List<String> functions;
        try {
            functions = getShowResult("FUNCTIONS");
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (functions.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            Collections.sort(functions);
            return functions.stream().collect(Collectors.joining("\n"));
        }
    }

    public List<String> getShowResult(String objectToShow) {
        TableResult tableResult = executor.executeSql(sessionId, "SHOW " + objectToShow);
        return CollectionUtil.iteratorToList(tableResult.collect()).stream()
                .map(r -> checkNotNull(r.getField(0)).toString())
                .collect(Collectors.toList());
    }

    public List<String> getShowResult(SqlCommandCall cmdCall) {
        TableResult tableResult = executor.executeSql(sessionId, cmdCall.operands[0]);
        return CollectionUtil.iteratorToList(tableResult.collect()).stream()
                .map(r -> checkNotNull(r.getField(0)).toString())
                .collect(Collectors.toList());
    }

    private String callShowModules() {
        final List<String> modules;
        try {
            modules = executor.listModules(sessionId);
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (modules.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            // modules are already in the loaded order
            return modules.stream().collect(Collectors.joining("\n"));
        }
    }

    private String callShowPartitions(SqlCommandCall cmdCall) {
        final List<String> partitions;
        try {
            partitions = getShowResult(cmdCall);
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        if (partitions.isEmpty()) {
            return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
        } else {
            return partitions.stream().collect(Collectors.joining("\n"));
        }
    }

    private String callUseCatalog(SqlCommandCall cmdCall) {
        try {
            executor.executeSql(sessionId, "USE CATALOG " + cmdCall.operands[0]);
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return "SUCCESS TO USE CATALOG " + cmdCall.operands[0];
    }

    private String callUseDatabase(SqlCommandCall cmdCall) {
        try {
            executor.executeSql(sessionId, "USE " + cmdCall.operands[0]);
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return "SUCCESS TO USE DATABASE " + cmdCall.operands[0];
    }

    private String callDescribe(SqlCommandCall cmdCall) {
        final TableResult tableResult;
        try {
            tableResult = executor.executeSql(sessionId, "DESCRIBE " + cmdCall.operands[0]);
        } catch (SqlExecutionException e) {
            throw e;
        }
        return tableResult.collect().next().getField(0).toString();
    }

    private String callExplain(SqlCommandCall cmdCall) {
        final String explanation;
        try {
            TableResult tableResult = executor.executeSql(sessionId, cmdCall.operands[0]);
            explanation = tableResult.collect().next().getField(0).toString();
        } catch (SqlExecutionException e) {
            return printExecutionException(e);
        }
        return explanation;
    }

    private void callSelect(SqlCommandCall cmdCall) {
        final ResultDescriptor resultDesc;
        try {
            resultDesc = executor.executeQuery(sessionId, cmdCall.operands[0]);
        } catch (SqlExecutionException e) {
            printExecutionException(e);
            return;
        }

        if (resultDesc.isTableauMode()) {
            try (CliTableauResultView tableauResultView =
                         new CliTableauResultView(terminal, executor, sessionId, resultDesc)) {
                if (resultDesc.isMaterialized()) {
                    tableauResultView.displayBatchResults();
                } else {
                    tableauResultView.displayStreamResults();
                }
            } catch (SqlExecutionException e) {
                printExecutionException(e);
            }
        } else {
            final CliResultView view;
            if (resultDesc.isMaterialized()) {
                view = new CliTableResultView(this, resultDesc);
            } else {
                view = new CliChangelogResultView(this, resultDesc);
            }

            // enter view
            try {
                view.open();

                // view left
                printInfo(CliStrings.MESSAGE_RESULT_QUIT);
            } catch (SqlExecutionException e) {
                printExecutionException(e);
            }
        }
    }

    private String callInsert(SqlCommandCall cmdCall) {
        StringBuilder sb = new StringBuilder();
        sb.append(CliStrings.MESSAGE_SUBMITTING_STATEMENT).append("\n");

        try {
            final ProgramTargetDescriptor programTarget = executor.executeUpdate(
                    sessionId,
                    cmdCall.operands[0]);
            sb.append(CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED)).append("\n");
            sb.append(programTarget.toString());
        } catch (SqlExecutionException e) {
            sb.append(printExecutionException(e));
        }
        return sb.toString();
    }

    private void callSource(SqlCommandCall cmdCall) {
        final String pathString = cmdCall.operands[0];

        // load file
        final String stmt;
        try {
            final Path path = Paths.get(pathString);
            byte[] encoded = Files.readAllBytes(path);
            stmt = new String(encoded, Charset.defaultCharset());
        } catch (IOException e) {
            printExecutionException(e);
            return;
        }

        // limit the output a bit
        if (stmt.length() > SOURCE_MAX_SIZE) {
            printExecutionError(CliStrings.MESSAGE_MAX_SIZE_EXCEEDED);
            return;
        }
        System.out.println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE));
        // try to run it
        final Optional<SqlCommandCall> call = parseCommand(stmt);
        call.ifPresent(this::callCommand);
    }

    private String callDdl(String ddl, String successMessage) {
      return  callDdl(ddl, successMessage, null);
    }

    private String callDdl(String ddl, String successMessage, String errorMessage) {
        try {
            executor.executeSql(sessionId, ddl);
            return printInfo(successMessage);
        } catch (SqlExecutionException e) {
            return printExecutionException(errorMessage, e);
        }
    }

    // --------------------------------------------------------------------------------------------

    private String printExecutionException(Throwable t) {
        return printExecutionException(null, t);
    }

    private String printExecutionException(String message, Throwable t) {
        final String finalMessage;
        if (message == null) {
            finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        } else {
            finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR + ' ' + message;
        }
        return printException(finalMessage, t);
    }

    private String printExecutionError(String message) {
        return CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, message);
    }

    private String printException(String message, Throwable t) {
        LOG.warn(message, t);
        return CliStrings.messageError(message, t);
    }

    private String printError(String message) {
        return CliStrings.messageError(message);
    }

    private String printInfo(String message) {
        return CliStrings.messageInfo(message);
    }

    // --------------------------------------------------------------------------------------------

    private static Terminal createDefaultTerminal() {
        try {
            return TerminalBuilder.builder().name(CliStrings.CLI_NAME).build();
        } catch (IOException e) {
            throw new SqlClientException("Error opening command line interface.", e);
        }
    }
}
