package dev.kdb.cli

/**
 * Public entry points for tooling and benchmarks that mirror one-shot CLI command bodies
 * without reopening the runtime (callers hold a [CliSession]).
 */
public suspend fun cliExecutePut(session: CliSession, payload: String) {
    CliCommands.executePut(session, payload)
}

public suspend fun cliExecuteGet(session: CliSession, docId: String) {
    CliCommands.executeGet(session, docId)
}

public suspend fun cliExecuteQuery(session: CliSession, sql: String) {
    CliCommands.executeQuery(session, sql)
}
