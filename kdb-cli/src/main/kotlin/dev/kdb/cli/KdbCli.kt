package dev.kdb.cli

public object KdbCli {
    public fun run(args: Array<String>): Int {
        val (config, command) =
            try {
                parseArgs(args)
            } catch (e: IllegalArgumentException) {
                printUsage()
                System.err.println("Error: ${e.message}")
                return 2
            }
        if (command == null) {
            printUsage()
            return 2
        }
        return CliCommands.execute(config, command)
    }
}

public fun main(args: Array<String>) {
    kotlin.system.exitProcess(KdbCli.run(args))
}

private fun printUsage() {
    System.err.println(
        """
        kdb — KDB command-line interface (v1)

        Usage:
          kdb [--data-dir DIR] [--quiet] <command> ...

        Commands:
          init <namespace>
          put <namespace> <file|json>
          get <namespace> <docId>
          query <namespace> <sql>
          log <namespace>
          status <namespace>
          sync <namespace> <peer-uri>
          shell <namespace>          interactive REPL (put, query, get, …)
        """.trimIndent(),
    )
}
