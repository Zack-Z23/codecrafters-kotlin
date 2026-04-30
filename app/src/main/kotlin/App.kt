import java.io.BufferedReader
import java.net.ServerSocket
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    System.err.println("Logs from your program will appear here!")

    val serverSocket = ServerSocket(6379)
    serverSocket.reuseAddress = true

    val store = java.util.concurrent.ConcurrentHashMap<String, Pair<String, Long?>>()
    val listOflists = java.util.concurrent.ConcurrentHashMap<String, MutableList<String>>()
    val streams = java.util.concurrent.ConcurrentHashMap<String, MutableList<Pair<String, Map<String, String>>>>()

    while (true) {
        val client = serverSocket.accept()
        thread {
            var inTransaction = false
            val transactions = mutableListOf<List<String>>()

            val input = client.getInputStream()
            val out = client.getOutputStream()

            while (true) {
                val command = parseCommand(input.bufferedReader()) ?: break

                if (inTransaction && command[0].uppercase() !in listOf("EXEC", "MULTI")) {
                    transactions.add(command)
                    out.write("+QUEUED\r\n".toByteArray())
                    out.flush()
                    continue
                }

                when (command[0].uppercase()) {
                    "PING" -> out.write("+PONG\r\n".toByteArray())

                    "ECHO" -> out.write("$${command[1].length}\r\n${command[1]}\r\n".toByteArray())

                    "SET" -> {
                        if (command.size >= 5) {
                            when (command[3].uppercase()) {
                                "EX" -> store[command[1]] =
                                    Pair(command[2], System.currentTimeMillis() + command[4].toLong() * 1000)
                                "PX" -> store[command[1]] =
                                    Pair(command[2], System.currentTimeMillis() + command[4].toLong())
                            }
                        } else {
                            store[command[1]] = Pair(command[2], null)
                        }
                        out.write("+OK\r\n".toByteArray())
                    }

                    "GET" -> {
                        val entry = store[command[1]]
                        val value = entry?.first
                        val expiry = entry?.second

                        if (value == null || (expiry != null && expiry <= System.currentTimeMillis())) {
                            out.write("$-1\r\n".toByteArray())
                        } else {
                            out.write("$${value.length}\r\n${value}\r\n".toByteArray())
                        }
                    }

                    "RPUSH" -> {
                        val list = listOflists.getOrPut(command[1]) { mutableListOf() }
                        for (i in 2 until command.size) list.add(command[i])
                        out.write(":${list.size}\r\n".toByteArray())
                    }

                    "LPUSH" -> {
                        val list = listOflists.getOrPut(command[1]) { mutableListOf() }
                        for (i in 2 until command.size) list.add(0, command[i])
                        out.write(":${list.size}\r\n".toByteArray())
                    }

                    "LLEN" -> {
                        val size = listOflists[command[1]]?.size ?: 0
                        out.write(":$size\r\n".toByteArray())
                    }

                    "LPOP" -> {
                        val list = listOflists[command[1]]
                        if (list == null || list.isEmpty()) {
                            out.write("$-1\r\n".toByteArray())
                        } else {
                            val value = list.removeAt(0)
                            out.write("$${value.length}\r\n${value}\r\n".toByteArray())
                        }
                    }

                    "INCR" -> {
                        val entry = store[command[1]]
                        if (entry == null) {
                            store[command[1]] = Pair("1", null)
                            out.write(":1\r\n".toByteArray())
                        } else {
                            try {
                                val num = entry.first.toLong() + 1
                                store[command[1]] = Pair(num.toString(), entry.second)
                                out.write(":$num\r\n".toByteArray())
                            } catch (e: Exception) {
                                out.write("-ERR value is not an integer or out of range\r\n".toByteArray())
                            }
                        }
                    }

                    "MULTI" -> {
                        inTransaction = true
                        transactions.clear()
                        out.write("+OK\r\n".toByteArray())
                    }

                    "EXEC" -> {
                        if (!inTransaction) {
                            out.write("-ERR EXEC without MULTI\r\n".toByteArray())
                        } else {
                            out.write("*0\r\n".toByteArray())
                            inTransaction = false
                            transactions.clear()
                        }
                    }
                }

                out.flush()
            }
        }
    }
}

fun parseCommand(reader: BufferedReader): List<String>? {
    val firstLine = reader.readLine() ?: return null
    val numElements = firstLine.substring(1).toInt()
    val result = mutableListOf<String>()

    repeat(numElements) {
        reader.readLine()
        val value = reader.readLine()
        result.add(value)
    }

    return result
}