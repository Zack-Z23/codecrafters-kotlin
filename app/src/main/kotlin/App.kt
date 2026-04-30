import java.io.BufferedReader
import java.net.ServerSocket
import javax.swing.text.html.parser.Parser
import kotlin.concurrent.thread
import kotlin.time.Duration

fun main(args: Array<String>) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!")

    // Uncomment the code below to pass the first stage
    var serverSocket = ServerSocket(6379)

    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
    serverSocket.reuseAddress = true
    val store = java.util.concurrent.ConcurrentHashMap<String, Pair<String, Long?>>()
    val listOflists = java.util.concurrent.ConcurrentHashMap<String, MutableList<String>>()
    val streams = java.util.concurrent.ConcurrentHashMap<String, MutableList<Pair<String, Map<String, String>>>>()
    while (true) {
        val client = serverSocket.accept()
        thread {
            val input = client.getInputStream()
            val out = client.getOutputStream()
            var inTransaction = false
            val transactions = mutableListOf<List<String>>()
            val watchedKeys = mutableSetOf<String>()
            while (true) {
                val command = parseCommand(input.bufferedReader())
                if (inTransaction && command[0].uppercase() !in listOf("EXEC", "MULTI", "DISCARD", "WATCH")) {
                    transactions.add(command)
                    out.write("+QUEUED\r\n".toByteArray())
                    out.flush()
                    continue
                }
                if (command == null) break
                when (command[0].uppercase()) {
                    "PING" -> out.write("+PONG\r\n".toByteArray())
                    "ECHO" -> out.write("$${command[1].length}\r\n${command[1]}\r\n".toByteArray())
                    "SET" -> {
                        if (command.size >= 5) {
                            if (command[3] != null) {
                                when (command[3].uppercase()) {
                                    "EX" -> store[command[1]] =
                                        Pair(command[2], System.currentTimeMillis() + (command[4].toLong() * 1000))

                                    "PX" -> store[command[1]] =
                                        Pair(command[2], System.currentTimeMillis() + command[4].toLong())

                                }
                            }
                            out.write("+OK\r\n".toByteArray())

                        } else {
                            store[command[1]] = Pair(command[2], null)
                            out.write("+OK\r\n".toByteArray())
                        }
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
                        if (!listOflists.containsKey(command[1])) {
                            listOflists[command[1]] = mutableListOf()
                        }
                        var i = 2
                        while (command.size >= 3 && i < command.size) {
                            listOflists[command[1]]!!.add(command[i])
                            i++
                        }
                        synchronized(listOflists) {
                            (listOflists as Object).notifyAll()
                        }
                        out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                    }
                    "LRANGE" -> {
                        var startIndex: Int = command[2].toInt()
                        var endIndex = command[3].toInt()
                        val list = listOflists[command[1]]

                        if (list == null) {
                            out.write("*0\r\n".toByteArray())
                        } else {
                            if (startIndex <= -1) {
                                startIndex = startIndex * -1
                                if (startIndex > list.size) {
                                    startIndex = 0

                                    startIndex = list.size - startIndex
                                }
                                startIndex = (list?.size?.minus(startIndex))!!

                            }
                            if (endIndex <= -1) {
                                endIndex = endIndex * -1
                                if (endIndex > list.size) {
                                    endIndex = (list?.size?.minus(1))!!
                                } else {
                                    endIndex = (list?.size?.minus(endIndex))!!
                                }
                            }
                            if (endIndex >= list.size) {
                                endIndex = list.size - 1
                            }

                            out.write("*${endIndex - startIndex + 1}\r\n".toByteArray())
                            for (i in startIndex..endIndex) {

                                out.write("$${list[i].length}\r\n${list[i]}\r\n".toByteArray())
                            }
                        }

                    }

                    "LPUSH" -> {
                        if (!listOflists.containsKey(command[1])) {
                            listOflists[command[1]] = mutableListOf()
                        }
                        var i = 2
                        while (command.size >= 3 && i < command.size) {
                            listOflists[command[1]]!!.addFirst(command[i])
                            i++
                            //commmand [2] = apple
                            //command [3] = orange
                            // array = 0 1 2 3

                        }
                        out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                    }

                    "LLEN" -> {
                        if (!listOflists.containsKey(command[1])) {
                            listOflists[command[1]] = mutableListOf()
                            out.write(":0\r\n".toByteArray())
                        } else {
                            out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                        }

                    }

                    "LPOP" -> {
                        var i = 0
                        val list = listOflists[command[1]]!!
                        if (!listOflists.containsKey(command[1])) {
                            out.write("$-1\r\n".toByteArray())

                        } else {
                            if (command.size >= 3) {

                                out.write("*${command[2].toInt()}\r\n".toByteArray())
                                while (i < command[2].toInt()) {
                                    out.write("$${list[0].length}\r\n${list[0]}\r\n".toByteArray())
                                    list.removeFirst()
                                    i++
                                }
                            } else {
                                out.write("$${list[0].length}\r\n${list[0]}\r\n".toByteArray())
                                list.removeFirst()
                            }
                        }
                    }

                    "BLPOP" -> {
                        val key = command[1]
                        val timeout = command[2].toDouble()
                        val startTime = System.currentTimeMillis()

                        synchronized(listOflists) {
                            while (true) {
                                val list = listOflists[key]
                                if (list != null && list.isNotEmpty()) {
                                    val value = list.removeFirst()
                                    out.write("*2\r\n".toByteArray())
                                    out.write("$${key.length}\r\n$key\r\n".toByteArray())
                                    out.write("$${value.length}\r\n$value\r\n".toByteArray())
                                    out.flush()
                                    break
                                }

                                if (timeout == 0.0) {
                                    (listOflists as Object).wait()
                                } else {
                                    val elapsed = System.currentTimeMillis() - startTime
                                    val remaining = (timeout * 1000).toLong() - elapsed
                                    if (remaining <= 0) {
                                        out.write("*-1\r\n".toByteArray())
                                        out.flush()
                                        break
                                    }
                                    (listOflists as Object).wait(remaining)
                                }
                            }
                        }
                    }

                    "TYPE" -> {
                        val key = command[1]
                        if (store.containsKey(key)) {
                            out.write("+string\r\n".toByteArray())
                        } else if (listOflists.containsKey(key)) {
                            out.write("+list\r\n".toByteArray())
                        } else if (streams.containsKey(key)) {
                            out.write("+stream\r\n".toByteArray())
                        } else {
                            out.write("+none\r\n".toByteArray())
                        }
                    }

                    "XADD" -> {
                        val key = command[1]
                        var id = command[2]

                        val stream = streams.getOrPut(key) { mutableListOf() }

                        var ms: Long
                        var seq: Long

                        if (id == "*") {
                            ms = System.currentTimeMillis()

                            val sameMs = stream.filter { it.first.startsWith("$ms-") }

                            seq = if (sameMs.isEmpty()) {
                                0
                            } else {
                                val lastSeq = sameMs.last().first.split("-")[1].toLong()
                                lastSeq + 1
                            }

                            id = "$ms-$seq"
                        } else if (id.contains("*")) {
                            val parts = id.split("-")
                            ms = parts[0].toLong()

                            val sameMs = stream.filter { it.first.startsWith("$ms-") }

                            seq = if (sameMs.isEmpty()) {
                                if (ms == 0L) 1 else 0
                            } else {
                                val lastSeq = sameMs.last().first.split("-")[1].toLong()
                                lastSeq + 1
                            }

                            id = "$ms-$seq"
                        } else {
                            val parts = id.split("-")
                            ms = parts[0].toLong()
                            seq = parts[1].toLong()
                        }

                        if (ms == 0L && seq == 0L) {
                            out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".toByteArray())
                        } else if (stream.isNotEmpty()) {
                            val lastId = stream.last().first
                            val lastParts = lastId.split("-")
                            val lastMs = lastParts[0].toLong()
                            val lastSeq = lastParts[1].toLong()

                            if (ms < lastMs || (ms == lastMs && seq <= lastSeq)) {
                                out.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".toByteArray())
                            } else {
                                val fields = mutableMapOf<String, String>()
                                var i = 3
                                while (i < command.size) {
                                    fields[command[i]] = command[i + 1]
                                    i += 2
                                }

                                stream.add(Pair(id, fields))

                                synchronized(streams) {
                                    (streams as Object).notifyAll()
                                }

                                out.write("$${id.length}\r\n${id}\r\n".toByteArray())
                            }
                        } else {
                            if (ms == 0L && seq <= 0L) {
                                out.write("-ERR The ID specified in XADD must be greater than 0-0\r\n".toByteArray())
                            } else {
                                val fields = mutableMapOf<String, String>()
                                var i = 3
                                while (i < command.size) {
                                    fields[command[i]] = command[i + 1]
                                    i += 2
                                }

                                stream.add(Pair(id, fields))
                                out.write("$${id.length}\r\n${id}\r\n".toByteArray())
                            }
                        }
                    }

                    "XRANGE" -> {
                        val key = command[1]
                        val startRaw = command[2]
                        val endRaw = command[3]

                        val stream = streams[key]

                        if (stream == null || stream.isEmpty()) {
                            out.write("*0\r\n".toByteArray())
                        } else {
                            fun parseStart(id: String): Pair<Long, Long> {
                                return when (id) {
                                    "-" -> Pair(Long.MIN_VALUE, Long.MIN_VALUE)
                                    else -> {
                                        if (id.contains("-")) {
                                            val p = id.split("-")
                                            Pair(p[0].toLong(), p[1].toLong())
                                        } else {
                                            Pair(id.toLong(), 0L)
                                        }
                                    }
                                }
                            }

                            fun parseEnd(id: String): Pair<Long, Long> {
                                return when (id) {
                                    "+" -> Pair(Long.MAX_VALUE, Long.MAX_VALUE)
                                    else -> {
                                        if (id.contains("-")) {
                                            val p = id.split("-")
                                            Pair(p[0].toLong(), p[1].toLong())
                                        } else {
                                            Pair(id.toLong(), Long.MAX_VALUE)
                                        }
                                    }
                                }
                            }

                            val (startMs, startSeq) = parseStart(startRaw)
                            val (endMs, endSeq) = parseEnd(endRaw)

                            val filtered = stream.filter {
                                val parts = it.first.split("-")
                                val ms = parts[0].toLong()
                                val seq = parts[1].toLong()

                                (ms > startMs || (ms == startMs && seq >= startSeq)) && (ms < endMs || (ms == endMs && seq <= endSeq))
                            }

                            out.write("*${filtered.size}\r\n".toByteArray())

                            for ((id, fields) in filtered) {
                                out.write("*2\r\n".toByteArray())

                                out.write("$${id.length}\r\n${id}\r\n".toByteArray())

                                val flat = fields.entries.flatMap { listOf(it.key, it.value) }
                                out.write("*${flat.size}\r\n".toByteArray())

                                for (v in flat) {
                                    out.write("$${v.length}\r\n${v}\r\n".toByteArray())
                                }
                            }
                        }
                    }

                    "XREAD" -> {

                        try {
                            var blockTime: Long? = null
                            var streamsIndex: Int

                            if (command[1].equals("BLOCK", ignoreCase = true)) {
                                blockTime = command[2].toLong()
                                streamsIndex = 3
                            } else {
                                streamsIndex = 1
                            }

                            val keysAndIds = command.subList(streamsIndex + 1, command.size)
                            val half = keysAndIds.size / 2

                            val keys = keysAndIds.subList(0, half)
                            val ids = keysAndIds.subList(half, keysAndIds.size)

                            val startTime = System.currentTimeMillis()

                            // resolve IDs ONCE (snapshot semantics)
                            val resolvedIds = mutableListOf<Pair<Long, Long>>()

                            for (i in keys.indices) {
                                val key = keys[i]
                                val startRaw = ids[i]
                                val stream = streams[key]

                                val resolved = if (startRaw == "$") {
                                    if (stream == null || stream.isEmpty()) {
                                        Pair(Long.MAX_VALUE, Long.MAX_VALUE)
                                    } else {
                                        val lastId = stream.last().first
                                        val p = lastId.split("-")
                                        Pair(p[0].toLong(), p[1].toLong())
                                    }
                                } else {
                                    val p = startRaw.split("-")
                                    Pair(p[0].toLong(), if (p.size > 1) p[1].toLong() else 0L)
                                }

                                resolvedIds.add(resolved)
                            }

                            while (true) {
                                val results =
                                    mutableListOf<Pair<String, List<Pair<String, Map<String, String>>>>>()

                                for (i in keys.indices) {
                                    val key = keys[i]
                                    val stream = streams[key] ?: continue
                                    val (startMs, startSeq) = resolvedIds[i]

                                    val filtered = stream.filter { entry ->
                                        val p = entry.first.split("-")
                                        val ms = p[0].toLong()
                                        val seq = p[1].toLong()

                                        (ms > startMs) || (ms == startMs && seq > startSeq)
                                    }

                                    if (filtered.isNotEmpty()) {
                                        results.add(Pair(key, filtered))
                                    }
                                }

                                if (results.isNotEmpty()) {
                                    out.write("*${results.size}\r\n".toByteArray())

                                    for ((key, entries) in results) {
                                        out.write("*2\r\n".toByteArray())
                                        out.write("$${key.length}\r\n${key}\r\n".toByteArray())

                                        out.write("*${entries.size}\r\n".toByteArray())

                                        for ((id, fields) in entries) {
                                            out.write("*2\r\n".toByteArray())
                                            out.write("$${id.length}\r\n${id}\r\n".toByteArray())

                                            val flat = fields.entries.flatMap { listOf(it.key, it.value) }
                                            out.write("*${flat.size}\r\n".toByteArray())

                                            for (v in flat) {
                                                out.write("$${v.length}\r\n${v}\r\n".toByteArray())
                                            }
                                        }
                                    }
                                    break
                                }
                                if (blockTime == null) {
                                    out.write("*0\r\n".toByteArray())
                                    break
                                }

                                if (blockTime == 0L) {
                                    synchronized(streams) {
                                        (streams as Object).wait()
                                    }
                                    continue
                                }

                                val elapsed = System.currentTimeMillis() - startTime
                                val remaining = blockTime - elapsed

                                if (remaining <= 0) {
                                    out.write("*-1\r\n".toByteArray())
                                    break
                                }

                                synchronized(streams) {
                                    (streams as Object).wait(remaining)
                                }
                            }
                        } catch (e: Exception) {
                            out.write("*-1\r\n".toByteArray())
                        }
                    }
                    "INCR" -> {
                        val key = command[1]
                        val entry = store[key]

                        if (entry == null) {
                            store[key] = Pair("1", null)
                            out.write(":1\r\n".toByteArray())
                        } else {
                            val value = entry.first

                            try {
                                val num = value.toLong() + 1
                                store[key] = Pair(num.toString(), entry.second)

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
                            out.flush()
                            return@thread
                        }

                        val responses = mutableListOf<String>()

                        for (cmd in transactions) {
                            val result = executeCommand(cmd, store)
                            responses.add(result)
                        }

                        out.write("*${responses.size}\r\n".toByteArray())
                        for (res in responses) {
                            out.write(res.toByteArray())
                        }

                        out.flush()

                        transactions.clear()
                        inTransaction = false
                    }
                    "DISCARD" -> {
                        if (!inTransaction) {
                            out.write("-ERR DISCARD without MULTI\r\n".toByteArray())
                        } else {
                            transactions.clear()
                            inTransaction = false
                            out.write("+OK\r\n".toByteArray())
                        }
                    }
                    "WATCH" -> {
                        if (inTransaction) {
                            out.write("-ERR WATCH inside MULTI is not allowed\r\n".toByteArray())
                        } else {
                            for (i in 1 until command.size) {
                                watchedKeys.add(command[i])
                            }
                            out.write("+OK\r\n".toByteArray())
                        }
                    }
                }
                out.flush()
            }

        }
    }
}

fun executeCommand(command: List<String>, store: MutableMap<String, Pair<String, Long?>>): String {
    return when (command[0].uppercase()) {

        "SET" -> {
            if (command.size >= 5 && command[3] != null) {
                when (command[3].uppercase()) {
                    "EX" -> store[command[1]] = Pair(command[2], System.currentTimeMillis() + (command[4].toLong() * 1000))
                    "PX" -> store[command[1]] = Pair(command[2], System.currentTimeMillis() + command[4].toLong())
                }
            } else {
                store[command[1]] = Pair(command[2], null)
            }
            "+OK\r\n"
        }

        "INCR" -> {
            val key = command[1]
            val entry = store[key]

            if (entry == null) {
                store[key] = Pair("1", null)
                ":1\r\n"
            } else {
                val num = entry.first.toLongOrNull()
                if (num == null) {
                    "-ERR value is not an integer or out of range\r\n"
                } else {
                    val newVal = num + 1
                    store[key] = Pair(newVal.toString(), entry.second)
                    ":$newVal\r\n"
                }
            }
        }

        "GET" -> {
            val entry = store[command[1]]
            val value = entry?.first
            val expiry = entry?.second

            if (value == null || (expiry != null && expiry <= System.currentTimeMillis())) {
                "$-1\r\n"
            } else {
                "$${value.length}\r\n${value}\r\n"
            }
        }

        else -> "-ERR unknown command\r\n"
    }
}

fun parseCommand(reader: BufferedReader): List<String> {
    val firstLine = reader.readLine()
    val numElements = firstLine.substring(1).toInt()
    val result = mutableListOf<String>()

    repeat(numElements) {
        reader.readLine()
        val value = reader.readLine()
        result.add(value)

    }
    return result
}
