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
            while(true) {
                val command = parseCommand(input.bufferedReader())
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

                        }
                        else{
                            store[command[1]] = Pair(command[2], null)
                            out.write("+OK\r\n".toByteArray())
                        }
                    }

                    "GET" -> {
                        val entry = store[command[1]]
                        val value = entry?.first
                        val expiry = entry?.second
                        if (expiry != null) {
                            if( value == null || expiry <= System.currentTimeMillis()) {
                                out.write("$-1\r\n".toByteArray())
                            }
                            else {
                                out.write("$${value.length}\r\n${value}\r\n".toByteArray())
                            }
                        }
                        else {
                            out.write("$${value?.length}\r\n${value}\r\n".toByteArray())
                        }
                    }
                    "RPUSH" -> {
                        if(!listOflists.containsKey(command[1])) {
                            listOflists[command[1]] = mutableListOf()
                        }
                        var i = 2
                        while (command.size >= 3 && i < command.size) {

                            listOflists[command[1]]!!.add(command[i])
                            i++

                           synchronized(listOflists) {
                               (listOflists as Object).notifyAll()
                           }
                        }

                        out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                    }
                    "LRANGE" -> {
                        var startIndex: Int = command[2].toInt()
                        var endIndex = command[3].toInt()
                        val list = listOflists[command[1]]

                        if(list == null){
                            out.write("*0\r\n".toByteArray())
                        }

                        else{
                            if(startIndex <= -1) {
                                startIndex = startIndex * -1
                                if(startIndex > list.size){
                                    startIndex = 0

                                    startIndex = list.size - startIndex
                                }
                                startIndex = (list?.size?.minus(startIndex))!!

                            }
                            if(endIndex <= -1) {
                                endIndex = endIndex * -1
                                if(endIndex > list.size){
                                    endIndex = (list?.size?.minus(1))!!
                                }
                                else {
                                    endIndex = (list?.size?.minus(endIndex))!!
                                }
                            }
                            if(endIndex >= list.size){
                                endIndex = list.size - 1
                            }

                            out.write("*${endIndex - startIndex + 1}\r\n".toByteArray())
                            for(i in startIndex .. endIndex){

                                out.write("$${list[i].length}\r\n${list[i]}\r\n".toByteArray())
                            }
                        }

                    }
                    "LPUSH" -> {
                        if(!listOflists.containsKey(command[1])) {
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
                        if(!listOflists.containsKey(command[1])) {
                            listOflists[command[1]] = mutableListOf()
                            out.write(":0\r\n".toByteArray())
                        }
                        else {
                            out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                        }

                    }
                    "LPOP" -> {
                        var i = 0
                        val list = listOflists[command[1]]!!
                        if(!listOflists.containsKey(command[1])) {
                            out.write("$-1\r\n".toByteArray())

                        }

                        else {
                            if( command.size >= 3) {

                                out.write("*${command[2].toInt()}\r\n".toByteArray())
                                while (i < command[2].toInt()) {
                                    out.write("$${list[0].length}\r\n${list[0]}\r\n".toByteArray())
                                    list.removeFirst()
                                    i++
                                }
                            }
                            else{
                                out.write("$${list[0].length}\r\n${list[0]}\r\n".toByteArray())
                                list.removeFirst()
                            }
                        }
                    }
                    "BLPOP" -> {
                        val timeout = command[2].toDouble()
                        synchronized(listOflists) {
                            val list = listOflists.getOrPut(command[1]) { mutableListOf() }

                            while (list.isEmpty()) {
                                if (timeout == 0.0) {
                                    (listOflists as Object).wait()
                                } else {
                                    val calc = (timeout * 1000).toLong()
                                    (listOflists as Object).wait(calc)
                                    if(list.isEmpty()){
                                        out.write("*-1\r\n".toByteArray())
                                        return@thread
                                    }
                                }
                            }
                            val value = list.removeFirst()
                            out.write("*2\r\n".toByteArray())
                            out.write("$${command[1].length}\r\n${command[1]}\r\n".toByteArray())
                            out.write("$${value?.length}\r\n${value}\r\n".toByteArray())
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

                }
                out.flush()
            }

        }
     }
}



fun parseCommand(reader : BufferedReader) : List<String> {
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
