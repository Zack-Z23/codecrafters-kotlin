import java.io.BufferedReader
import java.net.ServerSocket
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
                            //commmand [2] = apple
                            //command [3] = orange
                            // array = 0 1 2 3
                        }
                        out.write(":${listOflists[command[1]]?.size}\r\n".toByteArray())
                    }
                    "LRANGE" -> {
                        val startIndex = command[2].toInt()
                        var endIndex = command[3].toInt()
                        val list = listOflists[command[1]]

                        if(list == null){
                            out.write("*0\r\n".toByteArray())
                        }
                        else{
                            out.write("*${endIndex - startIndex + 1}\r\n".toByteArray())
                            if(endIndex >= list.size){
                                endIndex = list.size - 1
                            }
                            for(i in startIndex .. endIndex){

                                out.write("$${list[i].length}\r\n${list[i]}\r\n".toByteArray())
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
