import java.io.BufferedReader
import java.net.ServerSocket
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!")

    // Uncomment the code below to pass the first stage
     var serverSocket = ServerSocket(6379)

    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
     serverSocket.reuseAddress = true

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
                    "SET" -> out.write("+OK\r\n".toByteArray())
                    "GET ${command[1]}" -> out.write("$${command[2].length}\r\n${command[2]}\r\n".toByteArray())
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
