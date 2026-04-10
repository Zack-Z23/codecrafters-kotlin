import java.net.ServerSocket

fun main(args: Array<String>) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!")

    // Uncomment the code below to pass the first stage
     var serverSocket = ServerSocket(6379)

    // // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // // ensures that we don't run into 'Address already in use' errors
     serverSocket.reuseAddress = true


    val client = serverSocket.accept()
    val out = client.getOutputStream()
    val intput = client.getInputStream().bufferedReader().readText()

    while (true) {
        if (intput == "") {
            break
        }
        out.write("+PONG\r\n".toByteArray())


        out.flush()
    }

}
