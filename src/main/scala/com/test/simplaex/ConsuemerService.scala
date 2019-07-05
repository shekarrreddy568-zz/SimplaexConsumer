package com.test.simplaex

import java.io._
import java.net.Socket
import scala.collection.mutable.ListBuffer

object ConsuemerService {

  def main(args: Array[String]): Unit = {

    val hostname: String = args(0)
    val outputDir: String = args(1)
    val socket = new Socket(hostname, 9000, true)
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    println("reading!....")
    var i = 1
    var n = 1000
    val noOfLinesToProcess = 1000
    val linesListBuffer = new ListBuffer[Array[String]]()

    while (true) {
      // Reading the lines and splitting and appending to list buffer
      val line = in.readLine().split(",")
      linesListBuffer.append(line)
      val linesList = linesListBuffer.toList

      if (linesList.size == n) {

        var sum: BigInt = 0
        val UsersListBuffer = new ListBuffer[String]()

        linesList.takeRight(noOfLinesToProcess).foreach(list => {
          sum += BigInt(list(4))
          UsersListBuffer.append(list(0))
        })

        // writing the sum of data point 5 to file
        val writer = new PrintWriter(new File(s"$outputDir/$i.txt"))
        writer.write(sum.toString() + "\n")

        // writing the number of unique users
        val uniqueUsers = UsersListBuffer.toList.distinct.size
        writer.append(uniqueUsers.toString() + "\n")

        val eventsPerUser = linesList.takeRight(noOfLinesToProcess).groupBy(_ (0))
        val userIdAskeys = eventsPerUser.keys
        for (key <- userIdAskeys) {
          val transposedList = eventsPerUser(key).transpose
          // writing the average value of data point 3 and most recent value of data point 4 per user
          val averagePerUser = transposedList(2).map(_.toDouble).sum / transposedList(1).length.toDouble
          val latestValue = eventsPerUser(key).last(3)
          writer.append(key + "," + averagePerUser + "," + latestValue + "\n")
        }

        writer.close()
        println(s"Successfully written the output to file: $i.txt")
        n = n + noOfLinesToProcess
        i = i + 1
      }
    }
  }
}
