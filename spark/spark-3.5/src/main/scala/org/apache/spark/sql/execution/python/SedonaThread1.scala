package org.apache.spark.sql.execution.python

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.{BarrierTaskContext, SparkEnv, SparkException, SparkFiles, TaskContext}
import org.apache.spark.api.python.{BarrierTaskContextMessageProtocol, BasePythonRunner, EncryptedPythonBroadcastServer, PythonRDD, SedonaBasePythonRunner, SpecialLengths}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream, File}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets.UTF_8
import scala.util.control.NonFatal
//
//trait WorkerThreadFactory[IN] {
//  self: BasePythonRunner [IN, _] =>
//
//  // Define common functionality for worker threads here
//
//  def createWorkerThread(
//                          env: SparkEnv,
//                          worker: Socket,
//                          inputIterator: Iterator[IN],
//                          partitionIndex: Int,
//                          context: TaskContext,
//                          schema: StructType,
//                        ): WriterThread = {
//    new SedonaThread1 (env, worker, inputIterator, partitionIndex, context, schema)
//  }
//
//  class SedonaThread1(
//                       env: SparkEnv,
//                       worker: Socket,
//                       inputIterator: Iterator[IN],
//                       partitionIndex: Int,
//                       context: TaskContext,
//                       schema: StructType,
//                     ) extends WriterThread(env, worker, inputIterator, partitionIndex, context) {
//
//
//    override def run(): Unit = Utils.logUncaughtExceptions {
//      try {
//        val toReadCRS = inputIterator.buffered.headOption.flatMap(
//          el => el.asInstanceOf[Iterator[IN]].buffered.headOption
//        )
//
//        val row = toReadCRS match {
//          case Some(value) => value match {
//            case row: GenericInternalRow =>
//              Some(row)
//          }
//          case None => None
//        }
//
//        val geometryFields = schema.zipWithIndex.filter {
//          case (field, index) => field.dataType == GeometryUDT
//        }.map {
//          case (field, index) =>
//            if (row.isEmpty || row.get.values(index) == null) (index, 0) else {
//              val geom = row.get.get(index, GeometryUDT).asInstanceOf[Array[Byte]]
//              val preambleByte = geom(0) & 0xFF
//              val hasSrid = (preambleByte & 0x01) != 0
//
//              var srid = 0
//              if (hasSrid) {
//                val srid2 = (geom(1) & 0xFF) << 16
//                val srid1 = (geom(2) & 0xFF) << 8
//                val srid0 = geom(3) & 0xFF
//                srid = srid2 | srid1 | srid0
//              }
//              (index, srid)
//            }
//        }
//
//        TaskContext.setTaskContext(context)
//        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
//        val dataOut = new DataOutputStream(stream)
//
//        // Partition index
//        dataOut.writeInt(partitionIndex)
//        // Python version of driver
//        PythonRDD.writeUTF(pythonVer, dataOut)
//        // Init a ServerSocket to accept method calls from Python side.
//        val isBarrier = context.isInstanceOf[BarrierTaskContext]
//        if (isBarrier) {
//          serverSocket = Some(new ServerSocket(/* port */ 0,
//            /* backlog */ 1,
//            InetAddress.getByName("localhost")))
//          // A call to accept() for ServerSocket shall block infinitely.
//          serverSocket.foreach(_.setSoTimeout(0))
//          new Thread("accept-connections") {
//            setDaemon(true)
//
//            override def run(): Unit = {
//              while (!serverSocket.get.isClosed()) {
//                var sock: Socket = null
//                try {
//                  sock = serverSocket.get.accept()
//                  // Wait for function call from python side.
//                  sock.setSoTimeout(10000)
//                  authHelper.authClient(sock)
//                  val input = new DataInputStream(sock.getInputStream())
//                  val requestMethod = input.readInt()
//                  // The BarrierTaskContext function may wait infinitely, socket shall not timeout
//                  // before the function finishes.
//                  sock.setSoTimeout(10000)
//                  requestMethod match {
//                    case BarrierTaskContextMessageProtocol.BARRIER_FUNCTION =>
//                      barrierAndServe(requestMethod, sock)
//                    case BarrierTaskContextMessageProtocol.ALL_GATHER_FUNCTION =>
//                      val length = input.readInt()
//                      val message = new Array[Byte](length)
//                      input.readFully(message)
//                      barrierAndServe(requestMethod, sock, new String(message, UTF_8))
//                    case _ =>
//                      val out = new DataOutputStream(new BufferedOutputStream(
//                        sock.getOutputStream))
//                      writeUTF(BarrierTaskContextMessageProtocol.ERROR_UNRECOGNIZED_FUNCTION, out)
//                  }
//                } catch {
//                  case e: SocketException if e.getMessage.contains("Socket closed") =>
//                  // It is possible that the ServerSocket is not closed, but the native socket
//                  // has already been closed, we shall catch and silently ignore this case.
//                } finally {
//                  if (sock != null) {
//                    sock.close()
//                  }
//                }
//              }
//            }
//          }.start()
//        }
//        val secret = if (isBarrier) {
//          authHelper.secret
//        } else {
//          ""
//        }
//        // Close ServerSocket on task completion.
//        serverSocket.foreach { server =>
//          context.addTaskCompletionListener[Unit](_ => server.close())
//        }
//        val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
//        if (boundPort == -1) {
//          val message = "ServerSocket failed to bind to Java side."
//          logError(message)
//          throw new SparkException(message)
//        } else if (isBarrier) {
//          logDebug(s"Started ServerSocket on port $boundPort.")
//        }
//        // Write out the TaskContextInfo
//        dataOut.writeBoolean(isBarrier)
//        dataOut.writeInt(boundPort)
//        val secretBytes = secret.getBytes(UTF_8)
//        dataOut.writeInt(secretBytes.length)
//        dataOut.write(secretBytes, 0, secretBytes.length)
//        dataOut.writeInt(context.stageId())
//        dataOut.writeInt(context.partitionId())
//        dataOut.writeInt(context.attemptNumber())
//        dataOut.writeLong(context.taskAttemptId())
//        dataOut.writeInt(context.cpus())
//        val resources = context.resources()
//        dataOut.writeInt(resources.size)
//        resources.foreach { case (k, v) =>
//          PythonRDD.writeUTF(k, dataOut)
//          PythonRDD.writeUTF(v.name, dataOut)
//          dataOut.writeInt(v.addresses.size)
//          v.addresses.foreach { case addr =>
//            PythonRDD.writeUTF(addr, dataOut)
//          }
//        }
//        val localProps = context.getLocalProperties.asScala
//        dataOut.writeInt(localProps.size)
//        localProps.foreach { case (k, v) =>
//          PythonRDD.writeUTF(k, dataOut)
//          PythonRDD.writeUTF(v, dataOut)
//        }
//
//        // sparkFilesDir
//        val root = jobArtifactUUID.map { uuid =>
//          new File(SparkFiles.getRootDirectory(), uuid).getAbsolutePath
//        }.getOrElse(SparkFiles.getRootDirectory())
//        PythonRDD.writeUTF(root, dataOut)
//        // Python includes (*.zip and *.egg files)
//        dataOut.writeInt(pythonIncludes.size)
//        for (include <- pythonIncludes) {
//          PythonRDD.writeUTF(include, dataOut)
//        }
//        // Broadcast variables
//        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
//        val newBids = broadcastVars.map(_.id).toSet
//        // number of different broadcasts
//        val toRemove = oldBids.diff(newBids)
//        val addedBids = newBids.diff(oldBids)
//        val cnt = toRemove.size + addedBids.size
//        val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
//        dataOut.writeBoolean(needsDecryptionServer)
//        dataOut.writeInt(cnt)
//
//        def sendBidsToRemove(): Unit = {
//          for (bid <- toRemove) {
//            // remove the broadcast from worker
//            dataOut.writeLong(-bid - 1) // bid >= 0
//            oldBids.remove(bid)
//          }
//        }
//
//        if (needsDecryptionServer) {
//          // if there is encryption, we setup a server which reads the encrypted files, and sends
//          // the decrypted data to python
//          val idsAndFiles = broadcastVars.flatMap { broadcast =>
//            if (!oldBids.contains(broadcast.id)) {
//              oldBids.add(broadcast.id)
//              Some((broadcast.id, broadcast.value.path))
//            } else {
//              None
//            }
//          }
//          val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
//          dataOut.writeInt(server.port)
//          logTrace(s"broadcast decryption server setup on ${server.port}")
//          PythonRDD.writeUTF(server.secret, dataOut)
//          sendBidsToRemove()
//          idsAndFiles.foreach { case (id, _) =>
//            // send new broadcast
//            dataOut.writeLong(id)
//          }
//          dataOut.flush()
//          logTrace("waiting for python to read decrypted broadcast data from server")
//          server.waitTillBroadcastDataSent()
//          logTrace("done sending decrypted data to python")
//        } else {
//          sendBidsToRemove()
//          for (broadcast <- broadcastVars) {
//            if (!oldBids.contains(broadcast.id)) {
//              // send new broadcast
//              dataOut.writeLong(broadcast.id)
//              PythonRDD.writeUTF(broadcast.value.path, dataOut)
//              oldBids.add(broadcast.id)
//            }
//          }
//        }
//        dataOut.flush()
//
//        dataOut.writeInt(evalType)
//        writeCommand(dataOut)
//
//        // write number of geometry fields
//        dataOut.writeInt(geometryFields.length)
//        // write geometry field indices and their SRIDs
//        geometryFields.foreach { case (index, srid) =>
//          dataOut.writeInt(index)
//          dataOut.writeInt(srid)
//        }
//
//        writeIteratorToStream(dataOut)
//
//        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
//        dataOut.flush()
//      } catch {
//        case t: Throwable if (NonFatal(t) || t.isInstanceOf[Exception]) =>
//          if (context.isCompleted || context.isInterrupted) {
//            logDebug("Exception/NonFatal Error thrown after task completion (likely due to " +
//              "cleanup)", t)
//            if (!worker.isClosed) {
//              Utils.tryLog(worker.shutdownOutput())
//            }
//          } else {
//            // We must avoid throwing exceptions/NonFatals here, because the thread uncaught
//            // exception handler will kill the whole executor (see
//            // org.apache.spark.executor.Executor).
//            _exception = t
//            if (!worker.isClosed) {
//              Utils.tryLog(worker.shutdownOutput())
//            }
//          }
//      }
//    }
//
//    override protected def writeCommand(dataOut: DataOutputStream): Unit = ???
//
//    override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = ???
//  }
//}
