package com.stripe.ctf.instantcodesearch

import java.io._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil.UTF_8
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer

class SearchMasterServer(port: Int, id: Int) extends AbstractSearchServer(port, id) {
  val NumNodes = 3

  def this(port: Int) { this(port, 0) }

  val clients = (1 to NumNodes)
    .map { id => new SearchServerClient(port + id, id)}
    .toArray

  override def isIndexed() = {
    val responsesF = Future.collect(clients.map {client => client.isIndexed()})
    val successF = responsesF.map {responses => responses.forall { response =>

        (response.getStatus() == HttpResponseStatus.OK
          && response.getContent.toString(UTF_8).contains("true"))
      }
    }
    successF.map {success =>
      if (success) {
        successResponse()
      } else {
        errorResponse(HttpResponseStatus.BAD_GATEWAY, "Nodes are not indexed")
      }
    }.rescue {
      case ex: Exception => Future.value(
        errorResponse(HttpResponseStatus.BAD_GATEWAY, "Nodes are not indexed")
      )
    }
  }

  override def healthcheck() = {
    val responsesF = Future.collect(clients.map {client => client.healthcheck()})
    val successF = responsesF.map {responses => responses.forall { response =>
        response.getStatus() == HttpResponseStatus.OK
      }
    }
    successF.map {success =>
      if (success) {
        successResponse()
      } else {
        errorResponse(HttpResponseStatus.BAD_GATEWAY, "All nodes are not up")
      }
    }.rescue {
      case ex: Exception => Future.value(
        errorResponse(HttpResponseStatus.BAD_GATEWAY, "All nodes are not up")
      )
    }
  }

  override def index(path: String) = {
    System.err.println(
      "[master] Requesting " + NumNodes + " nodes to index path: " + path
    )

    val dir = new File(path);
    val subdirs = dir.listFiles
                   .filter(_.isDirectory)
                   .map(_.getName).toList
    val responses = Future.collect(
      subdirs.zipWithIndex.map { case (dir, i) => {
        clients(i % 3).index(path + '/' + dir)
      }
      })

    /*
    System.err.println("Subdirectory size: " + subdirs.size)
    assert(subdirs.size == 3, "Number of subdirs isn't 3")

    val responses = Future.collect(clients.zip(subdirs).map {case (c, s) => c.index(path + '/' + s)})
    //val responses = Future.collect(clients.map {client => client.index(path)})
    */
    responses.map {_ => successResponse()}
  }

  override def query(q: String) = {
    val responses = Future.collect(clients.map {client => client.query(q)})
    responses.map { rs => 
      val l = rs.map { r => r.getContent().toString(UTF_8).drop(1).dropRight(1).trim }
      val merged = l.filter({ s => s.length > 0 }).mkString("[", ",\n", "]")
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val content = "{\"success\": true,\n \"results\": " + merged + "}"
      response.setContent(copiedBuffer(content, UTF_8))
//      System.err.println(response)
      response
    }
  }
}
