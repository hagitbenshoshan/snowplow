/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package common
package enrichments
package registry
package apilookup

import scala.util.control.NonFatal

import scalaz._
import Scalaz._

import org.json4s.JObject

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.headers.{ Authorization, BasicHttpCredentials }


/**
 * Common trait for all possible API sources
 */
sealed trait Api {
  /**
   * Primary API method, taking kv-context derived from event (POJO and contexts),
   * generating request and sending it
   *
   * @param context template context of input keys -> event values
   * @param output output preference
   * @return self-describing JSON ready to be attached to event contexts
   */
  def perform(context: Map[String, String], output: Output): Validation[String, JObject]

  /**
   * Build URL from URI templates (http://acme.com/{{key1}}/{{key2}}
   *
   * @param context key-value context to substitute
   * @return
   */
  // TODO: Need to replace regex-based func with something more robust
  private[enrichments] def buildRequest(context: Map[String, String], template: String): String = {
    context.toList.foldLeft(template)(replace).replaceAll("""\{\{\w+\}\}""", "")
  }

  private def replace(t: String, pair: (String, String)): String = pair match {
    case (key, value) => t.replaceAll(s"\\{\\{$key\\}\\}", value)
  }
}

/**
 * API client able to make HTTP requests
 *
 * @param method HTTP method
 * @param uri URI template
 * @param authentication auth preferences
 */
case class HttpApi(method: String, uri: String, authentication: Option[Authentication]) extends Api {
  def perform(context: Map[String, String], output: Output): Validation[String, JObject] = {
    val request = buildRequest(context, uri)
    val response = Client.getBody(request)
    output.toContext(response)
  }

  /**
   * Inner client to perform HTTP API requests
   */
  private object Client {
    import scala.concurrent.{ Await, Future }
    import scala.concurrent.duration._
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.model._

    val auth: Option[Authorization] = for {
      authParams <- authentication
      credentials <- authParams.`http-basic`
      basic = BasicHttpCredentials(credentials.username, credentials.password)
    } yield Authorization(basic)

    implicit val system = ActorSystem("api-lookup-system")  // TODO: think how to share it with other enrichments
    implicit val context = system.dispatcher
    implicit val materializer = ActorMaterializer()

    private val http = Http()

    def getBody(uri: String): Validation[String, String] = {
      val response = getData(uri)
      val body = response.flatMap(_.entity.toStrict(3 seconds).map(_.data.utf8String))
      try {
        Await.result(body, 3.seconds).success
      } catch {
        case NonFatal(e) => e.toString.failure
      }
    }

    /**
     * Perform HTTP response
     *
     * @param uri generated HTTP query
     * @return raw HTTP response
     */
    private def getData(uri: String): Future[HttpResponse] = {
      http.singleRequest(HttpRequest(uri = uri, headers = auth.toList))
    }
  }
}

/**
 * Helper class to configure authentication for HTTP API
 *
 * @param `http-basic` single possible auth type is http-basic
 */
case class Authentication(`http-basic`: Option[HttpBasic])

/**
 * Container for HTTP Basic auth credentials
 */
case class HttpBasic(username: String, password: String)


/**
 * Hypothetical API client able to run SQL queries
 *
 * @param host PostgreSQL server
 */
case class PostgreSqlApi(host: String) extends Api {
  /**
   * @param context is the same output generated from `Inputs`
   *                (it's a template context, not event context; "context" really confusing here)
   * @param output is object responsible for generating derived context out of API raw response
   * @return self-describing JSON ready to be attached to event contexts
   */
  def perform(context: Map[String, String], output: Output): Validation[String, JObject] = {
    val request = buildRequest(context, "SELECT some_row FROM {{some_table}} WHERE id = {{user_id}}")
    ??? // Client.runSql(request)
  }

  /**
   * Client connected to Posgres
   */
  private object Client {
    // connect(host)
    def runSql(query: String) = ???
  }
}

