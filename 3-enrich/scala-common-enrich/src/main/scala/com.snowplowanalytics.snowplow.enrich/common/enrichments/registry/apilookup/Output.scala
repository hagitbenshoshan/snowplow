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

import org.json4s.{ JValue, JObject }
import org.json4s.JsonDSL._
import org.json4s.jackson.parseJson

/**
 * Base trait for API output format
 * Primary intention of these classes is to perform transformation
 * of API raw output to self-describing JSON instance
 */
sealed trait Output {
  /**
   * Iglu URI of JSON Schema for this instance
   */
  val schema: String      // All outputs need schema

  /**
   * Primary working function transforming raw API response (text) to JSON
   *
   * @param apiResponse response taken from `ApiMethod`
   * @return JSON object
   */
  def parse(apiResponse: String): Validation[String, JValue]

  /**
   * Primary public function
   * Extract and return as self-describing custom context
   */
  def toContext(apiResponse: Validation[String, String]): Validation[String, JObject] =
    apiResponse.flatMap(parse).map(describeJson)

  /**
   * Add `schema` (Iglu URI) to parsed instance
   *
   * @param json JValue parsed from API
   * @return self-describing JSON instance
   */
  protected def describeJson(json: JValue): JObject =
    ("schema" -> schema) ~ ("data" -> json)
}

/**
 * Preference for extracting JSON from API output
 *
 * @param jsonPath JSON Path to required value
 * @param schema schema used to describe instance
 */
case class JsonOutput(jsonPath: String, schema: String) extends Output {

  /**
   * Try to parse string as JSON and extract value by JSON PAth
   *
   * @param json string assumed to be JSON
   * @return JSON object
   */
  def parse(json: String): Validation[String, JValue] =
    try {
      getJsonPath(parseJson(json))
    } catch {
      case NonFatal(e) => e.toString.failure
    }

  /**
   * Extract value by JSON Path
   *
   * @param json JSON instance
   * @return JSON instance
   */
  private def getJsonPath(json: JValue): Validation[String, JValue] =
    (json, jsonPath) match {
      case (o, "*") => o.success
      case (o, _)   => "JSON Path extraction not implemented yet".failure   // TODO: implement
    }
}

