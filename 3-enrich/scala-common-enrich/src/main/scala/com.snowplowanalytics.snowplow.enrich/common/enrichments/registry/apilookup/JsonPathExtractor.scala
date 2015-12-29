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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apilookup

import scalaz._
import Scalaz._

import org.json4s._
import org.json4s.jackson.JsonMethods.mapper

import io.gatling.jsonpath.JsonPath


// TODO: move somewhere more appropriate

/**
 * Wrapper for `io.gatling.jsonpath` for `json4s` and `scalaz`
 */
object JsonPathExtractor {

  private val json4sMapper = mapper

  /**
   * Query some JSON by `jsonPath`
   * It always return List, even for single match
   */
  def query(jsonPath: String, json: JValue): Validation[String, List[JValue]] = {
    val pojo = json4sMapper.convertValue(json, classOf[Object])
    JsonPath.query(jsonPath, pojo) match {
      case Right(iterator) => iterator.map(anyToJValue).toList.success
      case Left(error)     => error.reason.fail
    }
  }

  /**
   * Wrap list of values into JSON array if several values present
   * Use in conjunction with `query`
   *
   * @param values list of JSON values
   * @return array if there's >1 values in list
   */
  def wrapArray(values: List[JValue]): JValue = values match {
    case Nil => JNothing
    case one :: Nil => one
    case many => JArray(many)
  }

  /**
   * Convert POJO to JValue with `jackson` mapper
   *
   * @param any raw JVM type representing JSON
   * @return JValue
   */
  private def anyToJValue(any: Any): JValue =
    json4sMapper.convertValue(any, classOf[JValue])

}
