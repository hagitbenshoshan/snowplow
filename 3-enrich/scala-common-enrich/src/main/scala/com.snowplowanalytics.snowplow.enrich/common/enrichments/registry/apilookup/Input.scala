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
package com.snowplowanalytics.snowplow
package enrich.common
package enrichments
package registry
package apilookup

import scalaz._
import Scalaz._

import scala.util.control.NonFatal

import org.json4s._
import org.json4s.jackson.compactJson

import outputs.EnrichedEvent


/**
 * Container for key with one (and only one) of possible input sources
 *
 * @param key extracted key
 * @param pojo optional pojo source to take stright from `EnrichedEvent`
 * @param json optional JSON source to take from event context
 */
case class Input(key: String, pojo: Option[PojoInput], json: Option[JsonInput]) {
  // Constructor validation for mapping JSON to `Input` instance
  (pojo, json) match {
    case (None, None)       => { throw new MappingException("Input must represent either json OR pojo, none present") }
    case (Some(_), Some(_)) => { throw new MappingException("Input must represent either json OR pojo, both present") }
    case _ =>
  }

  /**
   * Get key-value pair input for specific `event`
   *
   * @param event currently enriching event
   * @return tuple of input key and event value
   */
  def getFromEvent(event: EnrichedEvent): Option[(String, String)] = pojo match {
    case Some(pojoInput) => {
      val value = try {
        val method = event.getClass.getMethod(pojoInput.field)
        Option(method.invoke(event).asInstanceOf[String]) // TODO: check on Integer, Float
      } catch {
        case NonFatal(_) => None
      }
      value.map(v => (key, v))
    }
    case None => None
  }

  /**
   * Get value out of list of JSON contexts.
   * If more than one context match schemaCriterion, first will be picked
   *
   * @param contexts
   * @return
   */
  // TODO: it could make sense to return Failure for incorrect JSON Path and absence of JSON Input
  def getFromContexts(contexts: List[JObject]): Option[(String, String)] = json match {
    case Some(jsonInput) => {
      val matchedContext = getBySchemaCriterion(contexts, jsonInput.schemaCriterion).headOption  // short-circuit, pick first
      matchedContext.flatMap { ctx =>
        getByJsonPath(jsonInput.jsonPath, ctx).map(v => (key, v))
      }
    }
    case None => None
  }

  /**
   * Get data out of all JSON contexts matching `schemaCriterion`
   *
   * @param contexts list of self-describing JSON contexts attached to event
   * @param schemaCriterion part of URI
   * @return list of contexts matched `schemaCriterion`
   */
  def getBySchemaCriterion(contexts: List[JObject], schemaCriterion: String): List[JValue] = {
    val matched = contexts.filter { context =>
      context.obj.exists {
        case ("schema", JString(schema)) => {
          val schemaWithoutProtocol = schema.split(':').drop(1).mkString(":")   // TODO: do we need to drop 'iglu:'?
          schemaWithoutProtocol.startsWith(schemaCriterion)
        }
      }
    }
    matched.map { _ \ "data" }
  }

  /**
   * Get value by JSON Path and convert it to String
   * Absence of value is failure
   *
   * @param jsonPath JSON Path
   * @param context event context as JSON object
   * @return validated value
   */
  def getByJsonPath(jsonPath: String, context: JValue): Option[String] = {
    val result = JsonPathExtractor.query(jsonPath, context).toOption
    result.map(JsonPathExtractor.wrapArray).flatMap(stringifyJson)
  }

  /**
   * Helper function to stringify JValue to URL-friendly format
   * JValue should be converted to string for further use in URL template with following rules:
   * 1. JString -> as is
   * 2. JInt/JDouble/JBool/null -> stringify
   * 3. JArray -> concatenate with comma ([1,true,"foo"] -> "1,true,foo"). Nested will be flattened
   * 4. JObject -> use as is???
   *
   * @param json arbitrary JSON value
   * @return
   */
  def stringifyJson(json: JValue): Option[String] = json match {
    case JString(s) => s.some
    case JInt(i) => i.toString.some
    case JDouble(d) => d.toString.some
    case JDecimal(d) => d.toString.some
    case JNull => "null".some
    case JArray(array) => array.map(stringifyJson).mkString(",").some
    case obj: JObject => compactJson(obj).some
    case JNothing => None
  }
}

/**
 * Describes how to take key from POJO source
 *
 * @param field `EnrichedEvent` object field
 */
case class PojoInput(field: String)

/**
 *
 * @param field where to get this json, one of unstruct_event, contexts or derived_contexts
 * @param schemaCriterion self-describing JSON you are looking for in the given JSON field.
 *                        You can specify only the SchemaVer MODEL (e.g. 1-), MODEL plus REVISION (e.g. 1-1-) etc
 * @param jsonPath JSON Path statement to navigate to the field inside the JSON that you want to use as the input
 */
case class JsonInput(field: String, schemaCriterion: String, jsonPath: String)
