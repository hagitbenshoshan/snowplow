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

import org.json4s.MappingException

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
  def getFromEvent(event: EnrichedEvent): (String, String) = {
    (pojo, json) match {
      case (Some(p), _) => {
        val method = event.getClass.getMethod(p.field)  // TODO: it will transform null to "null", need to skip
        val value = method.invoke(event).asInstanceOf[String]
        (key, value)
      }
      case (_, Some(j)) => throw new NotImplementedError()
    }
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
