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
package enrich.common.enrichments
package registry
package apilookup

import org.json4s.JObject

import org.joda.time.DateTime

import com.twitter.util.SynchronizedLruMap

/**
 * Just LRU cache
 *
 * @param size amount of objects
 * @param ttl time to live
 */
// It could be necessary to include `Output` into cache key as well
// In some cases user may want to get different subobjects from one URI
// Now, with different `Outputs`, but with same `Input` he get same context
case class Cache(size: Int, ttl: Int) {

  type TimestampedContext = (JObject, Int)

  private val cache = new SynchronizedLruMap[Map[String, String], TimestampedContext](size)

  /**
   * Get a value if it's not outdated
   *
   * @param input all input parameters
   * @return context object (with Iglu URI, not just plain JSON)
   */
  def get(input: Map[String, String]): Option[JObject] = {
    cache.get(input) match {
      case Some((value, created)) if ttl == 0 => Some(value)
      case Some((value, created)) => {
        val now = (new DateTime().getMillis / 1000).toInt
        if (now - created < ttl) Some(value)
        else None
      }
      case _ => None
    }
  }

  /**
   * Put a value into cache with current timestamp
   *
   * @param key all inputs Map
   * @param value context object (with Iglu URI, not just plain JSON)
   */
  def put(key: Map[String, String], value: JObject): Unit = {
    val now = (new DateTime().getMillis / 1000).toInt
    cache.put(key, (value, now))
  }
}
