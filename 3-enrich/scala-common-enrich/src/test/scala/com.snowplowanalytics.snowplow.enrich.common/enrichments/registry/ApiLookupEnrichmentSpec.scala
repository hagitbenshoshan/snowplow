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
package enrich
package common
package enrichments
package registry

import com.snowplowanalytics.iglu.client.SchemaKey

import org.json4s.jackson.parseJson

import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

import apilookup._

object Configuration {
  object CorrectCase {
    val inputs = List(
      Input("user", pojo = Some(PojoInput("user_id")), json = None),
      Input("user", pojo = None, json = Some(JsonInput("contexts", "com.snowplowanalytics.snowplow/client_session/jsonschema/1-", "$.userId"))),
      Input("client", pojo = Some(PojoInput("app_id")), json = None)
    )
    val api = HttpApi("GET", "http://api.acme.com/users/{{client}}/{{user}}?format=json", Some(Authentication(Some(HttpBasic("xxx", "yyy")))))
    val output = JsonOutput("$.record", "iglu:com.acme/user/jsonschema/1-0-0")
    val cache = Cache(3000, 60)
    val config = ApiLookupEnrichment(inputs, api, output, cache)

    val example =
      """
      |{
      |
      |		"vendor": "com.snowplowanalytics.snowplow.enrichments",
      |		"name": "api_lookup_enrichment_config",
      |		"enabled": true,
      |		"parameters": {
      |			"inputs": [
      |				{
      |					"key": "user",
      |					"pojo": {
      |						"field": "user_id"
      |					}
      |				},
      |				{
      |					"key": "user",
      |					"json": {
      |						"field": "contexts",
      |						"schemaCriterion": "com.snowplowanalytics.snowplow/client_session/jsonschema/1-",
      |						"jsonPath": "$.userId"
      |					}
      |				},
      |				{
      |					"key": "client",
      |					"pojo": {
      |						"field": "app_id"
      |					}
      |				}
      |			],
      |			"api": {
      |				"http": {
      |					"method": "GET",
      |					"uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
      |					"authentication": {
      |						"http-basic": {
      |							"username": "xxx",
      |							"password": "yyy"
      |						}
      |					}
      |				}
      |			},
      |			"output": {
      |				"json": {
      |					"jsonPath": "$.record",
      |					"schema": "iglu:com.acme/user/jsonschema/1-0-0"
      |				}
      |			},
      |			"cache": {
      |				"size": 3000,
      |				"ttl": 60
      |			}
      |		}
      |	}
      |
    """.
        stripMargin
    }

  object EmptyInputCase {
    val example =
      """
        |{
        |
        |		"vendor": "com.snowplowanalytics.snowplow.enrichments",
        |		"name": "api_lookup_enrichment_config",
        |		"enabled": true,
        |		"parameters": {
        |			"inputs": [
        |				{
        |					"key": "user",
        |					"pojo": {
        |						"field": "user_id"
        |					}
        |				},
        |				{
        |					"key": "user"
        |				},
        |				{
        |					"key": "client",
        |					"pojo": {
        |						"field": "app_id"
        |					}
        |				}
        |			],
        |			"api": {
        |				"http": {
        |					"method": "GET",
        |					"uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
        |					"authentication": {
        |						"http-basic": {
        |							"username": "xxx",
        |							"password": "yyy"
        |						}
        |					}
        |				}
        |			},
        |			"output": {
        |				"json": {
        |					"jsonPath": "$.record",
        |					"schema": "iglu:com.acme/user/jsonschema/1-0-0"
        |				}
        |			},
        |			"cache": {
        |				"size": 3000,
        |				"ttl": 60
        |			}
        |		}
        |	}
        |
      """.
        stripMargin
  }

  object BothInputCase {
    val example =
      """
        |{
        |		"vendor": "com.snowplowanalytics.snowplow.enrichments",
        |		"name": "api_lookup_enrichment_config",
        |		"enabled": true,
        |		"parameters": {
        |			"inputs": [
        |				{
        |					"key": "user",
        |					"pojo": {
        |						"field": "user_id"
        |					}
        |				},
        |				{
        |					"key": "client",
        |					"pojo": {
        |						"field": "app_id"
        |					},
        |         "json": {
        |						"field": "contexts",
        |						"schemaCriterion": "com.snowplowanalytics.snowplow/client_session/jsonschema/1-",
        |						"jsonPath": "$.userId"
        |         }
        |				}
        |			],
        |			"api": {
        |				"http": {
        |					"method": "GET",
        |					"uri": "http://api.acme.com/users/{{client}}/{{user}}?format=json",
        |					"authentication": {
        |						"http-basic": {
        |							"username": "xxx",
        |							"password": "yyy"
        |						}
        |					}
        |				}
        |			},
        |			"output": {
        |				"json": {
        |					"jsonPath": "$.record",
        |					"schema": "iglu:com.acme/user/jsonschema/1-0-0"
        |				}
        |			},
        |			"cache": {
        |				"size": 3000,
        |				"ttl": 60
        |			}
        |		}
        |	}
        |
      """.
        stripMargin
  }

  val input1 = Input("user", Some(PojoInput("user_id")), None)
  val input2 = Input("time", Some(PojoInput("true_tstamp")), None)
  val uriTemplate = "http://localhost:8000/{{user}}/foo/{{time}}"
  val enrichment = ApiLookupEnrichment(List(input1, input2), HttpApi("GET", uriTemplate, None), JsonOutput("*", "iglu:wow"), Cache(10, 5))

}

class ApiLookupEnrichmentSpec extends Specification with ValidationMatchers { def is =
    "This is a specification to test the ApiLookupEnrichment" ^
      "Extract correct configuration"                                   ! e1^
      "Skip incorrect input (none of json or pojo) in configuration"    ! e2^
      "Skip incorrect input (both json and pojo) in configuration"      ! e3^
      "Create template context from POJO inputs"                        ! e4^
      "Build request string from template context"                      ! e5^
      "Make an HTTP request"                                            ! e6^
                                                                          end

  def e1 = {
    val configJson = parseJson(Configuration.CorrectCase.example)
    val config = ApiLookupEnrichmentConfig.parse(configJson, SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_lookup_enrichment_config", "jsonschema", "1-0-0"))
    config must beSuccessful(Configuration.CorrectCase.config)
  }

  def e2 = {
    val configJson = parseJson(Configuration.EmptyInputCase.example)
    val config = ApiLookupEnrichmentConfig.parse(configJson, SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_lookup_enrichment_config", "jsonschema", "1-0-0"))
    config must beFailing
  }

  def e3 = {
    val configJson = parseJson(Configuration.BothInputCase.example)
    val config = ApiLookupEnrichmentConfig.parse(configJson, SchemaKey("com.snowplowanalytics.snowplow.enrichments", "api_lookup_enrichment_config", "jsonschema", "1-0-0"))
    config must beFailing
  }

  def e4 = {
    val event = new common.outputs.EnrichedEvent
    event.setUser_id("chuwy")
    event.setTrue_tstamp("20")
    val templateContext = Configuration.enrichment.collectInputs(event, Nil)
    templateContext must beEqualTo(Map("user" -> "chuwy", "time" -> "20"))
  }

  def e5 = {
    val templateContext = Map("user" -> "admin", "time" -> "November 2015")
    val request = Configuration.enrichment.api.buildRequest(templateContext, Configuration.uriTemplate)
    request must beEqualTo("http://localhost:8000/admin/foo/November 2015")
  }

  def e6 = {
    val event = new common.outputs.EnrichedEvent
    event.setUser_id("chuwy")
    event.setTrue_tstamp("20")
    val a = Configuration.enrichment.lookup(event)
    a must beSuccessful
  }

  def e7 = {
    // Http auth
  }
}
