/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._

// Iglu
import iglu.client.{
  SchemaCriterion,
  SchemaKey
}

// This project
import utils.ScalazJson4sUtils
import outputs.EnrichedEvent
import apilookup._


// TODO: replace Options with Validations [x]
// TODO: request pool [x]
// TODO: package [x]
// TODO: auth [x]
// TODO: cache [x]
// TODO: rebase [x]
// TODO: add it to manager [x]
// TODO: JSON Path

/**
 * Lets us create an ApiLookupEnrichmentConfig from a JValue.
 */
object ApiLookupEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema = SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "api_lookup_enrichment_config", "jsonschema", 1, 0, 0)

  /**
   * Creates an ApiLookupEnrichment instance from a JValue.
   *
   * @param config The enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment
   * @return a configured ApiLookupEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[ApiLookupEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        inputs      <- ScalazJson4sUtils.extract[List[Input]](config, "parameters", "inputs")
        httpApi     <- ScalazJson4sUtils.extract[Option[HttpApi]](config, "parameters", "api", "http")
        jsonOutput  <- ScalazJson4sUtils.extract[Option[JsonOutput]](config, "parameters", "output", "json")
        cache       <- ScalazJson4sUtils.extract[Cache](config, "parameters", "cache")
        api         = List(httpApi).flatten.head      // put all further APIs here to pick single one
        output      = List(jsonOutput).flatten.head   // ...and outputs
      } yield ApiLookupEnrichment(inputs, api, output, cache)).toValidationNel
    })
  }
}

case class ApiLookupEnrichment(inputs: List[Input], api: Api, output: Output, cache: Cache) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  /**
   * Primary function of the enrichment
   *
   * @param event currently enriching event
   * @param contexts derived contexts
   * @return validated context object or error string in case of failure
   */
  // TODO: derived contexts
  def lookup(event: EnrichedEvent, contexts: List[JObject] = Nil): Validation[String, JObject] = {
    val inputs = collectInputs(event, contexts)
    cache.get(inputs) match {
      case Some(value) => value.success
      case None => api.perform(inputs, output)
    }
  }

  /**
   * Get template context out of all inputs
   *
   * @param event current enriching event
   * @return template context
   */
  def collectInputs(event: EnrichedEvent, contexts: List[JObject]): Map[String, String] =
    inputs.map(_.getFromEvent(event)).toMap

}

