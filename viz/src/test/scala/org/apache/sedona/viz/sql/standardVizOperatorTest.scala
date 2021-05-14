/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.viz.sql

import org.apache.sedona.viz.core.{ImageGenerator, ImageSerializableWrapper}
import org.apache.sedona.viz.utils.ImageType

class standardVizOperatorTest extends TestBaseScala {

  describe("SedonaViz SQL function Test") {

    it("Generate a single image") {
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 256, 256, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000)) ) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates), 'red')) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var image = spark.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW imagestring AS
          |SELECT ST_EncodeImage(image)
          |FROM images
        """.stripMargin)
      spark.table("imagestring").show(1)
    }

    it("Generate a single image using a fat query") {
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW boundtable AS
          |SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW EXPLODE(ST_Pixelize(ST_Transform(ST_FlipCoordinates(shape), 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(ST_FlipCoordinates(bound), 'epsg:4326','epsg:3857') FROM boundtable))) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_EncodeImage(ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)))) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary
          |FROM pixelaggregates
        """.stripMargin)
      spark.table("images").show(1)
    }

    it("Passed the pipeline on points") {
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 1000, 800, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      val pixelaggregates = spark.table("pixelaggregates")
      pixelaggregates.show(1)
    }

    it("Passed the pipeline on polygons") {
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, rate, shape FROM usdata
          |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates))) AS image
          |FROM pixelaggregates
        """.stripMargin)
      var imageDf = spark.sql(
        """
          |SELECT image
          |FROM images
        """.stripMargin)
      var image = imageDf.take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
      var imageGenerator = new ImageGenerator
      imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/polygons", ImageType.PNG)
    }

    it("Passed ST_TileName") {
      var zoomLevel = 2
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixels AS
          |SELECT pixel, shape FROM pointtable
          |LATERAL VIEW EXPLODE(ST_Pixelize(shape, 1000, 1000, ST_PolygonFromEnvelope(-126.790180,24.863836,-64.630926,50.000))) AS pixel
        """.stripMargin)
      spark.sql(
        """
          |CREATE OR REPLACE TEMP VIEW pixelaggregates AS
          |SELECT pixel, count(*) as weight
          |FROM pixels
          |GROUP BY pixel
        """.stripMargin)
      spark.sql(
        s"""
          |CREATE OR REPLACE TEMP VIEW pixel_weights AS
          |SELECT pixel, weight, ST_TileName(pixel, $zoomLevel) AS pid
          |FROM pixelaggregates
        """.stripMargin)
      spark.sql(
        s"""
          |CREATE OR REPLACE TEMP VIEW images AS
          |SELECT ST_Render(pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)), $zoomLevel) AS image
          |FROM pixel_weights
          |GROUP BY pid
        """.stripMargin).explain()
      spark.table("images").show(1)
    }
  }
}
