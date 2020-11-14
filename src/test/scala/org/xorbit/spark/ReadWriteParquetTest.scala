package org.xorbit.spark

import java.io.FileNotFoundException

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import org.xorbit.spark.ReadWriteParquet._

@RunWith(classOf[JUnitRunner])
class ReadWriteParquetTest extends FunSuite with Matchers{

  test("testReadSchema") {
     the [FileNotFoundException] thrownBy readSchema("")
  }
}
