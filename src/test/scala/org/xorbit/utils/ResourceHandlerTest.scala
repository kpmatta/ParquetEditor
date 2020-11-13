package org.xorbit.utils

import org.xorbit.utils.ResourceHandler._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ResourceHandlerTest extends FunSuite  {
  test("Test Using") {
    val ret = Try {
      val lines = using(scala.io.Source.fromFile("")) {
        f => f.getLines().toList
      }
      println(lines)
    }
    assert(ret.isFailure)
  }
}