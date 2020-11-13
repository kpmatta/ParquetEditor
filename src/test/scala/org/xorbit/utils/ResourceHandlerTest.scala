package org.xorbit.utils

import org.scalatest.funsuite.AnyFunSuite
import org.xorbit.utils.ResourceHandler._
import scala.util.{Failure, Success, Try}

class ResourceHandlerTest extends AnyFunSuite  {
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