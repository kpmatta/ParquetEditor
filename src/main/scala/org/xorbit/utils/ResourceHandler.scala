package org.xorbit.utils

object ResourceHandler {
  def using[A <: AutoCloseable, T](resource : A)(f: A => T): T = {
    try {
      f(resource)
    }finally {
      resource.close()
    }
  }
}
