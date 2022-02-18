package com.example

import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}

package object spark {

  object Using {

    def apply[R: Releasable, A](res: => R)(f: R => A): Try[A] = Try { resource(res)(f) }

    def resource[R, A](res: R)(body: R => A)(implicit releasable: Releasable[R]): A = {
      if (res == null) throw new NullPointerException("Resource is null")

      var toThrow: Throwable = null
      try {
        body(res)
      } catch {
        case t: Throwable =>
          toThrow = t
          null.asInstanceOf[A]
      } finally {
        if (toThrow eq null) releasable.release(res)
        else {
          try releasable.release(res)
          catch { case other: Throwable => toThrow = preferentiallySuppress(toThrow, other) }
          finally throw toThrow
        }
      }
    }

    private def preferentiallySuppress(primary: Throwable, secondary: Throwable): Throwable = {
      def score(t: Throwable): Int = t match {
        case _: VirtualMachineError                   => 4
        case _: LinkageError                          => 3
        case _: InterruptedException | _: ThreadDeath => 2
        case _: ControlThrowable                      => 0
        case e if !NonFatal(e)                        => 1
        case _                                        => -1
      }
      @inline def suppress(t: Throwable, suppressed: Throwable): Throwable = { t.addSuppressed(suppressed); t }

      if (score(secondary) > score(primary)) suppress(secondary, primary)
      else suppress(primary, secondary)
    }

    trait Releasable[-R] {
      def release(resource: R): Unit
    }

    object Releasable {
      implicit object AutoCloseableIsReleasable extends Releasable[AutoCloseable] {
        def release(resource: AutoCloseable): Unit = resource.close()
      }
    }
  }

}
