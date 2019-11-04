package freskog.concurrency.partition

import freskog.concurrency.partition.Partition.publish
import zio.stm.TQueue
import org.scalatest.{ Assertion, FlatSpec }
import zio.{ DefaultRuntime, UIO, ZIO }
import zio.clock.Clock
import zio.console.Console
import zio.stm.{ STM }

class PublishTests extends FlatSpec {
  val realRts: DefaultRuntime =
    new DefaultRuntime {}

  def unwrap[R, E, A](zio: ZIO[Clock with Console, E, A]): A =
    realRts.unsafeRun(zio)

  // val testRts: UIO[TestRuntime] =
  //   (clockData <*> consoleData <*> schedulerData).map {
  //     case clockR <*> consoleR <*> schedR => TestRuntime(clockR, consoleR, schedR)
  //   }

  def run(z: ZIO[Clock with Console, Throwable, Assertion]): Assertion =
    // val rts = unwrap(testRts)
    // rts.unsafeRunSync(z).getOrElse(c => throw c.squash)
    realRts.unsafeRun(z)

  def runSTM(z: STM[Throwable, Assertion]): Assertion =
    run(z.commit)

  behavior of "a publisher"

  it should "return true when publishing to an empty TQueue" in {
    runSTM {
      (TQueue.make[Int](1) >>= (publish(_, 1))) map (published => assert(published))
    }
  }

  it should "return false when publishing to a full TQueue" in {
    runSTM(
      (TQueue.make[Int](0) >>= (publish(_, 1))) map (published => assert(!published))
    )
  }
}
