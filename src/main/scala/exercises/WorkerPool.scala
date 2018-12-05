package exercises

import cats.data.NonEmptyList
import cats.effect._
import cats.effect.concurrent.{MVar, Ref}
import cats.effect.implicits._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.concurrent.duration._
import scala.util.Random


object WorkerPool extends IOApp {

  private def printOut[F[_]](s: String)(implicit C: Concurrent[F]): F[Unit] = C.delay(println(s))

  final case class Worker[F[_], A, B](run: A => F[B], id: Int)

  def mkWorker[F[_]](id: Int)(implicit T: Timer[F], C: Concurrent[F]): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        C.delay(50 + Random.nextInt(450)).map(_.millis).flatMap(T.sleep)

      def report: F[Unit] = for {
        i <- counter.get
        _ <- printOut(s"Total processed by $id: $i")
      } yield ()

      val work: Int => F[Int] = x => simulateWork >> counter.update(_ + 1) >> report >> C.pure(x + 1)

      Worker(work, id)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]

    def removeAllWorkers: F[Unit]

    def addWorker(worker: Worker[F, A, B]): F[Unit]
  }

  object WorkerPool {

    def of[F[_] : Concurrent : Timer, A, B](fs: NonEmptyList[Worker[F, A, B]]): F[WorkerPool[F, A, B]] = for {
      workersQueue <- MVar.empty[F, Worker[F, A, B]]
      _ <- fs.map(workersQueue.put).map(_.start.void).sequence
      active <- Ref[F].of(fs.map(_.id).toList)
      workersPool = PoolImpl(workersQueue, active)
    } yield workersPool


    private def getBack[F[_] : Concurrent, A, B](workersQueue: MVar[F, Worker[F, A, B]],
                                                 worker: Worker[F, A, B],
                                                 activeIdsRef: Ref[F, List[Int]]): F[Unit] = for {
      isActive <- activeIdsRef.get.map(_.contains(worker.id))
      result <- if (isActive) workersQueue.put(worker).start.void else Concurrent[F].unit
    } yield result


    private case class PoolImpl[F[_] : Concurrent, A, B](workersQueue: MVar[F, Worker[F, A, B]],
                                                         activeIds: Ref[F, List[Int]]) extends WorkerPool[F, A, B] {
      override def exec(a: A): F[B] = for {
        w <- workersQueue.take
        result <- w.run(a).guarantee(getBack(workersQueue, w, activeIds))
      } yield result

      override def removeAllWorkers: F[Unit] =
        activeIds.set(List.empty[Int]) >> printOut("All workers have been removed.")

      def addWorker(worker: Worker[F, A, B]): F[Unit] =
        activeIds.update(_ :+ worker.id) >> workersQueue.put(worker) >> printOut("New worker added")
    }
  }

  def getNewWorker(ids: Ref[IO, Int]): IO[Worker[IO, Int, Int]] = for {
    worker <- ids.get.flatMap(mkWorker[IO](_))
    _ <- ids.update(_ + 1)
  } yield worker

  def getNWorkers(n: Int, ids: Ref[IO, Int]): IO[List[Worker[IO, Int, Int]]] = getNewWorker(ids).replicateA(n)

  def testPool(ids: Ref[IO, Int]): IO[WorkerPool[IO, Int, Int]] = for {
    workers <- getNWorkers(5, ids)
    pool <- WorkerPool.of[IO, Int, Int](NonEmptyList.fromListUnsafe(workers))
  } yield pool

  def run(args: List[String]) =
    for {
      ids <- Ref[IO].of(0)
      pool <- testPool(ids)
      work <- pool.exec(42).replicateA(20).start
      _    <- pool.removeAllWorkers
      w <- getNewWorker(ids)
      _ <- pool.addWorker(w)
      v <- getNewWorker(ids)
      _ <- pool.addWorker(v)
      _ <- work.join
    } yield ExitCode.Success

}
