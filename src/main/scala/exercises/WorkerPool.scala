package exercises

import cats.effect._
import cats.effect.concurrent.{MVar, Ref}
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.concurrent.duration._
import scala.util.Random


object WorkerPool extends IOApp {

  type Worker[F[_], A, B] = A => F[B]

  def mkWorker[F[_]](id: Int)(implicit T: Timer[F], C: Concurrent[F]): F[Worker[F, Int, Int]] =
    Ref[F].of(0).map { counter =>
      def simulateWork: F[Unit] =
        C.delay(50 + Random.nextInt(450)).map(_.millis).flatMap(T.sleep)

      def report: F[Unit] = for {
        i <- counter.get
        _ <- C.delay(println(s"Total processed by $id: $i"))
      } yield ()

      x => simulateWork >> counter.update(_ + 1) >> report >> C.pure(x + 1)
    }

  trait WorkerPool[F[_], A, B] {
    def exec(a: A): F[B]

    def removeAllWorkers: F[Unit]

    def addWorker(worker: Worker[F, A, B]): F[Unit]
  }

  object WorkerPool {

    def of[F[_] : Concurrent : Timer, A, B](fs: List[Worker[F, A, B]]): F[WorkerPool[F, A, B]] = for {
      workers <- MVar.of[F, List[Worker[F, A, B]]](fs)
      workersPool = PoolImpl(workers)
    } yield workersPool


    private def addFreeWorker[F[_] : Concurrent, A, B](workers: MVar[F, List[Worker[F, A, B]]], worker: Worker[F, A, B]): F[Unit] =
      workers.take.flatMap { list =>
        Concurrent[F].start(workers.put(list :+ worker)) >> Concurrent[F].unit
      }

    private case class PoolImpl[F[_] : Concurrent : Timer, A, B](workers: MVar[F, List[Worker[F, A, B]]]) extends WorkerPool[F, A, B] {
      override def exec(a: A): F[B] = workers.take.flatMap {
        case x :: xs =>
          workers.put(xs) >> Concurrent[F].guarantee(x(a))(addFreeWorker(workers, x))
        case Nil =>
          Concurrent[F].delay(println("No workers. Let's wait for available.")) >>
            workers.put(List.empty) >>
            Timer[F].sleep(500 milliseconds) >>
            Concurrent[F].defer(exec(a))
      }

      override def removeAllWorkers: F[Unit] =
        workers.take >> workers.put(List.empty) >> Concurrent[F].delay(println("All workers have been removed."))

      def addWorker(worker: Worker[F, A, B]): F[Unit] =
        addFreeWorker(workers, worker) >> Concurrent[F].delay(println("New worker added"))
    }

  }

  def getNewWorker(ids: Ref[IO, Int]): IO[Worker[IO, Int, Int]] = for {
    worker <- ids.get.flatMap(mkWorker[IO](_)).guarantee(ids.update(_ + 1))
  } yield worker

  def getNWorkers(n: Int, ids: Ref[IO, Int]): IO[List[Worker[IO, Int, Int]]] = getNewWorker(ids).replicateA(n)

  // Sample test pool to play with in IOApp
  def testPool(ids: Ref[IO, Int]): IO[WorkerPool[IO, Int, Int]] = for {
    workers <- getNWorkers(10, ids)
    pool <- WorkerPool.of[IO, Int, Int](workers)
  } yield pool

  def run(args: List[String]) =
    for {
      ids <- Ref[IO].of(0)
      pool <- testPool(ids)
      _ <- pool.exec(42).replicateA(20)
      _ <- pool.removeAllWorkers
      work <- pool.exec(42).replicateA(20).start
      newWorker1 <- getNewWorker(ids)
      _ <- pool.addWorker(newWorker1)
      newWorker2 <- getNewWorker(ids)
      _ <- pool.addWorker(newWorker2)
      _ <- work.join
    } yield ExitCode.Success

}
