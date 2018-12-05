package exercises

import cats.Reducible
import cats.data._
import cats.effect._
import cats.syntax.applicative._
import cats.syntax.functor._

import scala.concurrent.duration._
import scala.util.Random

object Race extends IOApp {

  implicit class AttemptOps[F[_] : Concurrent, A](fa: F[A]) {

    def attempt: F[Either[Throwable, A]] = Concurrent[F].attempt[A](fa)
  }

  implicit class FiberOps[F[_]: Concurrent, A](fiber: Fiber[F, A]) {

    def cancelAndReturn[B](v: B): F[B] = fiber.cancel.map(_ => v)
  }



  implicit class CompositeExOps(cEx: CompositeException) {

    def withEx(ex: Throwable): CompositeException = cEx.withEx(ex)
  }

  case class Data(source: String, body: String)

  def provider(name: String)(implicit timer: Timer[IO]): IO[Data] = {
    val proc = for {
      dur <- IO { Random.nextInt(500) }
      _ <- IO.sleep { (100 + dur).millis }
      _ <- IO { if (Random.nextBoolean()) throw new Exception(s"Error in $name") }
      txt <- IO { Random.alphanumeric.take(16).mkString }
    } yield Data(name, txt)

    proc.guaranteeCase {
      case ExitCase.Completed => IO { println(s"$name request finished") }
      case ExitCase.Canceled => IO { println(s"$name request canceled") }
      case ExitCase.Error(_) => IO { println(s"$name errored")}
    }
  }

  // Use this class for reporting all failures.
  case class CompositeException(exs: NonEmptyList[Throwable]) extends Exception("All race candidates have failed") {
    override def getMessage: String = exs.toList.mkString(" : ")

    def withEx(ex: Throwable): CompositeException = CompositeException( ex :: exs )
  }

  type AttemptResult[A] = Either[Throwable, A]

  private def resultOrCompositeError[F[_] : Concurrent, A](f: Fiber[F, AttemptResult[A]], prevEx: Throwable)
                                                          (implicit C: Concurrent[F]): F[A] = {
    C.flatMap(f.join) {
      case Left(nextEx) => C.raiseError[A](prevEx match {
        case ex: CompositeException => ex.withEx(nextEx)
        case _ => CompositeException(NonEmptyList.of(prevEx, nextEx))
      })
      case Right(r) => C.pure(r)
    }
  }

  def raceToSuccess[F[_], R[_], A](tasks: R[F[A]])
                                  (implicit C: Concurrent[F], R: Reducible[R]): F[A] =
    R.reduce(tasks) { case (l, r) =>
      C.racePair(l.attempt, r.attempt).flatMap {
        case Right((l, Right(w))) =>
          l.cancelAndReturn(w)
        case Left((Right(w), l)) =>
          l.cancelAndReturn(w)
        case Left((Left(ex), f)) =>
          resultOrCompositeError(f, ex)
        case Right((f, Left(ex))) =>
          resultOrCompositeError(f, ex)
      }
    }

  // In your IOApp, you can use the following sample method list

  val methods: NonEmptyList[IO[Data]] = NonEmptyList.of(
    "memcached",
    "redis",
    "postgres",
    "mongodb",
    "hdd",
    "aws"
  )
    .map(provider)

  def run(args: List[String]): IO[ExitCode] = {
    def oneRace = raceToSuccess(methods)
      .flatMap(a => IO(println(s"Final result is $a")))
      .handleErrorWith(err => IO(err.printStackTrace()))

    oneRace.replicateA(5).as(ExitCode.Success)
  }

}
