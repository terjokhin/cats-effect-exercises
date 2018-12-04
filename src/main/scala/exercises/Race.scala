package exercises

import cats.Reducible
import cats.data._
import cats.effect._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import scala.concurrent.duration._
import scala.util.Random

object Race extends IOApp {

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
      case ExitCase.Error(_) => IO { println(s"$name errored") }
    }
  }

  // Use this class for reporting all failures.
  case class CompositeException(ex: NonEmptyList[Throwable]) extends Exception("All race candidates have failed") {
    override def getMessage: String = {
      ex.map(_.getMessage).toList.mkString(":")
    }
  }

  // And implement this function:
  def raceToSuccess[F[_], R[_], A](tasks: R[F[A]])
                                  (implicit C: Concurrent[F], R: Reducible[R]): F[A] =
    R.reduce(tasks) { case (l: F[A], r: F[A]) =>

      C.racePair(C.attempt[A](l), C.attempt[A](r)).flatMap {

        case Left((Right(w), l)) => l.cancel.map(_ => w)

        case Left((Left(ex), f)) => f.join.flatMap {
          case Left(ex2) => C.raiseError(CompositeException(NonEmptyList.of(ex, ex2)))
          case Right(r) => C.pure(r)
        }

        case Right((f, Left(ex))) => f.join.flatMap {
          case Left(ex2) => C.raiseError[A](CompositeException(NonEmptyList.of(ex, ex2)))
          case Right(r) => C.pure(r)
        }

        case Right((l, Right(w))) => l.cancel.map(_ => w)
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
