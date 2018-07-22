package example

object Hello extends Greeting with App {
  val seq: Seq[_] = Seq(1, "2", 1.5, 2L)
  seq.foreach(println)
}

trait Greeting {
  lazy val greeting: String = "hello"


}
