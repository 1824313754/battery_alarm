object Test {
  def main(args: Array[String]) {

    val x = Test(6)
    val  Test(a)=x
    print(a)

  }
  def apply(x: Int) = x
  def unapply(z: Int): Option[Int] = if (z%2==0) Some(z/2) else None
}