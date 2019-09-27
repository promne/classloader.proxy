import org.jgroups.JChannel
import java.io.Serializable
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import kotlin.math.sqrt


class IsPrime(private val number: Long) : Callable<Pair<Long, Boolean>>,Serializable {

    override fun call(): Pair<Long, Boolean> {
        println("Checking if it's a prime number $number")
        return Pair(number, isPrime(number))
    }

    private fun isPrime(i: Long) = (1..sqrt(i.toDouble()).toInt()).asSequence().filter { i%it == 0L }.count() == 1
}

class SleepyCaller(private val time: Long): Callable<Unit>,Serializable {
    override fun call() {
        println("I am sleeping")
        Thread.sleep(time)
    }
}

fun main() {
    val channel = JChannel()
    channel.connect("classloader.proxy")

    val es = ClusterExecutor(channel, 0)

    Thread.sleep(4000) //let it join a cluster

    arrayOf(IsPrime(1398341745571), SleepyCaller(3000), SleepyCaller(2000), SleepyCaller(4000), SleepyCaller(400000), IsPrime(63018038201)).forEach {
        es.submit(it as Callable<Any>, 5, TimeUnit.SECONDS).handle { t, u ->
            println("Got result for $it : $t $u")
        }
    }

    //hang in there
}