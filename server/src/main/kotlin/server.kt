
import mu.KotlinLogging
import org.jgroups.*
import java.io.Serializable
import java.util.*
import java.util.concurrent.*
import java.util.function.Supplier


class ClusterExecutor(private val channel: JChannel) {

    private val log = KotlinLogging.logger {}

    // data classes
    data class TaskId(val x: String = UUID.randomUUID().toString()): Serializable
    data class Task<T>(val id: TaskId, val callable: Callable<T>): Serializable

    // locally accepted tasks
    private val submittedTasks = ConcurrentHashMap<Task<Any>, CompletableFuture<Any>>()
    private val assignedTasks = ConcurrentHashMap<TaskId, Address>()

    // cluster status
    private val currentMembersWorkload = ConcurrentHashMap<Address, Long>()
    get() {
        field.computeIfAbsent(channel.address) { getLocalQueueSize() }
        return field
    }

    // messages
    data class ScheduleTaskMessage(val task: Task<Any>): Serializable
    data class TaskResultMessage(val id: TaskId, val result: Any): Serializable
    data class TaskResultExceptionMessage(val id: TaskId, val throwable: Throwable): Serializable
    data class ScheduledQueueSize(val size: Long): Serializable

    // locally executed tasks
    private val localExecutor = Executors.newCachedThreadPool() as ThreadPoolExecutor

    // classloader proxy
    // classloader proxy messages
    data class RequestClassDefinitionMessageData(val canonicalName: String) : Serializable
    data class ClassDefinitionMessageData(val canonicalName: String, val bytes: ByteArray) : Serializable
    final class ClearAllClassDefinitions() : Serializable

    // class loader
    class ProxyClassLoader(private val clusterExecutor: ClusterExecutor, private val address: Address, parent: ClassLoader, private val extraClassDefs: Map<String, ByteArray>) : ClassLoader(parent) {

        private val log = KotlinLogging.logger {}

        @Throws(ClassNotFoundException::class)
        override fun findClass(name: String): Class<*> {
            val classBytes= this.extraClassDefs.getValue(name)
            return defineClass(name, classBytes, 0, classBytes.size)
        }

        protected fun finalize() {
            clusterExecutor.clusterExtraClassDefs.remove(address)
        }
    }

    private val clusterExtraClassDefs: MutableMap<Address, MutableMap<String, ByteArray>> = with(ConcurrentHashMap<Address, MutableMap<String, ByteArray>>()) {
        withDefault { member ->
            this.getOrPut(member) {
                val thisIsHereSoTheLambdaReturnsTheNextLine = 2 //huh? Why?
                with(ConcurrentHashMap<String, ByteArray>()) {
                    withDefault { className ->
                        var counter = 0
                        while (!this.containsKey(className) && counter<100) {
                            if (counter%10==0) {
                                log.debug("Requesting class $className from $member")
                                channel.send(member, RequestClassDefinitionMessageData(className))
                            }
                            Thread.sleep(100) //nasty, but works. TODO: make reactive
                            counter++
                        }
                        this[className] ?: throw IllegalStateException("Unable to resolve class definition for $className from $member")
                    }
                }
            }
        }
    }

    private fun serializeClass(className: String): ByteArray {
        val c = this.javaClass.classLoader.loadClass(className)
        val src = c.protectionDomain.codeSource.location
        val classAsPath = c.name.replace('.', '/') + ".class"
        val stream = c.classLoader.getResourceAsStream(classAsPath)
        return stream.readAllBytes()
    }

    private fun createClassLoader(sourceMember: Address) =  ProxyClassLoader(this, sourceMember, this.javaClass.classLoader, clusterExtraClassDefs.getValue(sourceMember))

    init {
        log.debug("Initializing worker ${channel.address}")
        channel.receiver = object : ReceiverAdapter() {

            override fun viewAccepted(view: View) {
                val missing = currentMembersWorkload.keys.filterNot(view::containsMember)
                missing.forEach { currentMembersWorkload.remove(it) }
                //TODO: reassign work of missing
            }

            override fun receive(msg: Message) {
                CompletableFuture.runAsync { receiveInternal(msg) }
            }

            private fun receiveInternal(msg: Message) {
                val classLoader = createClassLoader(msg.src)

                val msgObject = try {
                    msg.getObject<Any>(classLoader)
                } catch (e: Exception) {
                    log.error(e) { "There was an error when resolving message" }
                    throw e
                }

                when (msgObject) {
                    is ScheduleTaskMessage -> {
                        log.debug("Queuing task ${msgObject.task.id} from ${msg.src} for local execution")
                        CompletableFuture.supplyAsync( Supplier { msgObject.task.callable.call() }, localExecutor)
                                .handle { res, th ->
                                    val resultMsg = if (th!=null) TaskResultExceptionMessage(msgObject.task.id, th) else TaskResultMessage(msgObject.task.id, res)
                                    log.debug { "Sending result ${msgObject.task.id} to ${msg.src}" }
                                    channel.send(msg.src, resultMsg)
                                    broadcastLoad()
                                }
                        broadcastLoad()
                    }
                    is ScheduledQueueSize -> {
                        if (!currentMembersWorkload.containsKey(msg.src)) {
                            log.debug("Introducing ${msg.src} to the worker pool: ${currentMembersWorkload.keys().toList()}")
                        }
                        currentMembersWorkload[msg.src]=msgObject.size
                    }
                    is TaskResultExceptionMessage -> {
                        if (assignedTasks[msgObject.id] == msg.src ) {
                            log.debug("Received throwable for task ${msgObject.id} from ${msg.src}: ${msgObject.throwable.message}")
                            assignedTasks.remove(msgObject.id)
                            submittedTasks.keys.first { it.id == msgObject.id }?.let {
                                submittedTasks.remove(it)?.completeExceptionally(msgObject.throwable)
                            }
                        }
                    }
                    is TaskResultMessage -> {
                        if (assignedTasks[msgObject.id] == msg.src ) {
                            log.debug("Received result for task ${msgObject.id} from ${msg.src}")
                            assignedTasks.remove(msgObject.id)
                            submittedTasks.keys.first { it.id == msgObject.id }?.let {
                                submittedTasks.remove(it)?.complete(msgObject.result)
                            }
                        }
                    }
                    is ClassDefinitionMessageData -> {
                        log.debug("Received class ${msgObject.canonicalName} from ${msg.src}")
                        clusterExtraClassDefs.computeIfAbsent(msg.src) { ConcurrentHashMap() } [msgObject.canonicalName] = msgObject.bytes
                    }
                    is ClearAllClassDefinitions -> {
                        log.debug("Clearing class proxy for ${msg.src}")
                        clusterExtraClassDefs.remove(msg.src)
                    }
                    is RequestClassDefinitionMessageData -> {
                        log.debug("Received class request ${msgObject.canonicalName} from ${msg.src}")
                        val classData = serializeClass(msgObject.canonicalName)
                        log.debug { "Sending class ${msgObject.canonicalName} to ${msg.src}" }
                        channel.send(msg.src, ClassDefinitionMessageData(msgObject.canonicalName, classData))
                    }
                }
            }
        }
        Executors.newSingleThreadExecutor().submit() {
            while (true) {
                Thread.sleep(5000)
                broadcastLoad()
            }
        }
    }

    private fun broadcastLoad() {
        channel.send(null, ScheduledQueueSize(getLocalQueueSize()))
    }

    fun clusterLoad() : Double {
        val load = currentMembersWorkload.reduceEntries(0, { it -> Pair(1L, it.value) }, { a, b -> Pair(a.first+b.first, a.second+b.second) })
        return 1.0 * load.second / load.first
    }

    private fun getLocalQueueSize() = localExecutor.taskCount - localExecutor.completedTaskCount

    private fun submitTaskToMember(task: Task<Any>, member: Address = currentMembersWorkload.minBy { (_,v) -> v }!!.key) {
        assignedTasks[task.id] = member
        log.debug("Sending task ${task.id} to $member")
        channel.send(member, ScheduleTaskMessage(task))
        //speculative increase of a remote work queue
        currentMembersWorkload[member]=(currentMembersWorkload[member]?:0) + 1
        // TODO: add timeout
    }

    fun <T> submit(callable: Callable<T>) : CompletableFuture<T> {
        if (callable !is Serializable) {
            throw IllegalArgumentException("Command $callable has to be serializable")
        }
        val task = Task(TaskId(), callable)
        val taskFuture = CompletableFuture<T>()

        log.debug("Accepting task ${task.id}")
        submittedTasks[task as Task<Any>]=taskFuture as CompletableFuture<Any>
        submitTaskToMember(task)

        return taskFuture
    }

}

fun main() {
    // to connect you can provide jgroups variables, e.g.:
    // -Djgroups.bind_addr=192.168.5.2

    val channel = JChannel()
    channel.connect("classloader.proxy")
    ClusterExecutor(channel)

    // hang in there
}