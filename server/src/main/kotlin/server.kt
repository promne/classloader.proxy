
import mu.KotlinLogging
import org.jboss.shrinkwrap.resolver.api.maven.Maven
import org.jgroups.*
import org.jgroups.util.Util
import java.io.Serializable
import java.net.URLClassLoader
import java.util.*
import java.util.concurrent.*
import java.util.function.BiFunction
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Supplier


interface ClusterEvent
data class MemberRemoved(val member: Address) : ClusterEvent
data class TaskSubmittedEvent(val id: ClusterExecutor.TaskId) : ClusterEvent
data class TaskAssignedEvent(val id: ClusterExecutor.TaskId, val memberDst: Address) : ClusterEvent
data class TaskScheduledEvent(val id: ClusterExecutor.TaskId, val memberSrc: Address) : ClusterEvent
data class TaskExecutedSuccessEvent(val id: ClusterExecutor.TaskId, val memberSrc: Address, val executionTime: Long) : ClusterEvent
data class TaskExecutedFailEvent(val id: ClusterExecutor.TaskId, val memberSrc: Address) : ClusterEvent
data class TaskResultReceivedSuccessEvent(val id: ClusterExecutor.TaskId, val memberDst: Address, val executionTime: Long) : ClusterEvent
data class TaskResultReceivedFailEvent(val id: ClusterExecutor.TaskId, val memberDst: Address) : ClusterEvent

typealias ClusterExecutorListener = (ClusterEvent) -> Unit

class ClusterExecutor(val channel: JChannel, private val maxTaskCount: Int, private val broadcastInterval : Long = 5000) {

    private val log = KotlinLogging.logger {}

    private val listeners = ConcurrentHashMap.newKeySet<ClusterExecutorListener>()

    // data classes
    data class TaskId(val x: String = UUID.randomUUID().toString()): Serializable
    data class Task(val id: TaskId, val callableData: ByteArray, val dependencies: Collection<String>, val timeout: Long, val timeUnit: TimeUnit): Serializable
    data class TaskStats(val time: Long): Serializable

    // locally accepted tasks
    private val submittedTasks = ConcurrentLinkedQueue<Pair<Task, CompletableFuture<Any>>>()
    private val assignedTasks = ConcurrentHashMap<Pair<TaskId, Address>, CompletableFuture<TaskResultMessage>>()
    private val executingTasks = ConcurrentHashMap.newKeySet<TaskId>()

    // cluster status
    private val currentMembersQueueStats = ConcurrentHashMap<Address, TaskQueueStats>()
    get() {
        field.computeIfAbsent(channel.address) { getLocalQueueStats() }
        return field
    }

    // messages
    class TaskRequestMessage(): Serializable
    data class ScheduleTaskMessage(val task: Task): Serializable
    data class TaskResultMessage(val id: TaskId, val stats: TaskStats, val result: Any): Serializable
    data class TaskResultExceptionMessage(val id: TaskId, val throwable: Throwable): Serializable
    data class TaskQueueStats(val capacity: Int, val executing: Int, val submitted: Int, val assigned: Int): Serializable

    // locally executed tasks
    private val localExecutor = Executors.newCachedThreadPool() as ThreadPoolExecutor
    private val localSingleThreadExecutor = Executors.newSingleThreadExecutor()

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
                            if (counter%20==0) {
                                log.debug("Requesting class $className from $member")
                                channel.send(member, RequestClassDefinitionMessageData(className))
                            }
                            Thread.sleep(100) //nasty, but works. TODO: make reactive
                            counter++
                        }
                        this[className]?.let {
                            log.debug("Received class $className from $member")
                        }
                        this[className] ?: throw IllegalStateException("Unable to resolve class definition for $className from $member")
                    }
                }
            }
        }
    }

    private fun serializeClass(className: String): ByteArray {
        try {
            val c = this.javaClass.classLoader.loadClass(className)
            val src = c.protectionDomain.codeSource.location
            val classAsPath = c.name.replace('.', '/') + ".class"
            val stream = c.classLoader.getResourceAsStream(classAsPath)
            return stream.readAllBytes()
        } catch (e : Exception) {
            e.printStackTrace()
            throw e
        }
    }

    private fun createClassLoader(sourceMember: Address, task: Task) : ClassLoader {
        val parentClassLoader = if (task.dependencies.isEmpty()) {
            this.javaClass.classLoader
        } else {
            log.debug("Resolving dependencies for task ${task.id}: ${task.dependencies}")
            val resolver = System.getProperty("m2settings")?.let { Maven.configureResolver().fromFile(it)  } ?: Maven.resolver()
            val libs = resolver.resolve(task.dependencies).withTransitivity().asFile()
            log.debug("Resolved dependencies for task ${task.id}")

            val libURLs = libs.map { it.toURI().toURL() }.toTypedArray()
            URLClassLoader(libURLs, this.javaClass.classLoader)
        }

        return ProxyClassLoader(this, sourceMember, parentClassLoader , clusterExtraClassDefs.getValue(sourceMember))
    }

    init {
        log.debug("Initializing worker ${channel.address}")
        channel.receiver = object : ReceiverAdapter() {

            override fun viewAccepted(view: View) {
                val missing = currentMembersQueueStats.keys.filterNot(view::containsMember)
                missing.forEach {memberAddress ->
                    currentMembersQueueStats.remove(memberAddress)
                    emmitEvent(MemberRemoved(memberAddress))
                    assignedTasks.filter { memberAddress.equals(it.key.second) }.values.forEach { it.completeExceptionally(IllegalStateException("Member $memberAddress lost")) }
                //TODO: reassign work of missing
                }
            }

            override fun receive(msg: Message) {
                CompletableFuture.runAsync { receiveInternal(msg) }
            }

            private fun receiveInternal(msg: Message) {

                val msgObject = try {
                    msg.getObject<Any>()
                } catch (e: Exception) {
                    log.error(e) { "There was an error when resolving message ${msg}" }
                    throw e
                }

                when (msgObject) {
                    is ScheduleTaskMessage -> {
                        log.debug("Queuing task ${msgObject.task.id} from ${msg.src} for local execution")

                        val task = msgObject.task
                        emmitEvent(TaskScheduledEvent(task.id, msg.src))
                        executingTasks.add(task.id)

                        CompletableFuture
                                .supplyAsync( Supplier { createClassLoader(msg.src, task) }, localSingleThreadExecutor)
                                .thenApplyAsync( Function { classLoader : ClassLoader ->
                                    Thread.currentThread().contextClassLoader = classLoader
                                    val startTime = System.currentTimeMillis()
                                    val result = Util.objectFromByteBuffer<Callable<Any>>(task.callableData, 0, task.callableData.size, classLoader).call()
                                    val executionTime = System.currentTimeMillis() - startTime
                                    Pair(result, executionTime)
                                }, localExecutor)
                                .orTimeout(task.timeout, task.timeUnit)
                                .handle { res, th ->
                                    executingTasks.remove(task.id)

                                    val resultMsg = if (th!=null) {
                                        log.error(th.message, th)
                                        emmitEvent(TaskExecutedFailEvent(task.id, msg.src))
                                        TaskResultExceptionMessage(task.id, th)
                                    } else {
                                        val (result, executionTime) = res
                                        log.trace { "Task ${task.id} execution time ${executionTime}" }
                                        emmitEvent(TaskExecutedSuccessEvent(task.id, msg.src, executionTime))
                                        TaskResultMessage(task.id, TaskStats(executionTime), result)
                                    }
                                    log.debug { "Sending result ${task.id} to ${msg.src}" }
                                    channel.send(msg.src, resultMsg)

                                    broadcastWorkRequest()
                                }

                        broadcastWorkRequest()
                    }
                    is TaskQueueStats -> {
                        if (!currentMembersQueueStats.containsKey(msg.src)) {
                            log.debug("Introducing ${msg.src} to the worker pool: ${currentMembersQueueStats.keys().toList()}")
                        }
                        currentMembersQueueStats[msg.src]=msgObject
                    }
                    is TaskResultExceptionMessage -> {
                        log.debug("Received throwable for task ${msgObject.id} from ${msg.src}: ${msgObject.throwable.message}")
                        assignedTasks[Pair(msgObject.id, msg.src)]?.completeExceptionally(msgObject.throwable)
                    }
                    is TaskResultMessage -> {
                        log.debug("Received result for task ${msgObject.id} from ${msg.src}")
                        assignedTasks[Pair(msgObject.id, msg.src)]?.complete(msgObject)
                    }
                    is ClassDefinitionMessageData -> {
                        log.debug("Received class data ${msgObject.canonicalName} from ${msg.src}")
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
                    is TaskRequestMessage -> {
                        submittedTasks.poll()?.let {
                            val (task, future) = it
                            submitTaskToMember(task, msg.src).handle { t, u ->
                                u?.let { future.completeExceptionally(it) } ?: future.complete(t)
                            }
                        }
                    }
                }
            }
        }

        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay( {
            broadcastWorkRequest()
            broadcastLoad()
            log.debug { "Cluster load $currentMembersQueueStats" }
        }, broadcastInterval, broadcastInterval, TimeUnit.MILLISECONDS)

    }

    public fun addListener(listener: ClusterExecutorListener) = listeners.add(listener)

    private fun emmitEvent(event: ClusterEvent) {
        localExecutor.execute {
            listeners.forEach {
                it(event)
            }
        }
    }

    private fun broadcastWorkRequest() {
        if (executingTasks.size < maxTaskCount) {
            log.debug { "Current load ${executingTasks.size} is less than allowed ${maxTaskCount}, broadcasting request for work" }
            channel.send(null, TaskRequestMessage())
        }
    }

    private fun getLocalQueueStats() = TaskQueueStats(maxTaskCount, executingTasks.size, submittedTasks.size, assignedTasks.size)

    private fun broadcastLoad() {
        channel.send(null, getLocalQueueStats())
    }

    private fun getLocalLoad() = localExecutor.taskCount - localExecutor.completedTaskCount

    public fun getClusterQueueStatus() = run {
        currentMembersQueueStats[channel.address] = getLocalQueueStats()
        currentMembersQueueStats
    }

    private fun submitTaskToMember(task: Task, member: Address): CompletableFuture<Any> {
        val memberKey = Pair(task.id,member)

        val taskFuture = CompletableFuture<TaskResultMessage>()
        taskFuture.whenComplete { t, u ->
            val event = u?.let { TaskResultReceivedFailEvent(task.id, member) } ?: TaskResultReceivedSuccessEvent(t.id, member, t.stats.time)
            emmitEvent(event)
            assignedTasks.remove(memberKey)
        }

        assignedTasks[memberKey] = taskFuture
        log.debug("Sending task ${task.id} to $member")
        channel.send(member, ScheduleTaskMessage(task))
        emmitEvent(TaskAssignedEvent(task.id, member))

        return taskFuture.thenApply {
            it.result
        }
    }

    fun <T> submit(callable: Callable<T>, executionTimeout: Long, timeUnit: TimeUnit, dependencies: Collection<String> = setOf()) : CompletableFuture<T> {
        require(callable is Serializable) { "Command $callable has to be serializable" }

        val callableObjectData = Util.objectToByteBuffer(callable)
        val task = Task(TaskId(), callableObjectData, dependencies, executionTimeout, timeUnit)

        val taskFuture = CompletableFuture<Any>()
        submittedTasks.add(Pair(task, taskFuture))

        taskFuture
            .whenComplete { t, u ->
                submittedTasks.removeIf { it.second == taskFuture }
            }

        log.debug("Accepting task ${task.id}")
        emmitEvent(TaskSubmittedEvent(task.id))
        return taskFuture as CompletableFuture<T>
    }

}



fun main() {
    // to connect you can provide jgroups variables, e.g.:
    // -Djgroups.bind_addr=192.168.5.2
//    System.setProperty("jgroups.bind_addr", "192.168.56.1")

    val channel = JChannel(System.getProperty("jgroups.property","udp.xml"))
    channel.connect("classloader.proxy")

    val maxTaskCount = System.getProperty("maxTaskCount", (2 * Runtime.getRuntime().availableProcessors()).toString())
    ClusterExecutor(channel, maxTaskCount.toInt())

    // hang in there
}