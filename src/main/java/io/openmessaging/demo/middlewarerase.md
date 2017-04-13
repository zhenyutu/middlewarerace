# middlewarerace
### (1)源码阅读理解

#### 1. DefaultBytesMassage：消息默认格式
DefaultBytesMassage继承自BytesMessage,Message
包含类属性：一个KeyValue对的Map类型的headers,一个存储字节数组主体byte[] body
#### 2. messageBuckets消息队列存储格式
消息队列集合messageBuckets：Map<String, ArrayList<Message>>类型,key为队列名称,value为具体队列
#### 3. DefaultProducer:消息生产者
- createBytesMessageToTopic(String topic, byte[] body):
功能：将字节数组消息创建到topic
返回值：返回一个Header为topic的Message
实现：调用messageFactory的createBytesMessageToTopic方法,返回一个Header为topic的消息
- createBytesMessageToQueue(String topic, byte[] body):
功能：将字节数组消息创建到Queue
返回值：返回一个Header为Queue的Message
实现：调用messageFactory的createBytesMessageToQueue方法,返回一个Header为Queue的消息
- send(Message message)
功能：发送消息
返回值：无
实现：判断消息类型为Topic或者Queue,
通过messageStore.putMessage(String bucket, Message message)将消息加入具体的消息队列中
#### ４. DefaultPullConsumer:消息消费者
- attachQueue(String queueName, Collection<String> topics):
功能：为消息消费者绑定需要消费的消息队列
返回值：无，直接操作类属性
实现：创建一个Set的类型的buckets,将需要绑定的Queue和Topic的名称放入Set中去重，然后将Set转换成消息队列名称列表
- Message pullNoWait():
功能：获取消息信息
返回值:Massage类型的信息实例
实现：首先判断当前消费者对否已经绑定消息队列Queue和Topic，如果没有则返回null;
如果已经绑定则开始轮询对象绑定的Queue和Topic,如果询问的队列有消息存在，则返回该消息否则返回空,利用偏移量保证轮询每一条message

### (2)理解
- 消息队列根据消息消费获取方式分为PUSH和PULL型,
PUSH型为消息队列主动推送消息到消费端；PULL型为消息消费端主动从队列中抓取Message.
PUSH型很难控制数据发送给不同消费者的速度,PULL型可以由消费者自己控制，但是PULL模型可能造成消费者在没有消息的情况下盲等，这种情况下可以通过long polling机制缓解。
对于几乎每时每刻都有消息传递的流式系统，使用Pull模型更合适




