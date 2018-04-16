package wazert.construcErp.rabbitmq;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import wazert.construcErp.util.CommonUtil;
import wazert.wolf.util.SysGlobalUtil;
import wazert.wolf.util.support.ToolUtil;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @author clq
 * @function rabbitMQ消费者
 * @date 2018-02-05
 */
public class Consumer {
	private static final Logger logger = Logger.getLogger(Consumer.class);
	public static ExecutorService pool = Executors.newCachedThreadPool();// 线程池的大小会根据执行的任务数动态分配  
	private static volatile String[] exchangeNameArray = new String[]{"construcDataExchange"};// 交换机名称(多个逗号隔开)默认:construcDataExchange
	
	private static Connection connection = null;// 连接
    private static Channel channel = null;// 渠道
	private static String[] queueNameArray = null;// 队列名称数组
	
	/**
	 * @author clq
	 * @function 监听获取消息
	 * @date 2018-02-05
	 */
	@SuppressWarnings("unchecked")
	public static void monitor(){
    	try {
			suspend(1L);// 暂停1秒
			
    		// 关闭数据流
    		closeStream();
    		
			// 获取连接
			connection = BaseConnector.gainConnection();
			if(connection == null)
				return;
			
			// 获取系统配置参数
			Map<String,Object> systemConfigParamsMap = CommonUtil.gainDictionary("systemConfigParams");// 系统配置参数
			String plateNameFlag = SysGlobalUtil.getMapString(systemConfigParamsMap, "plateNameFlag");// 系统平台名称(标识)
			plateNameFlag = "".equals(plateNameFlag) ? ToolUtil.getModuleName() : plateNameFlag;
			
			// 获取RabbitMQ配置参数
			Map<String,Object> rabbitMQConfigParamsMap = CommonUtil.gainDictionary("rabbitMQConfigParams");
			String exchangeNames = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "exchangeNames");// 交换机名称(多个逗号隔开)默认:construcDataExchange
			exchangeNameArray = !"".equals(exchangeNames) ? exchangeNames.split(",") : exchangeNameArray;
			
			int basicQos = SysGlobalUtil.getMapInt(rabbitMQConfigParamsMap, "basicQos");// 每次取消息数量 默认:8
			basicQos = basicQos == 0 ? 8 : basicQos;
			
			int consumerExchangeType = SysGlobalUtil.getMapInt(rabbitMQConfigParamsMap, "consumerExchangeType");// 消费者交换机类型 默认:1
			consumerExchangeType = consumerExchangeType == 0 ? 1 : consumerExchangeType;
			
			// 创建渠道
			channel = connection.createChannel();
	        channel.basicQos(basicQos);// server push消息时的队列长度,每次取32条消息
	        
			queueNameArray = new String[]{plateNameFlag};// 队列名称数组
 	        
			/*
			 * 交换机类型:
			 * 1 default 默认,向指定的队列发送消息,消息只会被一个consumer处理,多个消费者消息会轮训处理,消息发送时如果没有consumer,消息不会丢失
			 * 2 direct 直接交换器,工作方式类似于单播,exchange会将消息发送完全匹配routingKey的队列,向所有绑定了相应routingKey的队列发送消息
			 * 3 fanout 广播交换器,不管消息的routingKey设置了什么,exchange都会将消息转发给所有绑定的队列,接收方也必须通过fanout交换机获取消息,所有连接到该交换机的consumer均可获取消息
			 * 4 topic 主题交换器,工作方式类似于组播,exchange会将消息转发和routingKey匹配模式相同的所有队列,与direct模式有类似之处,都使用routingKey作为路由
			 * 5 headers 消息体匹配,与topic和direct有一定相似之处,但不是通过routingKey来路由消息(可忽略)
			 */
			switch(consumerExchangeType){
			case 1:// default
				String queueNames = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "queueNames");// 队列名称(多个逗号隔开)默认:construcDataQueue
				queueNameArray = !"".equals(queueNames) ? queueNames.split(",") : null;
				if(queueNameArray == null || queueNameArray.length <= 0){
    				logger.error("定义default默认错误,无队列名称");
    				return;
    			}

				// 遍历
				for(String queueName : queueNameArray){
					/*
			         * 定义队列 
			         * 队列的相关参数需要与第一次定义该队列时相同,否则会出错,使用channel.queueDeclarePassive()可只被动绑定已有队列,而不创建
			         * 参数1：队列名称
			         * 参数2：为true时server重启,队列不会消失
			         * 参数3：队列是否是独占的,如果为true只能被一个connection使用,其他连接建立时会抛出异常
			         * 参数4：队列不再使用时是否自动删除(没有连接,并且没有未处理的消息)
			         * 参数5：建立队列时的其他参数
			         */
			        channel.queueDeclare(queueName, RabbitMQDispose.queueDurableFlag, false, RabbitMQDispose.queueIdleDeleteFlag, null);// 声明队列,已存在则不重复声明
				}
    			break;
    		case 2:// direct
    			// 遍历
    			for(String exchangeName : exchangeNameArray){
    				/*
    				 * 定义交换机
    				 * 参数1：交换机名称
    				 * 参数2：交换机类型
    				 * 参数3：交换机持久性,如果为true则服务器重启时不会丢失
    				 * 参数4：如果服务器不再使用该交换器,则删除
    				 * 参数5：交换机的其他属性
    				 */
                	channel.exchangeDeclare(exchangeName, "direct", RabbitMQDispose.exchangeDurableFlag, RabbitMQDispose.exchangeIdleDeleteFlag, null);// 声明交换机,已存在则不重复声明
    			}
    			
    			// 声明一个临时队列,该队列会在使用完比后自动销毁
//              plateNameFlag = channel.queueDeclare().getQueue();
				channel.queueDeclare(plateNameFlag, RabbitMQDispose.queueDurableFlag, false, RabbitMQDispose.queueIdleDeleteFlag, null);// 声明队列,已存在则不重复声明
    			
				// 获取授权审批配置参数
				Map<String,Object> authorizeConfigParamsMap = CommonUtil.gainDictionary("authorizeConfigParams");
				String companyID = SysGlobalUtil.getMapString(authorizeConfigParamsMap, "companyID");// 企业ID
				
				// 遍历
    			for(String exchangeName : exchangeNameArray){
	            	/*
					 * 绑定队列入交换机
					 * 参数1：队列名称
					 * 参数2：交换机名称
					 * 参数3：路由标识
					 * 参数4：其他绑定参数
					 */
	        		channel.queueBind(plateNameFlag, exchangeName, "construcData-" + companyID);// 通过routingKey把queue在exchanger进行绑定
    			}
    			break;
    		case 3:// fanout
    			// 遍历
    			for(String exchangeName : exchangeNameArray){
    				channel.exchangeDeclare(exchangeName, "fanout", RabbitMQDispose.exchangeDurableFlag, RabbitMQDispose.exchangeIdleDeleteFlag, null);// 声明交换机,已存在则不重复声明
    			}
				channel.queueDeclare(plateNameFlag, RabbitMQDispose.queueDurableFlag, false, RabbitMQDispose.queueIdleDeleteFlag, null);// 声明队列,已存在则不重复声明
				
    			// 遍历
    			for(String exchangeName : exchangeNameArray){
    				channel.queueBind(plateNameFlag, exchangeName, "");// 通过routingKey把queue在exchanger进行绑定
    			}
    			break;
    		case 4:// topic
    			// 遍历
    			for(String exchangeName : exchangeNameArray){
    				channel.exchangeDeclare(exchangeName, "topic", RabbitMQDispose.exchangeDurableFlag, RabbitMQDispose.exchangeIdleDeleteFlag, null);// 声明交换机,已存在则不重复声明
    			}
    			
				channel.queueDeclare(plateNameFlag, RabbitMQDispose.queueDurableFlag, false, RabbitMQDispose.queueIdleDeleteFlag, null);// 声明队列,已存在则不重复声明
		        
            	String routingKeysForTopic = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "routingKeysForTopic");// 路由标识(多个逗号隔开)(主题交换器) 默认:#
            	String[] routingKeyForTopicArray = routingKeysForTopic.split(",");
            	
            	// 遍历
            	for(String routingKeyForTopic : routingKeyForTopicArray){
            		if("".equals(routingKeyForTopic))
            			continue;

        			// 遍历
        			for(String exchangeName : exchangeNameArray){
        				channel.queueBind(plateNameFlag, exchangeName, routingKeyForTopic);// 通过routingKey把queue在exchanger进行绑定
        			}
            	}
    			break;
    		case 5:// headers
    			// 遍历
    			for(String exchangeName : exchangeNameArray){
    				channel.exchangeDeclare(exchangeName, "headers", RabbitMQDispose.exchangeDurableFlag, RabbitMQDispose.exchangeIdleDeleteFlag, null);// 声明交换机,已存在则不重复声明
    			}
    			
				channel.queueDeclare(plateNameFlag, RabbitMQDispose.queueDurableFlag, false, RabbitMQDispose.queueIdleDeleteFlag, null);// 声明队列,已存在则不重复声明
		        
    			/* 
    			 * 头部条件匹配
    			 * all匹配所有条件 any匹配任意条件
    			 * ("x-match", "any")
    			 */
    			Map<String, Object> headersConditionMap = (Map<String, Object>)rabbitMQConfigParamsMap.get("headersConditionJson");

    			// 遍历
    			for(String exchangeName : exchangeNameArray){
	    			if(headersConditionMap != null && !headersConditionMap.isEmpty()){
	    		        channel.queueBind(plateNameFlag, exchangeName, "", headersConditionMap);// 通过routingKey把queue在exchanger进行绑定
	    			}else
	    		        channel.queueBind(plateNameFlag, exchangeName, "");// 通过routingKey把queue在exchanger进行绑定
    			}
    			break;
			}
			
			// 消费者监听保护2
			protectMonitor2();
		} catch (Exception e) {
			logger.error("msg", e);
			
			monitor();// 建立消费者监听
		}
    }
	
	/**
     * @author clq
     * @function 消费者监听保护1
     * @date 2018-02-05
     */
	public static void protectMonitor1(){
		try {
	        // 1、消费者(DefaultConsumer)
	        // 默认消费者
	        DefaultConsumer consumer = new DefaultConsumer(channel){
		        
				/**
				 * @author clq
				 * @function 提交帮助类
				 * @param consumerTag 获取该参数信封中包含的交付标记
		     	 * @param envelope 为消息打包数据
		     	 * @param properties 消息的内容头数据
		     	 * @param body 消息体(不透明的、客户机特定的字节数组)
				 * @date 2018-02-05
				 */
	            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body){
	            	int executeType = 2;// 执行状态 0:未知 1:成功 2:重复 3:拒绝
	            	try {
			        	TimeUnit.SECONDS.sleep(5L);// 休眠5秒
			        	
	            		if(channel == null || channel.isOpen() == false){
							reconnConsumer(1);// 重连
							return;
						}
	            		
						// 消息消息处理
						String receiveMessage = new String(body, "utf-8");
//						logger.error("rabbitmq接收到数据:" + receiveMessage);
						boolean messageBool = RabbitMQDispose.messageDoWith(channel, receiveMessage, envelope);
						if(messageBool)
							executeType = 1;

						executeType = 1;
		            } catch (Exception e) {
		    			logger.error("msg", e);
						executeType = 3;// 拒绝
						CommonUtil.getException(e.toString()
								, e.getStackTrace(), this.getClass());// 将异常写入记录模块
						
						reconnConsumer(1);// 重连
						return;
		            } finally {
						try {
							if(channel == null || channel.isOpen() == false || envelope == null){
								reconnConsumer(1);// 重连
								return;
							}
							
							long deliveryTag = envelope.getDeliveryTag();
							switch(executeType){
							case 1:
								/*
								 * 手动消息确认
								 * 回复ack包,如果不回复,消息不会在服务器删除
								 * 参数1：获取该参数信封中包含的交付标记
								 * 参数2：true所有的信息包括提供的交货标签,false只承认提供的交货标签
								 */
								channel.basicAck(deliveryTag, false);
								break;
							case 2:
								/*
								 * 拒绝一个或多个收到的消息
								 * 参数1：获取该参数信封中包含的交付标记
								 * 参数2：多重true拒绝所有的消息,包括提供的交货标签;不公正地拒绝提供的交付标签
								 * 参数3：如果true消息(s)被拒绝应该被重新请求
								 */
								channel.basicNack(deliveryTag, false, true);
								break;
							case 3:
								channel.basicNack(deliveryTag, false, false);
								break;
							}
						} catch (Exception e) {
							logger.error("msg", e);
							reconnConsumer(1);// 重连
							return;
						}
					}
	            }
	        };
		        
	        // 遍历1
	        for(String queueName : queueNameArray){
	        	/*
	 	         * 启动一个非独占的消费者
	 	         * 参数1: 队列名称
	 	         * 参数2：是否发送ack包,不发送ack消息会持续在服务端保存,直到收到ack 可以通过channel.basicAck手动回复ack
	 	         * 参数3：消费者
	 	         */
	 	        channel.basicConsume(queueName, false, consumer);
//	 		    channel.basicGet(queueName, true); // 主动去服务器检索是否有新消息,而不是等待服务器推送
	        }
		} catch (Exception e) {
			logger.error("msg", e);
			CommonUtil.getException(e.toString()
					, e.getStackTrace(), Consumer.class);// 将异常写入记录模块

			reconnConsumer(1);// 重连
			return;
		}
	}
	
	/**
     * @author clq
     * @function 消费者监听保护2
     * @date 2018-02-05
     */
	public static void protectMonitor2(){
		// 线程处理
		pool.execute(
		new Runnable() {
			public void run() {
				try {
					if(channel == null || channel.isOpen() == false){
				        reconnConsumer(2);// 重连
				        return;
					}
	            	
			        // 消费者
			        QueueingConsumer consumer = new QueueingConsumer(channel);
			        // 遍历2
			        for(String queueName : queueNameArray){
			 	        channel.basicConsume(queueName, false, consumer);
			        }
				    
			        /*
			         * 读取队列并且阻塞,
			         * 即在读到消息之前在这里阻塞,直到等到消息,完成消息的阅读后,继续阻塞循环
			         */
			        while(true){
			        	TimeUnit.SECONDS.sleep(5L);// 休眠5秒
			        	
			        	Envelope envelope = null;
			        	int executeType = 2;// 执行状态 0:未知 1:成功 2:重复 3:拒绝
			            try {
			            	if(channel == null || channel.isOpen() == false)// 尚未开启
			            		break;
			            	
			            	QueueingConsumer.Delivery delivery = consumer.nextDelivery(10000L);// 超时时间为10秒
			            	if(delivery == null)
			            		break;
			            	
				        	envelope = delivery.getEnvelope();
//				        	AMQP.BasicProperties properties = delivery.getProperties();
				        	byte[] body = delivery.getBody();
				        	
							// 消息消息处理
							String receiveMessage = new String(body, "utf-8");
//							logger.error("rabbitmq接收到数据:" + receiveMessage);
							boolean messageBool = RabbitMQDispose.messageDoWith(channel, receiveMessage, envelope);
							if(messageBool)
								executeType = 1;
			            } catch (Exception e) {
			    			logger.error("msg", e);
							executeType = 3;// 拒绝
							CommonUtil.getException(e.toString()
									, e.getStackTrace(), Consumer.class);// 将异常写入记录模块
							break;
						} finally {
							try {
								if(channel == null || channel.isOpen() == false 
										|| envelope == null)
									break;
								
								long deliveryTag = envelope.getDeliveryTag();
								switch(executeType){
								case 1:
									/*
									 * 手动消息确认
									 * 回复ack包,如果不回复,消息不会在服务器删除
									 * 参数1：获取该参数信封中包含的交付标记
									 * 参数2：true所有的信息包括提供的交货标签,false只承认提供的交货标签
									 */
									channel.basicAck(deliveryTag, false);
									break;
								case 2:
									/*
									 * 拒绝一个或多个收到的消息
									 * 参数1：获取该参数信封中包含的交付标记
									 * 参数2：多重true拒绝所有的消息,包括提供的交货标签;不公正地拒绝提供的交付标签
									 * 参数3：如果true消息(s)被拒绝应该被重新请求
									 */
									channel.basicNack(deliveryTag, false, true);
									break;
								case 3:
									channel.basicNack(deliveryTag, false, false);
									break;
								}
							} catch (Exception e) {
								logger.error("msg", e);
								break;
							}
						}
			        }
			        reconnConsumer(2);// 重连
			        return;
				} catch (Exception e) {
					logger.error("msg", e);
					CommonUtil.getException(e.toString()
							, e.getStackTrace(), Consumer.class);// 将异常写入记录模块
					
					reconnConsumer(2);// 重连
					return;
				}
			}
		});
	}
	
	/**
     * @author clq
     * @function 关闭数据流
     * @date 2018-02-07
     */
	private static void closeStream(){
		// 关闭渠道
		try {
			if(channel != null)
				channel.abort();
		} catch (Exception e) {
			logger.error("msg", e);
		} finally {
			try {
				if(channel != null)
					channel.close();
			} catch (Exception e) {
				logger.error("msg", e);
			} finally {
				channel = null;
			}
		}
		
		// 关闭连接
		try {
			if(connection != null)
				connection.abort();
		} catch (Exception e) {
			logger.error("msg", e);
		} finally {
			try {
				if(connection != null)
					connection.close();
			} catch (Exception e) {
				logger.error("msg", e);
			} finally {
				connection = null;
			}
		}
		logger.error("初始化消费者监听,关闭渠道与连接");
	}
	
	/**
     * @author clq
     * @function 消费者重连
     * @date 2018-02-05
     */
	private static void reconnConsumer(int type){
		if (channel.isOpen()) {
			switch(type){
			case 1:
				protectMonitor1();
				break;
			case 2:
				protectMonitor2();
				break;
			}
		}else
			monitor();// 建立消费者监听
	}
	
	/**
     * @author clq
     * @function 暂停
     * @param long 暂停时间
     * @date 2018-02-05
     */
	private static void suspend(long s){
		try {
			TimeUnit.SECONDS.sleep(s);// 休眠
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
     * @author clq
     * @function 调试
     * @date 2018-02-05
     */
	public static void main(String[] args){
		// 建立消费者监听
		monitor();
	}
}

