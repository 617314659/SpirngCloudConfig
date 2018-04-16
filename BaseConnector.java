package wazert.construcErp.rabbitmq;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import wazert.construcErp.util.CommonUtil;
import wazert.wolf.util.SysGlobalUtil;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author clq
 * @function rabbitMQ基础连接器
 * @date 2018-02-05
 */
public class BaseConnector {
	private static final Logger logger = Logger.getLogger(BaseConnector.class);
	
	/**
	 * @author clq
	 * @function 获取连接
	 * @date 2018-02-05
	 */
    public static Connection gainConnection(){
    	Connection connection = null;// 连接
    	try {
			synchronized (BaseConnector.class) {
				
				// 获取RabbitMQ配置参数
				Map<String,Object> rabbitMQConfigParamsMap = CommonUtil.gainDictionary("rabbitMQConfigParams");
				String hostPort = CommonUtil.gainRabbitMQConfig("hostPort", "172.16.0.41:5672");// Rabbit-Server IP地址:端口
				
				String userName = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "userName");// 用户名
				String password = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "password");// 密码
				String virtualHost = SysGlobalUtil.getMapString(rabbitMQConfigParamsMap, "virtualHost");// 虚拟主机名称
				if("".equals(hostPort) || hostPort.indexOf(":") <= 0
						|| "".equals(userName) || "".equals(password) || "".equals(virtualHost))
					return null;
				
				String[] hostPortArray = hostPort.split(":");
				
				// 配置工厂类
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(hostPortArray[0]);
				factory.setPort(Integer.parseInt(hostPortArray[1]));
				factory.setUsername(userName);
				factory.setPassword(password);
				factory.setVirtualHost(virtualHost);
				
				// 关键所在,指定线程池  
		        ExecutorService service = Executors.newFixedThreadPool(10);
		        factory.setSharedExecutor(service);
		        
		        // 设置自动恢复
		        factory.setAutomaticRecoveryEnabled(true);
		        factory.setNetworkRecoveryInterval(10);// 设置恢复间隔时间10s,重试一次
		        factory.setTopologyRecoveryEnabled(false);// 设置不重新声明交换器,队列等信息
				
				// 创建新连接
				connection = factory.newConnection();
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("msg", e);
			connection = null;
		}
    	return connection;
    }
}
