package wazert.construcErp.rabbitmq;

import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import wazert.construcErp.util.CommonUtil;
import wazert.wolf.json.Json2ObjectByGsonUtil;
import wazert.wolf.util.SysGlobalUtil;
import wazert.wolf.util.support.ToolUtil;

/**
 * @author clq
 * @function rabbitmq监听程序
 * @date 2018-02-05
 */
public class RabbitMQListener implements ServletContextListener{
	private static final Logger logger = Logger.getLogger(RabbitMQListener.class);
	
	/**
	 * @author clq
	 * @function 容器销毁
	 * @param ServletContextEvent 容器事件
	 * @date 2018-02-05
	 */
	public void contextDestroyed(ServletContextEvent event) {
		logger.debug("rabbitmq监听程序已经销毁");
	}
	
	/**
	 * @author clq
	 * @function 容器初始化
	 * @param ServletContextEvent 容器事件
	 * @date 2018-02-05
	 */
	public void contextInitialized(ServletContextEvent event) {
		ServletContext servletContext = event.getServletContext();// 上下文容器
		String initConfigParamJson = servletContext.getInitParameter("initConfigParamJson");// 初始化配置参数(json)
		Map<String,Object> initConfigParamMap = Json2ObjectByGsonUtil.parseJSON2Map(initConfigParamJson);
		int serviceEnvironmentType = SysGlobalUtil.getMapInt(initConfigParamMap, "serviceEnvironmentType");// 当前服务器环境 -1:未配置  0:realse 1:test 2:debug
		ToolUtil.setServiceEnvironmentType(serviceEnvironmentType);
		
		// 获取任务运行开关配置参数
		Map<String,Object> timingRunSwitchConfigParamsMap = CommonUtil.gainDictionary("timingRunSwitchConfigParams");
		boolean rabbitMQ2MonitorStart = SysGlobalUtil.getMapBoolean(timingRunSwitchConfigParamsMap, "rabbitMQ2MonitorStart");// 运行rabbitmq消费者程序 是否运行定时任务
		
		// 消费者应用开启
		if(rabbitMQ2MonitorStart){
			Consumer.monitor();
			logger.debug("rabbitmq监听程序已经启动");
		}
	}

	/**
	 * @author clq
	 * @function 调试
	 * @date 2018-02-05
	 */
	public static void main(String[] args) {
		// 消费者监听开启
		Consumer.monitor();
	}
}
