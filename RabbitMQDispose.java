package wazert.construcErp.rabbitmq;

import java.util.Map;

import org.apache.log4j.Logger;

import wazert.construcErp.util.CommonUtil;
import wazert.wolf.json.Json2ObjectByGsonUtil;
import wazert.wolf.util.SysGlobalUtil;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * @author clq
 * @function rabbitMQ处理类
 * @date 2018-02-05
 */
public class RabbitMQDispose {
	private static final Logger logger = Logger.getLogger(RabbitMQDispose.class);
	
	public static boolean exchangeDurableFlag = false;// 交换机是否持久化(即server重启不删除)
	public static boolean queueDurableFlag = false;// 队列是否持久化(即server重启不删除)
	
	public static boolean exchangeIdleDeleteFlag = false;// 交换机不再使用时是否自动删除
	public static boolean queueIdleDeleteFlag = true;// 队列不再使用时是否自动删除
	
	/**
	 * @author clq
	 * @function 接收数据处理
	 * @param Channel 渠道
	 * @param String 接收到数据
	 * @param envelope 为消息打包数据
	 * @return boolean 是否执行成功
	 * @date 2018-02-25
	 */
	public static boolean messageDoWith(Channel channel, String receiveMessage, Envelope envelope) {
		boolean backBool = false;
		try {
			if (channel == null 
					|| SysGlobalUtil.isVerifyJson(receiveMessage) == false) 
				return backBool;

			Map<String,Object> receiveMap = Json2ObjectByGsonUtil.parseJSON2Map(receiveMessage);// 接收数据
			if(receiveMap == null || receiveMap.isEmpty())
				return backBool;
			
			// 将来至大平台的实时轨迹数据处理
			CommonUtil.disposeRealTrack(receiveMap);
			
//			logger.error("从RabbitMQ队列中获取车辆流水号对应终端轨迹数据:" + CommonUtil.busIDToTrackDataMap);
			backBool = true;
		} catch (Exception e) {
			e.printStackTrace();
			backBool = false;
			logger.error("msg", e);
			CommonUtil.getException(e.toString(), e.getStackTrace(), RabbitMQDispose.class);// 将异常写入记录模块
		}
		return backBool;
	}
}
