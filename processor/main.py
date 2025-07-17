from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import threading
import time
from contextlib import asynccontextmanager
import os
import random
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 全局变量存储消费者状态
consumer_status = {"running": False, "messages_processed": 0, "last_message": None}

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.connect()
        self.init_tables()
    
    def connect(self):
        """连接数据库"""
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                database=os.getenv("POSTGRES_DB", "pipeline"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "password")
            )
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def init_tables(self):
        """初始化数据表"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        id SERIAL PRIMARY KEY,
                        sensor_id VARCHAR(255),
                        temperature FLOAT,
                        humidity FLOAT,
                        timestamp TIMESTAMP,
                        raw_data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS message_log (
                        id SERIAL PRIMARY KEY,
                        topic VARCHAR(255),
                        partition_id INTEGER,
                        offset_id BIGINT,
                        message_data JSONB,
                        processed_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                self.conn.commit()
                logger.info("Database tables initialized")
        except Exception as e:
            logger.error(f"Table initialization failed: {e}")
            self.conn.rollback()
            raise
    
    def save_sensor_data(self, data):
        """保存传感器数据"""
        try:
            with self.conn.cursor() as cursor:
                # 处理timestamp
                timestamp = data.get('timestamp')
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp)
                
                cursor.execute("""
                    INSERT INTO sensor_data (sensor_id, temperature, humidity, timestamp, raw_data)
                    VALUES (%(sensor_id)s, %(temperature)s, %(humidity)s, %(timestamp)s, %(raw_data)s)
                """, {
                    'sensor_id': data.get('sensor_id'),
                    'temperature': data.get('temperature'),
                    'humidity': data.get('humidity'),
                    'timestamp': timestamp,
                    'raw_data': json.dumps(data)
                })
                self.conn.commit()
                logger.info(f"Sensor data saved: {data.get('sensor_id')}")
        except Exception as e:
            logger.error(f"Failed to save sensor data: {e}")
            self.conn.rollback()
            raise
    
    def log_message(self, topic, partition, offset, message_data):
        """记录消息日志"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO message_log (topic, partition_id, offset_id, message_data)
                    VALUES (%s, %s, %s, %s)
                """, (topic, partition, offset, json.dumps(message_data)))
                self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to log message: {e}")
            self.conn.rollback()

# 全局数据库管理器
db_manager = None

def consume_data():
    """Kafka消费者函数"""
    global consumer_status, db_manager
    
    consumer_status["running"] = True
    logger.info("Starting Kafka consumer...")
    
    consumer = None
    try:
        # 等待Kafka服务可用
        time.sleep(10)
        
        consumer = KafkaConsumer(
            'sensor-data',
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='processor-group',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000
        )
        
        logger.info("Kafka consumer connected successfully")
        
        for message in consumer:
            try:
                if not consumer_status["running"]:
                    break
                    
                data = message.value
                logger.info(f"Received message: {data}")
                
                # 更新状态
                consumer_status["messages_processed"] += 1
                consumer_status["last_message"] = data
                
                # 保存到数据库
                if db_manager:
                    db_manager.save_sensor_data(data)
                    db_manager.log_message(
                        message.topic,
                        message.partition,
                        message.offset,
                        data
                    )
                
                # 打印处理信息
                print(f"Processed message {consumer_status['messages_processed']}: {data}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        if consumer:
            consumer.close()
        consumer_status["running"] = False
        logger.info("Kafka consumer stopped")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    global db_manager
    
    # 启动时初始化
    logger.info("Starting application...")
    
    # 初始化数据库
    db_manager = DatabaseManager()
    
    # 启动消费者线程
    consumer_thread = threading.Thread(target=consume_data, daemon=True)
    consumer_thread.start()
    
    yield
    
    # 关闭时清理
    logger.info("Shutting down application...")
    consumer_status["running"] = False

# 创建FastAPI应用
app = FastAPI(
    title="Data Pipeline API",
    description="Kafka消费者和数据处理服务",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
def read_root():
    """根路径"""
    return {
        "status": "Data Pipeline Running",
        "consumer_running": consumer_status["running"],
        "messages_processed": consumer_status["messages_processed"]
    }

@app.get("/health")
def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "consumer_status": consumer_status,
        "database_connected": db_manager is not None
    }

@app.get("/stats")
def get_stats():
    """获取统计信息"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 获取传感器数据统计
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT sensor_id) as unique_sensors,
                    AVG(temperature) as avg_temperature,
                    AVG(humidity) as avg_humidity,
                    MAX(created_at) as last_record_time
                FROM sensor_data
            """)
            sensor_stats = cursor.fetchone()
            
            # 获取消息日志统计
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_messages,
                    COUNT(DISTINCT topic) as unique_topics,
                    MAX(processed_at) as last_processed_time
                FROM message_log
            """)
            message_stats = cursor.fetchone()
            
            return {
                "sensor_data": dict(sensor_stats) if sensor_stats else {},
                "message_log": dict(message_stats) if message_stats else {},
                "consumer_status": consumer_status
            }
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recent-data")
def get_recent_data(limit: int = 10):
    """获取最近的数据"""
    try:
        if not db_manager:
            raise HTTPException(status_code=500, detail="Database not initialized")
        
        with db_manager.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT * FROM sensor_data 
                ORDER BY created_at DESC 
                LIMIT %s
            """, (limit,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error getting recent data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-message")
def send_test_message():
    """发送测试消息到Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_data = {
            'sensor_id': f'sensor_{random.randint(1, 10)}',
            'temperature': round(random.uniform(20, 30), 2),
            'humidity': round(random.uniform(40, 80), 2),
            'timestamp': time.time(),
            'location': f'Room {random.randint(1, 5)}'
        }
        
        producer.send('sensor-data', test_data)
        producer.flush()
        producer.close()
        
        logger.info(f"Test message sent: {test_data}")
        return {"status": "Test message sent", "data": test_data}
    except Exception as e:
        logger.error(f"Error sending test message: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)