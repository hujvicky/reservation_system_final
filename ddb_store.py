# ======================================
# ddb_store.py - DynamoDB高性能儲存層
# ======================================
import boto3
import json
import os
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
import logging
from decimal import Decimal
import uuid

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定義台灣時區 (UTC+8)
TAIWAN_TZ = timezone(timedelta(hours=8))

class DynamoDBStore:
    def __init__(self):
        self.VERSION = "1.0-performance-optimized"
        
        # 表名配置
        self.reservations_table = os.environ.get('RESERVATIONS_TABLE', 'seat-reservations')
        self.tables_table = os.environ.get('TABLES_TABLE', 'seat-tables')
        self.idempotency_table = os.environ.get('IDEMPOTENCY_TABLE', 'seat-idempotency')
        
        # DynamoDB客户端
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
        
        # 表引用
        self.reservations = self.dynamodb.Table(self.reservations_table)
        self.tables = self.dynamodb.Table(self.tables_table)
        self.idempotency = self.dynamodb.Table(self.idempotency_table)
        
        # 初始化表结构
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """確保所有需要的DynamoDB表存在"""
        try:
            # 檢查預訂表
            self._create_reservations_table()
            # 檢查桌位表
            self._create_tables_table()
            # 檢查防重複表
            self._create_idempotency_table()
            
        except Exception as e:
            logger.error(f"創建DynamoDB表失敗: {e}")

    def _create_reservations_table(self):
        """創建預訂表"""
        try:
            self.client.describe_table(TableName=self.reservations_table)
            logger.info(f"預訂表 {self.reservations_table} 已存在")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"創建預訂表: {self.reservations_table}")
                self.client.create_table(
                    TableName=self.reservations_table,
                    KeySchema=[
                        {'AttributeName': 'reservation_id', 'KeyType': 'HASH'},
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'reservation_id', 'AttributeType': 'S'},
                        {'AttributeName': 'table_id', 'AttributeType': 'N'},
                        {'AttributeName': 'login_id', 'AttributeType': 'S'},
                        {'AttributeName': 'created_at', 'AttributeType': 'S'},
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'table-id-index',
                            'KeySchema': [
                                {'AttributeName': 'table_id', 'KeyType': 'HASH'},
                                {'AttributeName': 'created_at', 'KeyType': 'RANGE'},
                            ],
                            'Projection': {'ProjectionType': 'ALL'},
                            'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
                        },
                        {
                            'IndexName': 'login-id-index',
                            'KeySchema': [
                                {'AttributeName': 'login_id', 'KeyType': 'HASH'},
                            ],
                            'Projection': {'ProjectionType': 'ALL'},
                            'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                        }
                    ],
                    BillingMode='PROVISIONED',
                    ProvisionedThroughput={'ReadCapacityUnits': 25, 'WriteCapacityUnits': 25}
                )
                
                # 等待表創建完成
                waiter = self.client.get_waiter('table_exists')
                waiter.wait(TableName=self.reservations_table)
                logger.info(f"預訂表 {self.reservations_table} 創建成功")

    def _create_tables_table(self):
        """創建桌位狀態表"""
        try:
            self.client.describe_table(TableName=self.tables_table)
            logger.info(f"桌位表 {self.tables_table} 已存在")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"創建桌位表: {self.tables_table}")
                self.client.create_table(
                    TableName=self.tables_table,
                    KeySchema=[
                        {'AttributeName': 'table_id', 'KeyType': 'HASH'},
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'table_id', 'AttributeType': 'N'},
                    ],
                    BillingMode='PROVISIONED',
                    ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10}
                )
                
                waiter = self.client.get_waiter('table_exists')
                waiter.wait(TableName=self.tables_table)
                logger.info(f"桌位表 {self.tables_table} 創建成功")

    def _create_idempotency_table(self):
        """創建防重複鍵表"""
        try:
            self.client.describe_table(TableName=self.idempotency_table)
            logger.info(f"防重複表 {self.idempotency_table} 已存在")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"創建防重複表: {self.idempotency_table}")
                self.client.create_table(
                    TableName=self.idempotency_table,
                    KeySchema=[
                        {'AttributeName': 'idempotency_key', 'KeyType': 'HASH'},
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'idempotency_key', 'AttributeType': 'S'},
                    ],
                    BillingMode='PROVISIONED',
                    ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                    TimeToLiveSpecification={
                        'AttributeName': 'ttl',
                        'Enabled': True
                    }
                )
                
                waiter = self.client.get_waiter('table_exists')
                waiter.wait(TableName=self.idempotency_table)
                logger.info(f"防重複表 {self.idempotency_table} 創建成功")

    def test_connection(self):
        """測試DynamoDB連接"""
        try:
            self.client.list_tables()
            logger.info("DynamoDB連接成功")
            return True
        except ClientError as e:
            logger.error(f"DynamoDB連接失敗: {e}")
            return False

    # ========== 預訂操作 ==========
    def save_reservation(self, slot_id, reservation_data, date_str=None):
        """保存預訂數據"""
        try:
            # 確保數據包含時間戳
            reservation_data['created_at'] = reservation_data.get('created_at', datetime.now(TAIWAN_TZ).isoformat())
            reservation_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()
            
            # 轉換數字類型為Decimal (DynamoDB要求)
            item = self._convert_to_dynamodb_format(reservation_data)
            item['reservation_id'] = slot_id
            
            # 原子寫入
            self.reservations.put_item(Item=item)
            logger.info(f"預訂數據已保存: {slot_id}")
            return True
            
        except ClientError as e:
            logger.error(f"保存預訂數據失敗: {e}")
            return False

    def get_reservation(self, slot_id, date_str=None):
        """獲取預訂數據"""
        try:
            response = self.reservations.get_item(
                Key={'reservation_id': slot_id}
            )
            
            if 'Item' in response:
                item = self._convert_from_dynamodb_format(response['Item'])
                logger.info(f"成功讀取預訂數據: {slot_id}")
                return item
            else:
                logger.info(f"預訂數據不存在: {slot_id}")
                return None
                
        except ClientError as e:
            logger.error(f"讀取預訂數據失敗: {e}")
            return None

    def delete_reservation(self, slot_id, date_str=None):
        """刪除預訂數據"""
        try:
            self.reservations.delete_item(
                Key={'reservation_id': slot_id}
            )
            logger.info(f"預訂數據已刪除: {slot_id}")
            return True
            
        except ClientError as e:
            logger.error(f"刪除預訂數據失敗: {e}")
            return False

    def update_reservation(self, slot_id, updated_data, date_str=None):
        """更新預訂數據"""
        try:
            # 添加更新時間戳
            updated_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()
            
            # 構建更新表達式
            update_expression = "SET "
            expression_values = {}
            
            for key, value in updated_data.items():
                if key != 'reservation_id':  # 不能更新主鍵
                    update_expression += f"#{key} = :{key}, "
                    expression_values[f":{key}"] = self._convert_value_to_dynamodb(value)
            
            update_expression = update_expression.rstrip(', ')
            expression_names = {f"#{key}": key for key in updated_data.keys() if key != 'reservation_id'}
            
            self.reservations.update_item(
                Key={'reservation_id': slot_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names
            )
            
            logger.info(f"預訂數據已更新: {slot_id}")
            return True
            
        except ClientError as e:
            logger.error(f"更新預訂數據失敗: {e}")
            return False

    def get_all_reservations(self):
        """獲取所有預訂數據 - 高性能掃描"""
        reservations = []
        try:
            # 使用分頁掃描避免超時
            scan_kwargs = {}
            
            while True:
                response = self.reservations.scan(**scan_kwargs)
                
                for item in response.get('Items', []):
                    reservation = self._convert_from_dynamodb_format(item)
                    reservations.append(reservation)
                
                # 檢查是否有更多數據
                if 'LastEvaluatedKey' not in response:
                    break
                    
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                
            logger.info(f"獲取到 {len(reservations)} 筆預訂數據")
            return reservations
            
        except ClientError as e:
            logger.error(f"獲取所有預訂失敗: {e}")
            return []

    def list_reservations_by_date(self, date_str):
        """按日期列出預訂 - 使用GSI優化查詢"""
        try:
            # 這裡我們可以通過created_at來過濾，但DynamoDB的日期查詢需要精確匹配
            # 為了簡化，我們先獲取所有然後過濾
            all_reservations = self.get_all_reservations()
            
            # 日期過濾
            target_date = self._normalize_date(date_str)
            if not target_date:
                return []
                
            filtered = []
            for r in all_reservations:
                created_at = r.get('created_at', '')
                if created_at.startswith(target_date):
                    filtered.append(r)
                    
            logger.info(f"找到 {len(filtered)} 筆預訂數據 ({target_date})")
            return filtered
            
        except Exception as e:
            logger.error(f"按日期列出預訂失敗: {e}")
            return []

    # ========== 桌位操作 ==========
    def get_tables_data(self):
        """獲取桌位數據"""
        try:
            response = self.tables.scan()
            tables_dict = {}
            
            for item in response.get('Items', []):
                table_id = str(int(item['table_id']))
                tables_dict[table_id] = self._convert_from_dynamodb_format(item)
                
            return tables_dict
            
        except ClientError as e:
            logger.error(f"獲取桌位數據失敗: {e}")
            return None

    def save_tables_data(self, tables_data):
        """保存桌位數據"""
        try:
            # 批量寫入桌位數據
            with self.tables.batch_writer() as batch:
                for table_id_str, table_data in tables_data.items():
                    item = self._convert_to_dynamodb_format(table_data)
                    item['table_id'] = int(table_id_str)
                    batch.put_item(Item=item)
                    
            logger.info(f"保存了 {len(tables_data)} 張桌位數據")
            return True
            
        except ClientError as e:
            logger.error(f"保存桌位數據失敗: {e}")
            return False

    def reserve_seats(self, table_id, seats_count):
        """原子性預訂座位"""
        try:
            response = self.tables.update_item(
                Key={'table_id': table_id},
                UpdateExpression='SET seats_left = seats_left - :seats',
                ConditionExpression='seats_left >= :seats',
                ExpressionAttributeValues={':seats': seats_count},
                ReturnValues='UPDATED_NEW'
            )
            
            logger.info(f"成功預訂座位: Table {table_id}, {seats_count} 席")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.warning(f"座位不足: Table {table_id}, 需要 {seats_count} 席")
                return False
            else:
                logger.error(f"預訂座位失敗: {e}")
                return False

    def release_seats(self, table_id, seats_count):
        """原子性釋放座位"""
        try:
            # 先獲取桌位信息
            table_info = self.tables.get_item(Key={'table_id': table_id})
            if 'Item' not in table_info:
                logger.error(f"桌位不存在: {table_id}")
                return False
                
            total_seats = int(table_info['Item'].get('total', 10))
            current_left = int(table_info['Item'].get('seats_left', 0))
            
            new_seats_left = min(total_seats, current_left + seats_count)
            
            self.tables.update_item(
                Key={'table_id': table_id},
                UpdateExpression='SET seats_left = :new_seats',
                ExpressionAttributeValues={':new_seats': new_seats_left}
            )
            
            logger.info(f"成功釋放座位: Table {table_id}, {seats_count} 席")
            return True
            
        except ClientError as e:
            logger.error(f"釋放座位失敗: {e}")
            return False

    # ========== Login ID檢查 ==========
    def check_login_id_exists(self, login_id):
        """檢查login_id是否已存在 - 使用GSI優化"""
        try:
            response = self.reservations.query(
                IndexName='login-id-index',
                KeyConditionExpression='login_id = :login_id',
                ExpressionAttributeValues={':login_id': login_id.lower()},
                Limit=1
            )
            
            exists = len(response.get('Items', [])) > 0
            logger.info(f"Login ID檢查: {login_id} -> {'存在' if exists else '不存在'}")
            return exists
            
        except ClientError as e:
            logger.error(f"檢查login_id失敗: {e}")
            return False

    # ========== 防重複鍵操作 ==========
    def save_idempotency_key(self, key, data):
        """保存防重複鍵 - 24小時TTL"""
        try:
            # TTL設置為24小時後
            ttl = int((datetime.now(TAIWAN_TZ) + timedelta(hours=24)).timestamp())
            
            item = self._convert_to_dynamodb_format(data)
            item['idempotency_key'] = key
            item['ttl'] = ttl
            
            self.idempotency.put_item(Item=item)
            logger.info(f"防重複鍵已保存: {key}")
            return True
            
        except ClientError as e:
            logger.error(f"保存防重複鍵失敗: {e}")
            return False

    def get_idempotency_key(self, key):
        """獲取防重複鍵"""
        try:
            response = self.idempotency.get_item(
                Key={'idempotency_key': key}
            )
            
            if 'Item' in response:
                item = self._convert_from_dynamodb_format(response['Item'])
                # 移除TTL字段
                item.pop('ttl', None)
                logger.info(f"找到防重複鍵: {key}")
                return item
            else:
                return None
                
        except ClientError as e:
            logger.error(f"獲取防重複鍵失敗: {e}")
            return None

    # ========== 工具方法 ==========
    def _normalize_date(self, date_str):
        """日期格式化"""
        if not date_str:
            return None
        ds = str(date_str).strip().replace('/', '-')
        if len(ds) >= 10:
            ds = ds[:10]
        try:
            datetime.strptime(ds, '%Y-%m-%d')
            return ds
        except Exception:
            return None

    def _convert_to_dynamodb_format(self, data):
        """轉換為DynamoDB格式"""
        if isinstance(data, dict):
            return {k: self._convert_value_to_dynamodb(v) for k, v in data.items()}
        return data

    def _convert_from_dynamodb_format(self, data):
        """從DynamoDB格式轉換"""
        if isinstance(data, dict):
            return {k: self._convert_value_from_dynamodb(v) for k, v in data.items()}
        return data

    def _convert_value_to_dynamodb(self, value):
        """轉換單個值為DynamoDB格式"""
        if isinstance(value, float):
            return Decimal(str(value))
        elif isinstance(value, int):
            return value
        return value

    def _convert_value_from_dynamodb(self, value):
        """從DynamoDB格式轉換單個值"""
        if isinstance(value, Decimal):
            return float(value) if value % 1 else int(value)
        return value
