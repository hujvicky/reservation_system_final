import boto3
import json
import os
from datetime import datetime
from botocore.exceptions import ClientError
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Store:
    def __init__(self):
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'seat-reservation-data-2025')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

    def _get_reservation_key(self, slot_id, date_str=None):
        """生成預訂資料的 S3 key"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        return f"reservations/{date_str}/{slot_id}.json"

    def save_reservation(self, slot_id, reservation_data):
        """儲存預訂資料到 S3"""
        try:
            # 確保資料包含時間戳記
            reservation_data['created_at'] = datetime.now().isoformat()
            reservation_data['updated_at'] = datetime.now().isoformat()

            key = self._get_reservation_key(slot_id)

            # 將資料轉換為 JSON 並上傳到 S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(reservation_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )

            logger.info(f"預訂資料已儲存到 S3: {key}")
            return True

        except ClientError as e:
            logger.error(f"儲存預訂資料失敗: {e}")
            return False

    def get_reservation(self, slot_id, date_str=None):
        """從 S3 讀取預訂資料"""
        try:
            key = self._get_reservation_key(slot_id, date_str)

            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )

            data = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"成功讀取預訂資料: {key}")
            return data

        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"預訂資料不存在: {key}")
                return None
            else:
                logger.error(f"讀取預訂資料失敗: {e}")
                return None

    def delete_reservation(self, slot_id, date_str=None):
        """從 S3 刪除預訂資料"""
        try:
            key = self._get_reservation_key(slot_id, date_str)

            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )

            logger.info(f"預訂資料已刪除: {key}")
            return True

        except ClientError as e:
            logger.error(f"刪除預訂資料失敗: {e}")
            return False

    def list_reservations_by_date(self, date_str):
        """列出指定日期的所有預訂"""
        try:
            prefix = f"reservations/{date_str}/"

            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            reservations = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # 提取 slot_id
                    key = obj['Key']
                    slot_id = key.split('/')[-1].replace('.json', '')

                    # 讀取預訂資料
                    reservation_data = self.get_reservation(slot_id, date_str)
                    if reservation_data:
                        reservation_data['slot_id'] = slot_id
                        reservations.append(reservation_data)

            logger.info(f"找到 {len(reservations)} 筆預訂資料 ({date_str})")
            return reservations

        except ClientError as e:
            logger.error(f"列出預訂資料失敗: {e}")
            return []

    def update_reservation(self, slot_id, updated_data, date_str=None):
        """更新預訂資料"""
        try:
            # 先讀取現有資料
            existing_data = self.get_reservation(slot_id, date_str)
            if existing_data is None:
                logger.warning(f"預訂資料不存在，無法更新: {slot_id}")
                return False

            # 合併資料
            existing_data.update(updated_data)
            existing_data['updated_at'] = datetime.now().isoformat()

            # 儲存更新後的資料
            return self.save_reservation(slot_id, existing_data)

        except Exception as e:
            logger.error(f"更新預訂資料失敗: {e}")
            return False

    def test_connection(self):
        """測試 S3 連線"""
        try:
            # 嘗試列出 bucket
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"S3 連線成功: {self.bucket_name}")
            return True

        except ClientError as e:
            logger.error(f"S3 連線失敗: {e}")
            return False
