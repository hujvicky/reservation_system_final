# ======================================
# s3_store.py - 最終修正版 v9
# ======================================
import boto3
import json
import os
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定義台灣時區 (UTC+8)
TAIWAN_TZ = timezone(timedelta(hours=8))

class S3Store:
    def __init__(self):
        self.VERSION = "3.0-fix-delete-bug" # <-- 版本號
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'seat-reservation-data-2025')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

    def _get_reservation_key(self, slot_id, date_str=None):
        """生成預訂資料的 S3 key"""
        if date_str is None:
            date_str = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return f"reservations/{date_str}/{slot_id}.json"

    # (!!! 修正 1 !!!)
    # save_reservation 必須接受 date_str 參數
    def save_reservation(self, slot_id, reservation_data, date_str=None):
        """儲存預訂資料到 S3"""
        try:
            # 確保資料包含時間戳記
            reservation_data['created_at'] = reservation_data.get('created_at', datetime.now(TAIWAN_TZ).isoformat())
            reservation_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()

            # (MODIFIED) 使用傳入的 date_str (如果有的話)
            key = self._get_reservation_key(slot_id, date_str)

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

    # (!!! 修正 2 !!!)
    # update_reservation 必須將 date_str 傳遞給 save_reservation
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
            existing_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()

            # (MODIFIED) 儲存更新後的資料 (傳入 date_str)
            return self.save_reservation(slot_id, existing_data, date_str)

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

    # ========== 新增的方法（注意縮排和 self 參數）==========

    def get_tables_data(self):
        """獲取所有桌位資料"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="tables/tables.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"獲取桌位資料失敗: {e}")
            return None

    def save_tables_data(self, tables_data):
        """儲存桌位資料"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key="tables/tables.json",
                Body=json.dumps(tables_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"儲存桌位資料失敗: {e}")
            return False

    def reserve_seats(self, table_id, seats_count):
        """預訂座位（原子操作）"""
        try:
            tables_data = self.get_tables_data()
            if not tables_data:
                return False

            table_key = str(table_id)
            if table_key not in tables_data:
                return False

            if tables_data[table_key]["seats_left"] < seats_count:
                return False

            tables_data[table_key]["seats_left"] -= seats_count
            return self.save_tables_data(tables_data)

        except Exception as e:
            logger.error(f"預訂座位失敗: {e}")
            return False

    def release_seats(self, table_id, seats_count):
        """釋放座位 (!!! 已升級 !!!)"""
        try:
            tables_data = self.get_tables_data()
            if not tables_data:
                return False

            table_key = str(table_id)
            if table_key not in tables_data:
                return False

            # (NEW) 增加保護，避免座位數超過總數
            total_seats = tables_data[table_key].get("total", 10)
            new_seats_left = tables_data[table_key]["seats_left"] + seats_count
            
            if new_seats_left > total_seats:
                logger.warning(f"座位數修正：Table {table_id} 釋放 {seats_count} 後座位數 ({new_seats_left}) 超過總數 {total_seats}。將設定為 {total_seats}。")
                tables_data[table_key]["seats_left"] = total_seats
            else:
                tables_data[table_key]["seats_left"] = new_seats_left
                
            return self.save_tables_data(tables_data)

        except Exception as e:
            logger.error(f"釋放座位失敗: {e}")
            return False

    def check_login_id_exists(self, login_id):
        """檢查 login_id 是否已存在"""
        try:
            reservations = self.get_all_reservations()
            return any(r.get("login_id", "").lower() == login_id.lower() for r in reservations)
        except Exception as e:
            logger.error(f"檢查 login_id 失敗: {e}")
            return False

    def get_all_reservations(self):
        """獲取所有預訂資料 (已修正分頁問題)"""
        reservations = []
        try:
            # 使用 Paginator 處理分頁，確保讀取所有檔案
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix='reservations/'
            )

            # 遍歷所有頁面
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # 檢查 Key 是否為 JSON 檔案，並排除目錄本身 (Key 結尾為 / 的情況)
                        if obj['Key'].endswith('.json'):
                            # 讀取物件內容
                            try:
                                obj_response = self.s3_client.get_object(
                                    Bucket=self.bucket_name,
                                    Key=obj['Key']
                                )
                                reservation_data = json.loads(obj_response['Body'].read().decode('utf-8'))
                                reservations.append(reservation_data)
                            except Exception as e:
                                logger.warning(f"無法讀取預約檔案 {obj['Key']}: {e}")
            
            return reservations

        except ClientError as e:
            logger.error(f"獲取所有預約失敗: {e}")
            return []

    def save_idempotency_key(self, key, data):
        """儲存防重複鍵"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"idempotency/{key}.json",
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"儲存防重複鍵失敗: {e}")
            return False

    def get_idempotency_key(self, key):
        """獲取防重複鍵"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"idempotency/{key}.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"獲取防重複鍵失敗: {e}")
            return None
