# ======================================
# s3_store.py - 最終修正版 v10 (date-auto-fix)
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
        self.VERSION = "3.1-date-auto-fix"  # <-- 版本號
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'seat-reservation-data-2025')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

    # -------- 日期與 Key 輔助 --------
    def _normalize_date(self, date_str):
        """
        將多種格式正規化為 'YYYY-MM-DD'；若無法判讀則回傳 None
        可處理：'2025/10/27 08:38:22', '2025/10/27', '2025-10-27T08:38:22' 等
        """
        if not date_str:
            return None
        ds = str(date_str).strip().replace('/', '-')
        if len(ds) >= 10:
            ds = ds[:10]
        try:
            # 驗證
            datetime.strptime(ds, '%Y-%m-%d')
            return ds
        except Exception:
            return None

    def _find_date_by_slot(self, slot_id):
        """
        當未知或傳錯 date_str 時，遍歷 reservations/*/slot_id.json 找出實際日期資料夾
        回傳 'YYYY-MM-DD' 或 None
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix='reservations/'):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith(f'/{slot_id}.json'):
                        # e.g. reservations/2025-10-27/abcd.json
                        parts = key.split('/')
                        if len(parts) >= 3:
                            return parts[1]
        except ClientError as e:
            logger.error(f"遍歷查找 slot 所在日期失敗: {e}")
        return None

    def _get_reservation_key(self, slot_id, date_str=None):
        """生成預訂資料的 S3 key（會先正規化日期）"""
        ds = self._normalize_date(date_str)
        if ds is None:
            ds = datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
        return f"reservations/{ds}/{slot_id}.json"

    # -------- 讀寫單筆 --------
    def save_reservation(self, slot_id, reservation_data, date_str=None):
        """儲存預訂資料到 S3（自動修正日期；若沒帶日期會先嘗試找舊檔的日期）"""
        try:
            # 確保資料包含時間戳記
            reservation_data['created_at'] = reservation_data.get('created_at', datetime.now(TAIWAN_TZ).isoformat())
            reservation_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()

            # 優先使用正規化後的 date_str；若沒有，嘗試找出舊檔所在日期；仍無 → 今日
            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id) \
                 or datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
            key = f"reservations/{ds}/{slot_id}.json"

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
        """從 S3 讀取預訂資料（會自動嘗試修正日期與跨日期搜尋）"""
        # 第一次：依傳入日期（若可正規化）
        ds = self._normalize_date(date_str)
        if ds:
            key = f"reservations/{ds}/{slot_id}.json"
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                logger.info(f"成功讀取預訂資料: {key}")
                return data
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"讀取預訂資料失敗: {e}")

        # 第二次：跨日期搜尋 slot 檔
        real_ds = self._find_date_by_slot(slot_id)
        if real_ds:
            key = f"reservations/{real_ds}/{slot_id}.json"
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                logger.info(f"成功讀取預訂資料(跨日期): {key}")
                return data
            except ClientError as e:
                logger.error(f"讀取預訂資料失敗(跨日期): {e}")

        logger.info(f"預訂資料不存在: slot_id={slot_id}, date={date_str}")
        return None

    def delete_reservation(self, slot_id, date_str=None):
        """從 S3 刪除預訂資料（會自動修正日期與跨日期搜尋）"""
        try:
            # 先試使用傳入日期；失敗時自動尋找實際日期
            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id)
            if not ds:
                logger.warning(f"刪除失敗，找不到日期資料夾: slot_id={slot_id}")
                return False

            key = f"reservations/{ds}/{slot_id}.json"
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"預訂資料已刪除: {key}")
            return True

        except ClientError as e:
            logger.error(f"刪除預訂資料失敗: {e}")
            return False

    # -------- 列表查詢 --------
    def list_reservations_by_date(self, date_str):
        """列出指定日期的所有預訂（允許傳入 '2025/11/07 10:10:26' 等格式）"""
        try:
            ds = self._normalize_date(date_str)
            if not ds:
                logger.warning(f"list_reservations_by_date：無法辨識日期，輸入={date_str}")
                return []

            prefix = f"reservations/{ds}/"
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)

            reservations = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if not key.endswith('.json'):
                        continue
                    slot_id = key.split('/')[-1].replace('.json', '')
                    reservation_data = self.get_reservation(slot_id, ds)
                    if reservation_data:
                        reservation_data['slot_id'] = slot_id
                        reservations.append(reservation_data)

            logger.info(f"找到 {len(reservations)} 筆預訂資料 ({ds})")
            return reservations

        except ClientError as e:
            logger.error(f"列出預訂資料失敗: {e}")
            return []

    # -------- 更新 --------
    def update_reservation(self, slot_id, updated_data, date_str=None):
        """更新預訂資料（日期自動修正；若沒帶正確日期仍可更新舊檔）"""
        try:
            existing_data = self.get_reservation(slot_id, date_str)
            if existing_data is None:
                logger.warning(f"預訂資料不存在，無法更新: {slot_id}")
                return False

            existing_data.update(updated_data)
            existing_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()

            # 儲存時會自動決定正確的日期資料夾
            return self.save_reservation(slot_id, existing_data, date_str)

        except Exception as e:
            logger.error(f"更新預訂資料失敗: {e}")
            return False

    # -------- 其它原有功能 --------
    def test_connection(self):
        """測試 S3 連線"""
        try:
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

            total_seats = tables_data[table_key].get("total", 10)
            new_seats_left = tables_data[table_key]["seats_left"] + seats_count
            
            if new_seats_left > total_seats:
                logger.warning(
                    f"座位數修正：Table {table_id} 釋放 {seats_count} 後座位數 ({new_seats_left}) 超過總數 {total_seats}。將設定為 {total_seats}。"
                )
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
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix='reservations/'
            )
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.json'):
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
