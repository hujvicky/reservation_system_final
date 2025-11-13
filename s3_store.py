# ======================================
# s3_store.py - v11 (快取 + CAS 優化版)
# ======================================
import boto3
import json
import os
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
import logging
import threading
import time

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定義台灣時區 (UTC+8)
TAIWAN_TZ = timezone(timedelta(hours=8))

class S3Store:
    def __init__(self):
        self.VERSION = "4.0-cache-cas-optimized"  # <-- 版本號
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'seat-reservation-data-2025')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

        # (新) 記憶體快取 (用於 get_all_reservations)
        self.all_reservations_cache = None
        self.all_reservations_expiry = 0
        self.all_reservations_lock = threading.Lock() # (新) Single-flight 鎖
        self.CACHE_TTL_SECONDS = 5 # 快取 5 秒

    # -------- (新) 快取控制 --------
    def _clear_all_reservations_cache(self):
        """清除 get_all_reservations 的快取"""
        logger.info("清除 'all_reservations' 快取...")
        self.all_reservations_expiry = 0
        self.all_reservations_cache = None

    # -------- 日期與 Key 輔助 (保持不變) --------
    def _normalize_date(self, date_str):
        if not date_str: return None
        ds = str(date_str).strip().replace('/', '-')
        if len(ds) >= 10: ds = ds[:10]
        try:
            datetime.strptime(ds, '%Y-%m-%d')
            return ds
        except Exception:
            return None

    def _find_date_by_slot(self, slot_id):
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix='reservations/'):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith(f'/{slot_id}.json'):
                        parts = key.split('/')
                        if len(parts) >= 3:
                            return parts[1]
        except ClientError as e:
            logger.error(f"遍歷查找 slot 所在日期失敗: {e}")
        return None

    # -------- 讀寫單筆 (!!! 已優化 !!!) --------
    def save_reservation(self, slot_id, reservation_data, date_str=None):
        """儲存預訂資料到 S3 (!!! 已優化：儲存後清除快取 !!!)"""
        try:
            reservation_data['created_at'] = reservation_data.get('created_at', datetime.now(TAIWAN_TZ).isoformat())
            reservation_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()
            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id) \
                 or datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
            key = f"reservations/{ds}/{slot_id}.json"
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=key,
                Body=json.dumps(reservation_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            logger.info(f"預訂資料已儲存到 S3: {key}")
            self._clear_all_reservations_cache() # (優化) 清除快取
            return True
        except ClientError as e:
            logger.error(f"儲存預訂資料失敗: {e}")
            return False

    def get_reservation(self, slot_id, date_str=None):
        """從 S3 讀取預訂資料 (保持不變)"""
        ds = self._normalize_date(date_str)
        if ds:
            key = f"reservations/{ds}/{slot_id}.json"
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                return json.loads(response['Body'].read().decode('utf-8'))
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"讀取預訂資料失敗: {e}")

        real_ds = self._find_date_by_slot(slot_id)
        if real_ds:
            key = f"reservations/{real_ds}/{slot_id}.json"
            try:
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                return json.loads(response['Body'].read().decode('utf-8'))
            except ClientError as e:
                logger.error(f"讀取預訂資料失敗(跨日期): {e}")
        return None

    def delete_reservation(self, slot_id, date_str=None):
        """從 S3 刪除預訂資料 (!!! 已優化：刪除後清除快取 !!!)"""
        try:
            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id)
            if not ds:
                logger.warning(f"刪除失敗，找不到日期資料夾: slot_id={slot_id}")
                return False
            key = f"reservations/{ds}/{slot_id}.json"
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"預訂資料已刪除: {key}")
            self._clear_all_reservations_cache() # (優化) 清除快取
            return True
        except ClientError as e:
            logger.error(f"刪除預訂資料失敗: {e}")
            return False

    def update_reservation(self, slot_id, updated_data, date_str=None):
        """更新預訂資料 (!!! 已優化：更新後清除快取 !!!)"""
        try:
            existing_data = self.get_reservation(slot_id, date_str)
            if existing_data is None:
                logger.warning(f"預訂資料不存在，無法更新: {slot_id}")
                return False
            existing_data.update(updated_data)
            existing_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()
            # save_reservation 內部會自動清除快取
            return self.save_reservation(slot_id, existing_data, date_str)
        except Exception as e:
            logger.error(f"更新預訂資料失敗: {e}")
            return False

    # -------- 列表查詢 (!!! 已優化 !!!) --------
    def get_all_reservations(self):
        """
        (!!! 關鍵優化 !!!)
        獲取所有預訂資料 (帶 5 秒記憶體快取 + Single-Flight 鎖)
        """
        current_time = time.monotonic()
        
        # 1. 檢查快取是否有效
        if self.all_reservations_expiry > current_time:
            logger.info("從 'all_reservations' 快取提供資料")
            return self.all_reservations_cache

        # 2. 快取失效，嘗試取得鎖 (Single-Flight)
        with self.all_reservations_lock:
            # 3. 取得鎖後，再次檢查快取 (可能在等待時已被其他執行緒更新)
            if self.all_reservations_expiry > current_time:
                logger.info("從 'all_reservations' 快取提供資料 (double-check)")
                return self.all_reservations_cache
            
            # 4. 仍失效，執行昂貴的 S3 查詢
            logger.warning("快取失效！正在執行 S3 'get_all_reservations'...")
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
                
                # 5. 儲存到快取
                self.all_reservations_cache = reservations
                self.all_reservations_expiry = current_time + self.CACHE_TTL_SECONDS
                logger.warning(f"S3 'get_all_reservations' 查詢完成，快取 {len(reservations)} 筆資料")
                return reservations

            except ClientError as e:
                logger.error(f"獲取所有預約失敗: {e}")
                return [] # 發生錯誤時回傳空列表，但不快取

    # -------- 其它原有功能 --------
    def test_connection(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"S3 連線成功: {self.bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"S3 連線失敗: {e}")
            return False

    # ========== (!!! 已優化：CAS 功能 !!!) ==========

    def get_tables_data(self):
        """(舊) 獲取所有桌位資料 (不含 ETag)"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key="tables/tables.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey': return None
            logger.error(f"獲取桌位資料失敗: {e}")
            return None

    def get_tables_data_with_etag(self):
        """(新) 獲取所有桌位資料 (包含 ETag 以供 CAS 使用)"""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key="tables/tables.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            etag = response['ETag']
            return data, etag
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey': return None, None
            logger.error(f"獲取桌位資料 (含ETag) 失敗: {e}")
            return None, None

    def save_tables_data(self, tables_data):
        """(舊) 儲存桌位資料 (不含 CAS)"""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key="tables/tables.json",
                Body=json.dumps(tables_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"儲存桌位資料失敗: {e}")
            return False

    def save_tables_data_cas(self, tables_data, etag):
        """
        (新) 儲存桌位資料 (使用 CAS)
        如果 ETag 不匹配，將會拋出 ClientError (PreconditionFailed)
        """
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="tables/tables.json",
            Body=json.dumps(tables_data, ensure_ascii=False, indent=2),
            ContentType='application/json',
            IfMatch=etag # (!!! 關鍵的 CAS !!!)
        )
        logger.info(f"CAS save_tables_data 成功 (ETag: {etag})")
        return True

    def reserve_seats_cas(self, table_id, seats_count, retries=3):
        """(新) 原子性預訂座位 (帶重試的 CAS 迴圈)"""
        table_key = str(table_id)
        for attempt in range(retries):
            try:
                tables_data, etag = self.get_tables_data_with_etag()
                if not tables_data: return False
                if table_key not in tables_data: return False
                
                if tables_data[table_key]["seats_left"] < seats_count:
                    logger.warning(f"CAS 預訂失敗: 桌號 {table_key} 座位不足")
                    return False

                tables_data[table_key]["seats_left"] -= seats_count
                self.save_tables_data_cas(tables_data, etag)
                logger.info(f"CAS 預訂成功: 桌號 {table_key} 減少 {seats_count} 座位")
                return True # 成功

            except ClientError as e:
                if e.response['Error']['Code'] == 'PreconditionFailed':
                    logger.warning(f"CAS 預訂衝突 (Attempt {attempt + 1}/{retries})，正在重試...")
                    time.sleep(0.05 * (attempt + 1)) # 退避
                    continue # 重試
                else:
                    logger.error(f"CAS 預訂時 S3 錯誤: {e}")
                    return False # 其他 S3 錯誤
            except Exception as e:
                 logger.error(f"CAS 預訂時未知錯誤: {e}")
                 return False
        
        logger.error(f"CAS 預訂失敗: {retries} 次重試後仍衝突")
        return False # 重試耗盡

    def release_seats_cas(self, table_id, seats_count, retries=3):
        """(新) 原子性釋放座位 (帶重試的 CAS 迴圈)"""
        table_key = str(table_id)
        for attempt in range(retries):
            try:
                tables_data, etag = self.get_tables_data_with_etag()
                if not tables_data: return False
                if table_key not in tables_data: return False

                total_seats = tables_data[table_key].get("total", 10)
                new_seats_left = tables_data[table_key]["seats_left"] + seats_count
                
                # 確保不會超過總數
                tables_data[table_key]["seats_left"] = min(new_seats_left, total_seats)
                
                self.save_tables_data_cas(tables_data, etag)
                logger.info(f"CAS 釋放成功: 桌號 {table_key} 增加 {seats_count} 座位")
                return True # 成功

            except ClientError as e:
                if e.response['Error']['Code'] == 'PreconditionFailed':
                    logger.warning(f"CAS 釋放衝突 (Attempt {attempt + 1}/{retries})，正在重試...")
                    time.sleep(0.05 * (attempt + 1)) # 退避
                    continue # 重試
                else:
                    logger.error(f"CAS 釋放時 S3 錯誤: {e}")
                    return False
            except Exception as e:
                 logger.error(f"CAS 釋放時未知錯誤: {e}")
                 return False
        
        logger.error(f"CAS 釋放失敗: {retries} 次重試後仍衝突")
        return False

    def check_login_id_exists(self, login_id):
        """(!!! 已優化 !!!) 檢查 login_id 是否已存在 (受益於快取)"""
        try:
            # (優化) 這裡會呼叫帶快取的 get_all_reservations
            reservations = self.get_all_reservations() 
            return any(r.get("login_id", "").lower() == login_id.lower() for r in reservations)
        except Exception as e:
            logger.error(f"檢查 login_id 失敗: {e}")
            return False # 發生錯誤時，保守地假設不存在

    # -------- 防重複提交 (保持不變) --------
    def save_idempotency_key(self, key, data):
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name, Key=f"idempotency/{key}.json",
                Body=json.dumps(data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"儲存防重複鍵失敗: {e}")
            return False

    def get_idempotency_key(self, key):
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=f"idempotency/{key}.json"
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey': return None
            logger.error(f"獲取防重複鍵失敗: {e}")
            return None
