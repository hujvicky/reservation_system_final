# ======================================
# s3_store.py - v4.5 (最佳努力 CAS + 5秒快取 + 中文日誌)
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

# 台灣時區 (UTC+8)
TAIWAN_TZ = timezone(timedelta(hours=8))

class S3Store:
    def __init__(self):
        self.VERSION = "4.5-cas-best-effort"
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'seat-reservation-data-2025')
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

        # get_all_reservations 記憶體快取（5 秒）
        self.all_reservations_cache = None
        self.all_reservations_expiry = 0.0
        self.all_reservations_lock = threading.Lock()
        self.CACHE_TTL_SECONDS = 5

    # -------------- 工具：清除快取 --------------
    def _clear_all_reservations_cache(self):
        logger.info("清除 'all_reservations' 快取…")
        self.all_reservations_cache = None
        self.all_reservations_expiry = 0.0

    # -------------- 工具：日期處理 --------------
    def _normalize_date(self, date_str):
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

    # -------------- 單筆讀寫（預約檔） --------------
    def save_reservation(self, slot_id, reservation_data, date_str=None):
        """儲存預訂資料到 S3（儲存後清除 all_reservations 快取）"""
        try:
            reservation_data['created_at'] = reservation_data.get('created_at', datetime.now(TAIWAN_TZ).isoformat())
            reservation_data['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()

            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id) \
                 or datetime.now(TAIWAN_TZ).strftime('%Y-%m-%d')
            key = f"reservations/{ds}/{slot_id}.json"

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(reservation_data, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            logger.info(f"預訂資料已儲存至 S3: {key}")
            self._clear_all_reservations_cache()
            return True
        except ClientError as e:
            logger.error(f"儲存預訂資料失敗: {e}")
            return False

    def get_reservation(self, slot_id, date_str=None):
        """讀取單一預訂資料；支援 date_str 為 None（自動尋找）"""
        ds = self._normalize_date(date_str)
        if ds:
            key = f"reservations/{ds}/{slot_id}.json"
            try:
                resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                return json.loads(resp['Body'].read().decode('utf-8'))
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey':
                    logger.error(f"讀取預訂資料失敗: {e}")

        real_ds = self._find_date_by_slot(slot_id)
        if real_ds:
            key = f"reservations/{real_ds}/{slot_id}.json"
            try:
                resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                return json.loads(resp['Body'].read().decode('utf-8'))
            except ClientError as e:
                logger.error(f"讀取預訂資料失敗(跨日期): {e}")

        return None

    def delete_reservation(self, slot_id, date_str=None):
        """刪除單一預訂資料；刪除後清除 all_reservations 快取"""
        try:
            ds = self._normalize_date(date_str) or self._find_date_by_slot(slot_id)
            if not ds:
                logger.warning(f"刪除失敗：找不到日期資料夾 slot_id={slot_id}")
                return False

            key = f"reservations/{ds}/{slot_id}.json"
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"預訂資料已刪除: {key}")
            self._clear_all_reservations_cache()
            return True
        except ClientError as e:
            logger.error(f"刪除預訂資料失敗: {e}")
            return False

    def update_reservation(self, slot_id, updated_data, date_str=None):
        """更新預訂資料；更新後清除 all_reservations 快取"""
        try:
            existing = self.get_reservation(slot_id, date_str)
            if existing is None:
                logger.warning(f"預訂資料不存在，無法更新: {slot_id}")
                return False

            existing.update(updated_data)
            existing['updated_at'] = datetime.now(TAIWAN_TZ).isoformat()
            return self.save_reservation(slot_id, existing, date_str)
        except Exception as e:
            logger.error(f"更新預訂資料失敗: {e}")
            return False

    # -------------- 列表查詢（含 5 秒快取 + Single-Flight） --------------
    def get_all_reservations(self):
        """
        取得所有預訂資料（昂貴 S3 遍歷 + 讀檔），加入 5 秒記憶體快取與鎖避免併發重複打 S3。
        """
        now = time.monotonic()
        if self.all_reservations_expiry > now:
            logger.info("從 'all_reservations' 快取提供資料")
            return self.all_reservations_cache

        with self.all_reservations_lock:
            if self.all_reservations_expiry > now:
                logger.info("從 'all_reservations' 快取提供資料 (double-check)")
                return self.all_reservations_cache

            logger.warning("快取失效！正在執行 S3 'get_all_reservations'…")
            reservations = []
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=self.bucket_name, Prefix='reservations/')
                for page in pages:
                    for obj in page.get('Contents', []):
                        key = obj['Key']
                        if not key.endswith('.json'):
                            continue
                        try:
                            obj_resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                            data = json.loads(obj_resp['Body'].read().decode('utf-8'))
                            reservations.append(data)
                        except Exception as e:
                            logger.warning(f"無法讀取預約檔案 {key}: {e}")

                self.all_reservations_cache = reservations
                self.all_reservations_expiry = now + self.CACHE_TTL_SECONDS
                logger.warning(f"S3 'get_all_reservations' 完成，快取 {len(reservations)} 筆")
                return reservations
            except ClientError as e:
                logger.error(f"獲取所有預約失敗: {e}")
                return []

    # -------------- 其它 --------------
    def test_connection(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"S3 連線成功: {self.bucket_name}")
            return True
        except ClientError as e:
            logger.error(f"S3 連線失敗: {e}")
            return False

    # ========== tables.json 作業（最佳努力 CAS） ==========

    def _head_tables_etag(self):
        """讀取 tables.json 的當前 ETag（去除引號），不存在時回傳 None"""
        try:
            h = self.s3_client.head_object(Bucket=self.bucket_name, Key="tables/tables.json")
            return h["ETag"].strip('"')
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey"):
                return None
            raise

    def get_tables_data(self):
        """獲取所有桌位資料（不含 ETag）"""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket_name, Key="tables/tables.json")
            return json.loads(resp['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"獲取桌位資料失敗: {e}")
            return None

    def get_tables_data_with_etag(self):
        """獲取桌位資料 + ETag（ETag 已去除引號）"""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket_name, Key="tables/tables.json")
            data = json.loads(resp['Body'].read().decode('utf-8'))
            etag = resp['ETag'].strip('"')
            return data, etag
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None, None
            logger.error(f"獲取桌位資料(含ETag) 失敗: {e}")
            return None, None

    def save_tables_data(self, tables_data):
        """儲存桌位資料（不含 CAS）"""
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

    def save_tables_data_cas(self, tables_data, etag_before):
        """
        最佳努力 CAS：
        1) 再次 head 取得當前 ETag
        2) 若與 etag_before 不同 -> 拋 ClientError(PreconditionFailed)
        3) 若相同 -> 直接 put（S3 PutObject 不支援 IfMatch，只能靠程式層檢查）
        """
        logger.info(f"CAS 檢查：etag_before={etag_before}")
        etag_now = self._head_tables_etag()
        # 檔案首次建立（etag_none）時允許寫入；若有 etag 就比對
        if etag_before and etag_now and etag_now != etag_before:
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed", "Message": "ETag changed"}}, "PutObject"
            )

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key="tables/tables.json",
            Body=json.dumps(tables_data, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )
        logger.info("CAS 寫入成功")
        return True

    def reserve_seats_cas(self, table_id, seats_count, retries=5):
        """
        原子性預訂座位（最佳努力）：讀 -> 算 -> 再 head 比對 -> put；若衝突則退避重試。
        """
        key = str(table_id)
        for i in range(retries):
            try:
                etag_before = self._head_tables_etag()
                obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="tables/tables.json")
                tables = json.loads(obj["Body"].read().decode("utf-8"))

                if key not in tables:
                    return False
                curr_left = int(tables[key].get("seats_left", 0))
                need = int(seats_count)
                if curr_left < need:
                    logger.warning(f"CAS 預訂失敗：桌號 {key} 座位不足（剩 {curr_left}，需 {need}）")
                    return False

                tables[key]["seats_left"] = curr_left - need
                self.save_tables_data_cas(tables, etag_before)
                logger.info(f"CAS 預訂成功：桌號 {key} 扣 {need} -> 剩 {tables[key]['seats_left']}")
                return True

            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "PreconditionFailed":
                    logger.warning(f"CAS 預訂衝突 (Attempt {i+1}/{retries})，退避重試…")
                    time.sleep(0.05 * (i + 1))
                    continue
                logger.error(f"reserve_seats_cas S3 錯誤: {e}")
                return False
            except Exception as e:
                logger.error(f"reserve_seats_cas 未知錯誤: {e}")
                return False

        logger.warning("reserve_seats_cas 重試耗盡")
        return False

    def release_seats_cas(self, table_id, seats_count, retries=5):
        """
        原子性釋放座位（最佳努力）：讀 -> 算 -> 再 head 比對 -> put；若衝突則退避重試。
        """
        key = str(table_id)
        for i in range(retries):
            try:
                etag_before = self._head_tables_etag()
                obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="tables/tables.json")
                tables = json.loads(obj["Body"].read().decode("utf-8"))

                if key not in tables:
                    return False

                total = int(tables[key].get("total", 10))
                curr_left = int(tables[key].get("seats_left", 0))
                add = int(seats_count)
                new_left = min(curr_left + add, total)
                tables[key]["seats_left"] = new_left

                self.save_tables_data_cas(tables, etag_before)
                logger.info(f"CAS 釋放成功：桌號 {key} 加 {add} -> 剩 {new_left}")
                return True

            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "PreconditionFailed":
                    logger.warning(f"CAS 釋放衝突 (Attempt {i+1}/{retries})，退避重試…")
                    time.sleep(0.05 * (i + 1))
                    continue
                logger.error(f"release_seats_cas S3 錯誤: {e}")
                return False
            except Exception as e:
                logger.error(f"release_seats_cas 未知錯誤: {e}")
                return False

        logger.warning("release_seats_cas 重試耗盡")
        return False

    # -------------- login_id 防重複查詢 --------------
    def check_login_id_exists(self, login_id):
        """檢查 login_id 是否已存在（使用 get_all_reservations 快取）"""
        try:
            reservations = self.get_all_reservations()
            target = str(login_id).lower()
            return any((r.get("login_id", "") or "").lower() == target for r in reservations)
        except Exception as e:
            logger.error(f"檢查 login_id 失敗: {e}")
            # 發生錯誤時保守地回 False（讓外層自行決定是否阻擋）
            return False

    # -------------- Idempotency Key --------------
    def save_idempotency_key(self, key, data):
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
        try:
            resp = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"idempotency/{key}.json"
            )
            return json.loads(resp['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            logger.error(f"獲取防重複鍵失敗: {e}")
            return None
