"""
موديول العروض المادية والتحديث التزايدي (Materialized Views & Incremental Refresh)
مقرر البيانات الضخمة - العملي | المرحلة النهائية (Phase 2)

يوفر:
1. إنشاء وإدارة مجموعتي العروض المادية:
   - daily_sales_summary: ملخص المبيعات اليومية
   - top_products_summary: ملخص مبيعات المنتجات الكبرى
2. محرك التحديث التزايدي الذكي (Incremental Refresh Engine):
   - تتبع علامة الماء الزمنية (Watermark) في مجموعة 'mv_metadata'.
   - معالجة السجلات الجديدة فقط منذ آخر تحديث دون إعادة فحص أو معالجة كامل البيانات القديمة.
   - استخدام عمليات التحديث الذري ($inc و upsert) لتعديل المجاميع التراكمية بكفاءة فائقة O(ΔN).
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo import UpdateOne, ASCENDING
from pymongo.database import Database

# إعداد المسار
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database

# أسماء مجموعات العروض المادية ومجموعة التتبع
DAILY_SALES_MV = "daily_sales_summary"
TOP_PRODUCTS_MV = "top_products_summary"
MV_METADATA_COLLECTION = "mv_metadata"


def get_watermark(db: Database, view_name: str) -> Optional[str]:
    """استرجاع علامة الماء (آخر تاريخ تم تحديثه) للعرض المادي."""
    meta = db[MV_METADATA_COLLECTION].find_one({"_id": view_name})
    if meta and "last_watermark" in meta:
        return meta["last_watermark"]
    return None


def set_watermark(db: Database, view_name: str, watermark: str, records_count: int, refresh_type: str = "incremental"):
    """تحديث علامة الماء وإحصائيات التحديث في مجموعة البيانات الوصفية."""
    db[MV_METADATA_COLLECTION].update_one(
        {"_id": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_watermark": watermark,
                "last_refresh_time": datetime.now(timezone.utc).isoformat(),
                "last_refresh_type": refresh_type,
                "last_processed_records": records_count,
            }
        },
        upsert=True
    )


def initialize_views_indexes(db: Database):
    """تهيئة الفهارس الأساسية لمجموعات العروض المادية ومجموعة التتبع."""
    db[DAILY_SALES_MV].create_index([("date", ASCENDING)], unique=True)
    db[TOP_PRODUCTS_MV].create_index([("sku", ASCENDING)], unique=True)
    db[TOP_PRODUCTS_MV].create_index([("total_revenue", -1)])
    db[DAILY_SALES_MV].create_index([("date", -1)])


def refresh_daily_sales_mv(db: Database, batch_limit: int = 50000, force_full: bool = False) -> Dict[str, Any]:
    """تنفيذ التحديث التزايدي للعرض المادي: daily_sales_summary."""
    col_validated = db[VALIDATED_COLLECTION]
    col_mv = db[DAILY_SALES_MV]
    
    watermark = None if force_full else get_watermark(db, DAILY_SALES_MV)
    
    # 1. إعداد شرط التصفية التزايدي
    match_filter: Dict[str, Any] = {"status": {"$nin": ["ملغي"]}}
    if watermark:
        # فحص إما validated_at أو order_date
        match_filter["order_date"] = {"$gt": watermark}
        
    # البحث عن السجلات الجديدة
    sample_docs = list(col_validated.find(match_filter, {"order_date": 1, "total_amount": 1}).sort("order_date", 1).limit(batch_limit))
    
    if not sample_docs:
        return {
            "view": DAILY_SALES_MV,
            "status": "already_up_to_date",
            "message": "لا توجد سجلات جديدة للمعالجة التزايدية.",
            "processed_count": 0,
            "current_watermark": watermark
        }
        
    latest_order_date = sample_docs[-1]["order_date"]
    
    # تجميع محلي سريع لتخفيف العبء على الشبكة
    day_aggregates: Dict[str, Dict[str, Any]] = {}
    for doc in sample_docs:
        odate = str(doc.get("order_date", ""))[:10]  # استخراج YYYY-MM-DD
        amt = float(doc.get("total_amount", 0.0))
        if odate not in day_aggregates:
            day_aggregates[odate] = {"revenue": 0.0, "count": 0}
        day_aggregates[odate]["revenue"] += amt
        day_aggregates[odate]["count"] += 1
        
    # تحديث تزايدي في MongoDB باستخدام UpdateOne
    bulk_ops = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for odate, vals in day_aggregates.items():
        bulk_ops.append(
            UpdateOne(
                {"_id": odate},
                {
                    "$inc": {
                        "total_revenue": vals["revenue"],
                        "order_count": vals["count"]
                    },
                    "$set": {
                        "date": odate,
                        "last_updated": now_iso
                    }
                },
                upsert=True
            )
        )
        
    if bulk_ops:
        col_mv.bulk_write(bulk_ops, ordered=False)
        
    # حساب أو تحديث avg_order_value للأيام المعدلة
    for odate in day_aggregates.keys():
        doc = col_mv.find_one({"_id": odate})
        if doc and doc.get("order_count", 0) > 0:
            avg_val = round(doc["total_revenue"] / doc["order_count"], 2)
            col_mv.update_one({"_id": odate}, {"$set": {"avg_order_value": avg_val, "total_revenue": round(doc["total_revenue"], 2)}})
            
    # حفظ علامة الماء الجديدة
    set_watermark(db, DAILY_SALES_MV, latest_order_date, len(sample_docs), "full" if force_full else "incremental")
    
    return {
        "view": DAILY_SALES_MV,
        "status": "success",
        "mode": "full" if force_full else "incremental",
        "processed_count": len(sample_docs),
        "days_updated": len(day_aggregates),
        "new_watermark": latest_order_date,
        "last_refresh_time": now_iso
    }


def refresh_top_products_mv(db: Database, batch_limit: int = 50000, force_full: bool = False) -> Dict[str, Any]:
    """تنفيذ التحديث التزايدي للعرض المادي: top_products_summary."""
    col_validated = db[VALIDATED_COLLECTION]
    col_mv = db[TOP_PRODUCTS_MV]
    
    watermark = None if force_full else get_watermark(db, TOP_PRODUCTS_MV)
    
    match_filter: Dict[str, Any] = {"status": {"$nin": ["ملغي"]}}
    if watermark:
        match_filter["order_date"] = {"$gt": watermark}
        
    sample_docs = list(col_validated.find(match_filter, {"order_date": 1, "items": 1}).sort("order_date", 1).limit(batch_limit))
    
    if not sample_docs:
        return {
            "view": TOP_PRODUCTS_MV,
            "status": "already_up_to_date",
            "message": "لا توجد سجلات جديدة للمعالجة التزايدية.",
            "processed_count": 0,
            "current_watermark": watermark
        }
        
    latest_order_date = sample_docs[-1]["order_date"]
    
    product_aggregates: Dict[str, Dict[str, Any]] = {}
    for doc in sample_docs:
        items = doc.get("items", [])
        for itm in items:
            sku = itm.get("sku")
            if not sku:
                continue
            name = itm.get("name", "غير محدد")
            qty = int(itm.get("qty", 1))
            total = float(itm.get("total", 0.0))
            
            if sku not in product_aggregates:
                product_aggregates[sku] = {
                    "product_name": name,
                    "total_qty_sold": 0,
                    "total_revenue": 0.0,
                    "order_count": 0
                }
            product_aggregates[sku]["total_qty_sold"] += qty
            product_aggregates[sku]["total_revenue"] += total
            product_aggregates[sku]["order_count"] += 1
            
    bulk_ops = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for sku, pdata in product_aggregates.items():
        bulk_ops.append(
            UpdateOne(
                {"_id": sku},
                {
                    "$inc": {
                        "total_qty_sold": pdata["total_qty_sold"],
                        "total_revenue": pdata["total_revenue"],
                        "order_count": pdata["order_count"]
                    },
                    "$set": {
                        "sku": sku,
                        "product_name": pdata["product_name"],
                        "last_updated": now_iso
                    }
                },
                upsert=True
            )
        )
        
    if bulk_ops:
        col_mv.bulk_write(bulk_ops, ordered=False)
        
    # تقريب الأرقام العشرية للإيرادات
    for sku in product_aggregates.keys():
        doc = col_mv.find_one({"_id": sku})
        if doc:
            col_mv.update_one({"_id": sku}, {"$set": {"total_revenue": round(doc.get("total_revenue", 0.0), 2)}})
            
    set_watermark(db, TOP_PRODUCTS_MV, latest_order_date, len(sample_docs), "full" if force_full else "incremental")
    
    return {
        "view": TOP_PRODUCTS_MV,
        "status": "success",
        "mode": "full" if force_full else "incremental",
        "processed_count": len(sample_docs),
        "products_updated": len(product_aggregates),
        "new_watermark": latest_order_date,
        "last_refresh_time": now_iso
    }


def refresh_all_materialized_views(db: Optional[Database] = None, force_full: bool = False) -> Dict[str, Any]:
    """تحديث جميع العروض المادية بشكل تزايدي موحد."""
    if db is None:
        db = get_database()
        
    initialize_views_indexes(db)
    
    start_time = datetime.now(timezone.utc)
    res_daily = refresh_daily_sales_mv(db, force_full=force_full)
    res_products = refresh_top_products_mv(db, force_full=force_full)
    duration = (datetime.now(timezone.utc) - start_time).total_seconds()
    
    return {
        "status": "success",
        "refresh_timestamp": start_time.isoformat(),
        "total_duration_seconds": round(duration, 3),
        "views": {
            DAILY_SALES_MV: res_daily,
            TOP_PRODUCTS_MV: res_products
        }
    }


def get_materialized_view_data(view_name: str, limit: int = 20, db: Optional[Database] = None) -> Dict[str, Any]:
    """استرجاع محتوى عرض مادي معين مع بيانات التعريف."""
    if db is None:
        db = get_database()
        
    if view_name not in (DAILY_SALES_MV, TOP_PRODUCTS_MV):
        raise ValueError(f"العرض المادي '{view_name}' غير مدعوم. المتاح: {[DAILY_SALES_MV, TOP_PRODUCTS_MV]}")
        
    col = db[view_name]
    sort_field = "date" if view_name == DAILY_SALES_MV else "total_revenue"
    cursor = col.find({}, {"_id": 0}).sort(sort_field, -1).limit(limit)
    data = list(cursor)
    
    meta = db[MV_METADATA_COLLECTION].find_one({"_id": view_name}, {"_id": 0})
    
    return {
        "view_name": view_name,
        "total_returned": len(data),
        "metadata": meta,
        "data": data
    }
