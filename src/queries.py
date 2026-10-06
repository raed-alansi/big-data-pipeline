"""
موديول الاستعلامات العملية (Practical Queries)
مقرر البيانات الضخمة - العملي | المرحلة النهائية (Phase 2)

يوفر 5 استعلامات عملية مخصصة لبيانات الطلبات المقبولة (orders_validated).
تم تصميم كل استعلام لخدمة حالات استخدام واقعية وربطه بفهارس مخصصة لضمان السرعة والكفاءة.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pymongo.database import Database
from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


def query_customer_orders(
    db: Database,
    customer_id: str = "CUST-1001",
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """استعلام 1: استرجاع جميع طلبات عميل محدد مرتبة زمنياً من الأحدث إلى الأقدم.
    
    الفهرس الخادم: idx_customer_id على ('customer.customer_id', 1)
    السبب والأثر: تمكين العميل من معاينة سجل طلباته فورياً دون فحص كامل المجموعة.
    """
    col = db[VALIDATED_COLLECTION]
    query_filter = {"customer.customer_id": customer_id}
    cursor = col.find(query_filter).sort("order_date", -1).skip(skip).limit(limit)
    results = list(cursor)
    
    # تنظيف معرفات BSON لتكون قابلة للتسلسل بتنسيق JSON
    for doc in results:
        doc["_id"] = str(doc["_id"])
        
    return {
        "query_name": "customer_orders",
        "description": "استرجاع جميع طلبات عميل محدد مرتبة تنازلياً حسب التاريخ",
        "filter": query_filter,
        "count": len(results),
        "data": results,
    }


def query_orders_by_status_and_date(
    db: Database,
    status: str = "تم التسليم",
    start_date: Optional[str] = "2025-01-01",
    end_date: Optional[str] = "2025-12-31",
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """استعلام 2: استرجاع الطلبات بحالة معينة خلال فترة زمنية محددة.
    
    الفهرس الخادم: idx_status_order_date المركب على [('status', 1), ('order_date', -1)]
    السبب والأثر: تتبع دورة الطلبات (مثل المسلمة أو المرتجعة) خلال مدى زمني لمراقبة العمليات اللوجستية.
    """
    col = db[VALIDATED_COLLECTION]
    query_filter: Dict[str, Any] = {"status": status}
    
    date_filter: Dict[str, Any] = {}
    if start_date:
        date_filter["$gte"] = start_date
    if end_date:
        date_filter["$lte"] = end_date
    if date_filter:
        query_filter["order_date"] = date_filter

    cursor = col.find(query_filter).sort("order_date", -1).skip(skip).limit(limit)
    results = list(cursor)
    
    for doc in results:
        doc["_id"] = str(doc["_id"])
        
    return {
        "query_name": "orders_by_status_and_date",
        "description": "استرجاع الطلبات بحالة معينة خلال فترة زمنية محددة",
        "filter": query_filter,
        "count": len(results),
        "data": results,
    }


def query_city_high_value_orders(
    db: Database,
    city: str = "صنعاء",
    min_amount: float = 200000.0,
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """استعلام 3: استرجاع الطلبات الكبرى (عالية القيمة) في مدينة محددة مرتبة من الأعلى قيمة.
    
    الفهرس الخادم: idx_city_total_amount المركب على [('customer.address.city', 1), ('total_amount', -1)]
    السبب والأثر: توجيه العروض الترويجية وخدمات الشحن المميزة للمدن ذات الإيرادات المرتفعة.
    """
    col = db[VALIDATED_COLLECTION]
    query_filter = {
        "customer.address.city": city,
        "total_amount": {"$gte": float(min_amount)}
    }
    
    cursor = col.find(query_filter).sort("total_amount", -1).skip(skip).limit(limit)
    results = list(cursor)
    
    for doc in results:
        doc["_id"] = str(doc["_id"])
        
    return {
        "query_name": "city_high_value_orders",
        "description": "استرجاع الطلبات الكبرى عالية القيمة في مدينة معينة",
        "filter": query_filter,
        "count": len(results),
        "data": results,
    }


def query_orders_by_payment(
    db: Database,
    payment_method: str = "محفظة إلكترونية",
    payment_status: str = "تم الدفع",
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """استعلام 4: استرجاع الطلبات حسب طريقة الدفع وحالة السداد.
    
    الفهرس الخادم: idx_payment_method_status المركب على [('payment.method', 1), ('payment.status', 1)]
    السبب والأثر: تدقيق ومطابقة الدفعيات الرقمية والتحويلات المالية والتأكد من نجاح عمليات الدفع.
    """
    col = db[VALIDATED_COLLECTION]
    query_filter = {
        "payment.method": payment_method,
        "payment.status": payment_status,
    }
    
    cursor = col.find(query_filter).limit(limit).skip(skip)
    results = list(cursor)
    
    for doc in results:
        doc["_id"] = str(doc["_id"])
        
    return {
        "query_name": "orders_by_payment",
        "description": "استرجاع الطلبات حسب طريقة السداد وحالة عملية الدفع",
        "filter": query_filter,
        "count": len(results),
        "data": results,
    }


def query_orders_by_sku(
    db: Database,
    sku: str = "SKU-1001",
    limit: int = 20,
    skip: int = 0
) -> Dict[str, Any]:
    """استعلام 5: استرجاع الطلبات التي تحتوي على منتج معين بواسطة رمز SKU.
    
    الفهرس الخادم: idx_items_sku الفهرس المتعدد (Multikey Index) على ('items.sku', 1)
    السبب والأثر: تتبع توزيع مبيعات منتج معين، والتحقق من الكميات المستهلكة في المخزون.
    """
    col = db[VALIDATED_COLLECTION]
    query_filter = {"items.sku": sku}
    
    cursor = col.find(query_filter).limit(limit).skip(skip)
    results = list(cursor)
    
    for doc in results:
        doc["_id"] = str(doc["_id"])
        
    return {
        "query_name": "orders_by_sku",
        "description": "استرجاع الطلبات التي تتضمن منتجاً محدداً بواسطة SKU",
        "filter": query_filter,
        "count": len(results),
        "data": results,
    }


# قاموس الاستعلامات المسجلة لدعم استدعائها برمجياً وعبر الـ API
AVAILABLE_QUERIES = {
    "customer_orders": {
        "name": "customer_orders",
        "description": "استرجاع جميع طلبات عميل محدد مرتبة تنازلياً حسب التاريخ",
        "func": query_customer_orders,
        "target_index": "idx_customer_id",
        "default_params": {"customer_id": "CUST-1001", "limit": 20, "skip": 0},
    },
    "orders_by_status_and_date": {
        "name": "orders_by_status_and_date",
        "description": "استرجاع الطلبات بحالة معينة وتاريخ محدد أو نطاق زمني",
        "func": query_orders_by_status_and_date,
        "target_index": "idx_status_order_date (Compound)",
        "default_params": {"status": "تم التسليم", "start_date": "2025-01-01", "end_date": "2025-12-31", "limit": 20},
    },
    "city_high_value_orders": {
        "name": "city_high_value_orders",
        "description": "استرجاع الطلبات الكبرى عالية القيمة في مدينة معينة",
        "func": query_city_high_value_orders,
        "target_index": "idx_city_total_amount (Compound)",
        "default_params": {"city": "صنعاء", "min_amount": 200000.0, "limit": 20},
    },
    "orders_by_payment": {
        "name": "orders_by_payment",
        "description": "استرجاع الطلبات حسب طريقة السداد وحالة الدفع",
        "func": query_orders_by_payment,
        "target_index": "idx_payment_method_status (Compound)",
        "default_params": {"payment_method": "محفظة إلكترونية", "payment_status": "تم الدفع", "limit": 20},
    },
    "orders_by_sku": {
        "name": "orders_by_sku",
        "description": "استرجاع الطلبات التي تتضمن منتجاً محدداً برمز SKU",
        "func": query_orders_by_sku,
        "target_index": "idx_items_sku (Multikey)",
        "default_params": {"sku": "SKU-1001", "limit": 20},
    },
}


def execute_query_by_name(name: str, params: Optional[Dict[str, Any]] = None, db: Optional[Database] = None) -> Dict[str, Any]:
    """تنفيذ استعلام مسجل بواسطة اسمه وتمرير المعاملات المطلوبة."""
    if db is None:
        db = get_database()
        
    if name not in AVAILABLE_QUERIES:
        raise ValueError(f"الاستعلام '{name}' غير موجود. الاستعلامات المتاحة: {list(AVAILABLE_QUERIES.keys())}")
        
    meta = AVAILABLE_QUERIES[name]
    func = meta["func"]
    
    # دمج المعاملات الافتراضية مع المعاملات الممررة
    call_params = meta["default_params"].copy()
    if params:
        for k, v in params.items():
            if v is not None:
                # تحويل الأنواع إذا لزم
                if k in ("limit", "skip"):
                    call_params[k] = int(v)
                elif k == "min_amount":
                    call_params[k] = float(v)
                else:
                    call_params[k] = v
                    
    return func(db=db, **call_params)
