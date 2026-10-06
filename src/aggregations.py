"""
موديول التجميعات والتقارير التحليلية (Aggregation Reports)
مقرر البيانات الضخمة - العملي | المرحلة النهائية (Phase 2)

يوفر 5 تقارير تجميعية متقدمة مبنية على خطوط أنابيب MongoDB Aggregation Framework:
1. المبيعات حسب المدينة (sales_by_city)
2. أفضل المنتجات مبيعاً وأعلاها إيراداً (top_products)
3. أفضل العملاء إنفاقاً وتكراراً للشراء (top_customers)
4. المبيعات حسب الفترة الزمنية (sales_by_period)
5. توزيع الطلبات حسب الحالة التشغيلية (orders_by_status)

كل تقرير يمتلك معرفاً واضحاً، ودالة تشغيل مستقلة، ويعيد نتائج فعلية محسوبة وقابلة للتسلسل بتنسيق JSON.
"""

import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from pymongo.database import Database

# إعداد المسار
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import VALIDATED_COLLECTION
from src.mongo_setup import get_database


def aggregate_sales_by_city(
    db: Database,
    limit: int = 15,
    min_orders: int = 1
) -> Dict[str, Any]:
    """تقرير 1: تجميع المبيعات حسب المدينة.
    
    يحسب: إجمالي الإيرادات، عدد الطلبات، ومتوسط قيمة الطلب لكل مدينة.
    """
    col = db[VALIDATED_COLLECTION]
    
    pipeline = [
        {
            "$match": {
                "status": {"$nin": ["ملغي", "مرتجع"]}
            }
        },
        {
            "$group": {
                "_id": "$customer.address.city",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
            }
        },
        {
            "$match": {
                "order_count": {"$gte": min_orders}
            }
        },
        {
            "$sort": {"total_sales": -1}
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
            }
        }
    ]
    
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    
    return {
        "report_name": "sales_by_city",
        "title": "تقرير المبيعات وتوزيع الطلبات حسب المدينة",
        "total_cities_returned": len(results),
        "data": results
    }


def aggregate_top_products(
    db: Database,
    limit: int = 15
) -> Dict[str, Any]:
    """تقرير 2: أفضل المنتجات مبيعاً وأعلاها إيراداً.
    
    يفكك عناصر الطلب (Unwind) لحساب: إجمالي الكميات المبيعة وإجمالي الإيرادات لكل منتج.
    """
    col = db[VALIDATED_COLLECTION]
    
    pipeline = [
        {
            "$match": {
                "status": {"$nin": ["ملغي", "مرتجع"]}
            }
        },
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.sku",
                "product_name": {"$first": "$items.name"},
                "total_qty_sold": {"$sum": "$items.qty"},
                "total_revenue": {"$sum": "$items.total"},
                "orders_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"total_revenue": -1}
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$_id",
                "product_name": 1,
                "total_qty_sold": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1
            }
        }
    ]
    
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    
    return {
        "report_name": "top_products",
        "title": "تقرير أفضل المنتجات مبيعاً وإيراداً",
        "total_products_returned": len(results),
        "data": results
    }


def aggregate_top_customers(
    db: Database,
    limit: int = 15
) -> Dict[str, Any]:
    """تقرير 3: أفضل العملاء الأكثر إنفاقاً وولاءً.
    
    يحسب إجمالي ما أنفقه العميل وعدد مرات طلبه وأحدث مدينة طلب منها.
    """
    col = db[VALIDATED_COLLECTION]
    
    pipeline = [
        {
            "$match": {
                "status": {"$nin": ["ملغي"]}
            }
        },
        {
            "$group": {
                "_id": "$customer.customer_id",
                "customer_name": {"$first": "$customer.name"},
                "city": {"$first": "$customer.address.city"},
                "phone": {"$first": "$customer.phone"},
                "total_spent": {"$sum": "$total_amount"},
                "orders_count": {"$sum": 1},
                "last_order_date": {"$max": "$order_date"}
            }
        },
        {
            "$sort": {"total_spent": -1}
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "city": 1,
                "phone": 1,
                "total_spent": {"$round": ["$total_spent", 2]},
                "orders_count": 1,
                "last_order_date": 1
            }
        }
    ]
    
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    
    return {
        "report_name": "top_customers",
        "title": "تقرير أفضل العملاء إنفاقاً",
        "total_customers_returned": len(results),
        "data": results
    }


def aggregate_sales_by_period(
    db: Database,
    period_format: str = "monthly",  # 'monthly' أو 'daily'
    limit: int = 12
) -> Dict[str, Any]:
    """تقرير 4: المبيعات حسب الفترة الزمنية (شهرياً أو يومياً).
    
    يستخرج الفترة من حقل order_date ويحسب إجمالي الإيرادات وحجم المبيعات.
    """
    col = db[VALIDATED_COLLECTION]
    
    # استخراج YYYY-MM (7 أحرف) أو YYYY-MM-DD (10 أحرف)
    substr_len = 10 if period_format == "daily" else 7
    
    pipeline = [
        {
            "$match": {
                "status": {"$nin": ["ملغي", "مرتجع"]}
            }
        },
        {
            "$group": {
                "_id": {"$substrCP": ["$order_date", 0, substr_len]},
                "total_revenue": {"$sum": "$total_amount"},
                "orders_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$sort": {"_id": -1}
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "orders_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]}
            }
        }
    ]
    
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    
    return {
        "report_name": "sales_by_period",
        "title": f"تقرير المبيعات حسب الفترة الزمنية ({period_format})",
        "period_type": period_format,
        "total_periods_returned": len(results),
        "data": results
    }


def aggregate_orders_by_status(
    db: Database
) -> Dict[str, Any]:
    """تقرير 5: توزيع الطلبات حسب الحالة التشغيلية ومعدلات الإيراد.
    
    يحسب عدد الطلبات وقيمتها الإجمالية لكل حالة من حالات النظام.
    """
    col = db[VALIDATED_COLLECTION]
    
    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "order_count": {"$sum": 1},
                "total_amount": {"$sum": "$total_amount"}
            }
        },
        {
            "$sort": {"order_count": -1}
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "order_count": 1,
                "total_amount": {"$round": ["$total_amount", 2]}
            }
        }
    ]
    
    results = list(col.aggregate(pipeline, allowDiskUse=True))
    
    return {
        "report_name": "orders_by_status",
        "title": "تقرير توزيع حالات الطلبات ومبالغها",
        "total_statuses_returned": len(results),
        "data": results
    }


# سجل تقارير التجميع المتاحة لتمكين الوصول الموحد
AVAILABLE_AGGREGATIONS = {
    "sales_by_city": {
        "name": "sales_by_city",
        "title": "المبيعات حسب المدينة",
        "description": "إجمالي المبيعات وعدد الطلبات ومتوسط السلة لكل مدينة",
        "func": aggregate_sales_by_city,
        "default_params": {"limit": 15, "min_orders": 1}
    },
    "top_products": {
        "name": "top_products",
        "title": "أفضل المنتجات",
        "description": "المنتجات الأكثر مبيعاً وتحقيقاً للإيرادات في المتجر",
        "func": aggregate_top_products,
        "default_params": {"limit": 15}
    },
    "top_customers": {
        "name": "top_customers",
        "title": "أفضل العملاء",
        "description": "العملاء الأكثر شراءً وإنفاقاً وسجل طلباتهم",
        "func": aggregate_top_customers,
        "default_params": {"limit": 15}
    },
    "sales_by_period": {
        "name": "sales_by_period",
        "title": "المبيعات حسب الفترة",
        "description": "تطور المبيعات والإيرادات شهرياً أو يومياً",
        "func": aggregate_sales_by_period,
        "default_params": {"period_format": "monthly", "limit": 12}
    },
    "orders_by_status": {
        "name": "orders_by_status",
        "title": "توزيع الطلبات حسب الحالة",
        "description": "تحليل توزيع الطلبات عبر مختلف الحالات التشغيلية",
        "func": aggregate_orders_by_status,
        "default_params": {}
    }
}


def execute_aggregation_by_name(
    name: str,
    params: Optional[Dict[str, Any]] = None,
    db: Optional[Database] = None
) -> Dict[str, Any]:
    """تنفيذ تقرير تجميعي بواسطة اسمه مع تمرير المعاملات المخصصة."""
    if db is None:
        db = get_database()
        
    if name not in AVAILABLE_AGGREGATIONS:
        raise ValueError(f"التقرير '{name}' غير موجود. التقارير المتاحة: {list(AVAILABLE_AGGREGATIONS.keys())}")
        
    meta = AVAILABLE_AGGREGATIONS[name]
    func = meta["func"]
    
    call_params = meta["default_params"].copy()
    if params:
        for k, v in params.items():
            if v is not None:
                if k in ("limit", "min_orders"):
                    call_params[k] = int(v)
                else:
                    call_params[k] = v
                    
    return func(db=db, **call_params)
