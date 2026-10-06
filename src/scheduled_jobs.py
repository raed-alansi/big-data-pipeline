"""
موديول المهام المجدولة وسجل التدقيق (Scheduled Jobs & Audit Logs)
مقرر البيانات الضخمة - العملي | المرحلة النهائية (Phase 2)

يوفر:
1. تعريف وتشغيل مهمتين مجدولتين فعليتين مرتبطتين بالمشروع:
   - refresh_views_job: التحديث التزايدي للعروض المادية (Materialized Views).
   - executive_report_job: توليد وحفظ تقرير الأداء التنفيذي للمبيعات وحالات الطلبات دورياً.
2. دعم الجدولة التلقائية في الخلفية باستخدام BackgroundScheduler (APScheduler).
3. إمكانية التشغيل اليدوي الفوري لأي مهمة عبر الـ API وأثناء الاختبار والمناقشة.
4. تسجيل دقيق وتوثيق كامل لكل عملية تنفيذ (Audit Log) في MongoDB يتضمن:
   وقت البداية والنهاية، مدة التنفيذ بالثواني، وحالة النجاح أو الفشل.
"""

import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from pymongo.database import Database

# إعداد المسار
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.mongo_setup import get_database
from src.materialized_views import refresh_all_materialized_views, DAILY_SALES_MV, TOP_PRODUCTS_MV
from src.aggregations import aggregate_orders_by_status, aggregate_sales_by_period

# اسم مجموعة سجلات تنفيذ المهام في MongoDB
JOB_LOGS_COLLECTION = "job_execution_logs"
SYSTEM_REPORTS_COLLECTION = "system_reports"

logger = logging.getLogger("scheduled_jobs")

# كائن الجدولة العام
scheduler: Optional[BackgroundScheduler] = None


def log_job_execution(
    db: Database,
    job_name: str,
    start_time: datetime,
    end_time: datetime,
    status: str,
    triggered_by: str = "scheduler",
    details: Optional[Dict[str, Any]] = None,
    error_message: Optional[str] = None
):
    """توثيق تفاصيل تنفيذ المهمة في مجموعة job_execution_logs."""
    duration = (end_time - start_time).total_seconds()
    log_doc = {
        "job_name": job_name,
        "triggered_by": triggered_by,
        "status": status,  # "SUCCESS" أو "FAILED"
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": round(duration, 3),
        "details": details or {},
        "error_message": error_message
    }
    
    try:
        col = db[JOB_LOGS_COLLECTION]
        col.insert_one(log_doc)
    except Exception as e:
        logger.error(f"فشل حفظ سجل المهمة في MongoDB: {e}")


def execute_job_refresh_views(triggered_by: str = "scheduler", db: Optional[Database] = None) -> Dict[str, Any]:
    """المهمة 1: التحديث التزايدي للعروض المادية (Materialized Views)."""
    if db is None:
        db = get_database()
        
    job_name = "refresh_views_job"
    start_time = datetime.now(timezone.utc)
    
    try:
        result = refresh_all_materialized_views(db=db, force_full=False)
        end_time = datetime.now(timezone.utc)
        
        log_job_execution(
            db=db,
            job_name=job_name,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            triggered_by=triggered_by,
            details={"refresh_result": result}
        )
        return {
            "job_name": job_name,
            "status": "SUCCESS",
            "triggered_by": triggered_by,
            "duration_seconds": round((end_time - start_time).total_seconds(), 3),
            "result": result
        }
    except Exception as exc:
        end_time = datetime.now(timezone.utc)
        log_job_execution(
            db=db,
            job_name=job_name,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            triggered_by=triggered_by,
            error_message=str(exc)
        )
        return {
            "job_name": job_name,
            "status": "FAILED",
            "triggered_by": triggered_by,
            "duration_seconds": round((end_time - start_time).total_seconds(), 3),
            "error": str(exc)
        }


def execute_job_executive_report(triggered_by: str = "scheduler", db: Optional[Database] = None) -> Dict[str, Any]:
    """المهمة 2: توليد وحفظ تقرير الأداء التنفيذي للمبيعات وحالات الطلبات."""
    if db is None:
        db = get_database()
        
    job_name = "executive_report_job"
    start_time = datetime.now(timezone.utc)
    
    try:
        # استخراج مؤشرات الأداء التنفيذية مباشرة من العروض المادية (Materialized Views) لتحقيق أداء فائق
        daily_col = db[DAILY_SALES_MV]
        products_col = db[TOP_PRODUCTS_MV]
        
        recent_daily_sales = list(daily_col.find({}, {"_id": 0}).sort("date", -1).limit(7))
        top_selling = list(products_col.find({}, {"_id": 0}).sort("total_revenue", -1).limit(5))
        
        report_data = {
            "generated_at": start_time.isoformat(),
            "report_type": "executive_summary",
            "source": "Materialized Views (daily_sales_summary & top_products_summary)",
            "recent_daily_performance": recent_daily_sales,
            "top_revenue_products": top_selling,
            "total_days_summarized": len(recent_daily_sales),
            "total_top_products_tracked": len(top_selling)
        }
        
        # حفظ التقرير في MongoDB
        db[SYSTEM_REPORTS_COLLECTION].insert_one(report_data)
        
        # حفظ نسخة في مجلد التقارير reports
        reports_dir = PROJECT_ROOT / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        latest_file = reports_dir / "latest_executive_report.json"
        
        # تنظيف معرف BSON
        clean_rep = {k: v for k, v in report_data.items() if k != "_id"}
        with open(latest_file, "w", encoding="utf-8") as f:
            json.dump(clean_rep, f, ensure_ascii=False, indent=2)
            
        end_time = datetime.now(timezone.utc)
        log_job_execution(
            db=db,
            job_name=job_name,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            triggered_by=triggered_by,
            details={"saved_records": len(report_data.get("recent_daily_performance", []))}
        )
        return {
            "job_name": job_name,
            "status": "SUCCESS",
            "triggered_by": triggered_by,
            "duration_seconds": round((end_time - start_time).total_seconds(), 3),
            "report": clean_rep
        }
    except Exception as exc:
        end_time = datetime.now(timezone.utc)
        log_job_execution(
            db=db,
            job_name=job_name,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            triggered_by=triggered_by,
            error_message=str(exc)
        )
        return {
            "job_name": job_name,
            "status": "FAILED",
            "triggered_by": triggered_by,
            "duration_seconds": round((end_time - start_time).total_seconds(), 3),
            "error": str(exc)
        }


# جدول تعريف المهام المسجلة في النظام
REGISTERED_JOBS = {
    "refresh_views_job": {
        "name": "refresh_views_job",
        "title": "التحديث التزايدي للعروض المادية",
        "description": "تحديث مجموعات daily_sales_summary و top_products_summary بالسجلات الجديدة تزايدياً.",
        "schedule": "Every 30 minutes",
        "func": execute_job_refresh_views,
    },
    "executive_report_job": {
        "name": "executive_report_job",
        "title": "توليد تقرير الأداء التنفيذي",
        "description": "حساب وتخزين ملخص دوري لتوزيع حالات الطلبات والمبيعات الشهرية في قاعدة البيانات.",
        "schedule": "Every 60 minutes",
        "func": execute_job_executive_report,
    }
}


def trigger_job_manually(job_name: str, db: Optional[Database] = None) -> Dict[str, Any]:
    """تشغيل يدوي فوري لمهمة مجدولة محددة بالاسم."""
    if job_name not in REGISTERED_JOBS:
        raise ValueError(f"المهمة '{job_name}' غير مسجلة. المهام المتاحة: {list(REGISTERED_JOBS.keys())}")
        
    job_info = REGISTERED_JOBS[job_name]
    return job_info["func"](triggered_by="manual", db=db)


def get_job_history(job_name: Optional[str] = None, limit: int = 20, db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """استرجاع سجل التدقيق لتنفيذ المهام المجدولة من MongoDB."""
    if db is None:
        db = get_database()
        
    query_filter = {"job_name": job_name} if job_name else {}
    col = db[JOB_LOGS_COLLECTION]
    
    logs = list(col.find(query_filter, {"_id": 0}).sort("start_time", -1).limit(limit))
    return logs


def get_all_jobs_metadata(db: Optional[Database] = None) -> List[Dict[str, Any]]:
    """إرجاع قائمة بجميع المهام المسجلة، وجدولها الزمني، وتاريخ آخر تنفيذ وحالته."""
    if db is None:
        db = get_database()
        
    col = db[JOB_LOGS_COLLECTION]
    summary = []
    
    for key, info in REGISTERED_JOBS.items():
        last_log = col.find_one({"job_name": key}, sort=[("start_time", -1)], projection={"_id": 0})
        summary.append({
            "name": info["name"],
            "title": info["title"],
            "description": info["description"],
            "schedule": info["schedule"],
            "last_execution": last_log
        })
        
    return summary


def start_scheduler():
    """تشغيل مجدول المهام في الخلفية."""
    global scheduler
    if scheduler is not None and scheduler.running:
        return
        
    scheduler = BackgroundScheduler(daemon=True)
    # جدولة المهمة 1 كل 30 دقيقة
    scheduler.add_job(
        func=execute_job_refresh_views,
        trigger="interval",
        minutes=30,
        id="refresh_views_job",
        name="Refresh Materialized Views",
        replace_existing=True
    )
    # جدولة المهمة 2 كل 60 دقيقة
    scheduler.add_job(
        func=execute_job_executive_report,
        trigger="interval",
        minutes=60,
        id="executive_report_job",
        name="Generate Executive Report",
        replace_existing=True
    )
    scheduler.start()
    logger.info("[OK] BackgroundScheduler started with 2 jobs.")


def stop_scheduler():
    """إيقاف مجدول المهام عند إغلاق التطبيق."""
    global scheduler
    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)
