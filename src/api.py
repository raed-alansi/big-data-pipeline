"""
الواجهة الموحدة للتشغيل والاختبار (Unified FastAPI Service)
مقرر البيانات الضخمة - العملي | المرحلة النهائية (Phase 2)

توفر واجهة RESTful API موحدة لتشغيل واختبار جميع وظائف المشروع:
  • GET  /health               : فحص جاهزية النظام والاتصال بقاعدة البيانات
  • POST /ingest               : تشغيل خط إدخال البيانات المعتمد في المشروع النصفي
  • POST /indexes              : إنشاء وضبط فهارس المرحلة النهائية
  • GET  /queries              : استعراض الاستعلامات العملية الـ 5 المتاحة
  • GET  /queries/{name}       : تنفيذ استعلام محدد بالاسم
  • GET  /aggregations         : استعراض تقارير التجميع الـ 5
  • GET  /aggregations/{name}  : تشغيل تقرير تجميع محدد
  • POST /refresh-mv           : تشغيل التحديث التزايدي للعروض المادية
  • GET  /jobs                 : استعراض المهام المجدولة وسجل التدقيق
  • POST /jobs/{name}/run      : التشغيل اليدوي الفوري لمهمة مجدولة
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Query, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# إعداد المسارات
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DB_NAME, MONGO_URI, VALIDATED_COLLECTION, RAW_COLLECTION, QUARANTINE_COLLECTION
from src.mongo_setup import get_database, get_client
from src.indexes import create_phase2_indexes, list_indexes, run_explain_comparison
from src.queries import AVAILABLE_QUERIES, execute_query_by_name
from src.aggregations import AVAILABLE_AGGREGATIONS, execute_aggregation_by_name
from src.materialized_views import (
    refresh_all_materialized_views,
    get_materialized_view_data,
    DAILY_SALES_MV,
    TOP_PRODUCTS_MV,
)
from src.scheduled_jobs import (
    REGISTERED_JOBS,
    trigger_job_manually,
    get_job_history,
    get_all_jobs_metadata,
    start_scheduler,
    stop_scheduler,
)
from src import elt_pipeline
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.responses import Response
import json


# فئة استجابة JSON منسقة ومرتبة مع دعم كامل للغة العربية (Indented & UTF-8)
class PrettyJSONResponse(Response):
    media_type = "application/json; charset=utf-8"

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            indent=2,
            default=str
        ).encode("utf-8")


# إدارة دورة حياة التطبيق (تشغيل وإيقاف الجدولة)
@asynccontextmanager
async def lifespan(app: FastAPI):
    # بدء تشغيل مجدول المهام عند الإقلاع
    start_scheduler()
    yield
    # إيقاف المجدول بأمان عند الخروج
    stop_scheduler()


app = FastAPI(
    title="Big Data - Phase 2 Unified API",
    description="واجهة موحدة لتشغيل واختبار وظائف خط البيانات، الاستعلامات، الفهارس، التجميعات، العروض المادية، والمهام المجدولة.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    default_response_class=PrettyJSONResponse,
    lifespan=lifespan
)

# تفعيل CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# الصفحة الرئيسية التفاعلية لتسهيل التنقل وتجربة المشروع
@app.get("/", response_class=HTMLResponse, tags=["Dashboard"], summary="الصفحة الترحيبية وروابط التشغيل السريع")
def root_dashboard():
    return """
    <!DOCTYPE html>
    <html lang="ar" dir="rtl">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>منظومة البيانات الضخمة - Phase 2</title>
        <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;800&display=swap" rel="stylesheet">
        <style>
            :root {
                --primary: #2563eb;
                --primary-dark: #1d4ed8;
                --bg: #0f172a;
                --card-bg: #1e293b;
                --text: #f8fafc;
                --text-muted: #94a3b8;
                --accent: #10b981;
                --border: #334155;
            }
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Cairo', sans-serif; }
            body { background: var(--bg); color: var(--text); padding: 40px 20px; line-height: 1.6; }
            .container { max-width: 1100px; margin: 0 auto; }
            header { text-align: center; margin-bottom: 40px; }
            header h1 { font-size: 2.3rem; font-weight: 800; color: #60a5fa; margin-bottom: 10px; }
            header p { color: var(--text-muted); font-size: 1.1rem; }
            .badge { display: inline-block; background: #065f46; color: #34d399; padding: 4px 14px; border-radius: 9999px; font-weight: 600; font-size: 0.9rem; margin-top: 10px; }
            
            .swagger-cta {
                background: linear-gradient(135deg, #1e40af, #3b82f6);
                border-radius: 16px;
                padding: 30px;
                text-align: center;
                margin-bottom: 40px;
                box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.3);
            }
            .swagger-cta h2 { font-size: 1.7rem; margin-bottom: 10px; }
            .swagger-cta p { color: #dbeafe; margin-bottom: 20px; font-size: 1rem; }
            .btn {
                display: inline-block;
                background: #ffffff;
                color: #1e3a8a;
                padding: 12px 30px;
                font-size: 1.1rem;
                font-weight: 700;
                border-radius: 10px;
                text-decoration: none;
                transition: transform 0.2s, background 0.2s;
            }
            .btn:hover { background: #f1f5f9; transform: translateY(-2px); }
            
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 20px; }
            .card {
                background: var(--card-bg);
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 24px;
                transition: border-color 0.2s, transform 0.2s;
                text-decoration: none;
                color: inherit;
                display: block;
            }
            .card:hover { border-color: #60a5fa; transform: translateY(-3px); }
            .card-title { font-size: 1.25rem; font-weight: 700; margin-bottom: 8px; color: #93c5fd; display: flex; align-items: center; justify-content: space-between; }
            .method { font-size: 0.75rem; padding: 2px 8px; border-radius: 6px; font-weight: 700; }
            .method.get { background: #065f46; color: #6ee7b7; }
            .method.post { background: #831843; color: #f472b6; }
            .card p { color: var(--text-muted); font-size: 0.95rem; }
            
            footer { text-align: center; margin-top: 50px; color: var(--text-muted); font-size: 0.9rem; border-top: 1px solid var(--border); padding-top: 25px; }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>مشروع خط البيانات الضخمة (Big Data - Phase 2)</h1>
                <p>بوابة التشغيل والاختبار الموحدة للمرحلة النهائية | جامعة الرازي</p>
                <span class="badge">المنظومة متصلة بنجاح بـ 26.2 مليون مستند</span>
            </header>

            <div class="swagger-cta">
                <h2>واجهة التوثيق التفاعلية (Swagger UI)</h2>
                <p>لتشغيل واختبار جميع الوظائف الـ 10 بصورة تفاعلية مع أزرار التنفيذ المباشرة</p>
                <a href="/docs" class="btn">🚀 فتح صفحة Swagger التفاعلية (/docs)</a>
            </div>

            <h3 style="margin-bottom: 20px; font-size: 1.4rem; color: #cbd5e1;">روابط المسارات السريعة (API Endpoints):</h3>
            <div class="grid">
                <a href="/health" class="card" target="_blank">
                    <div class="card-title">فحص حالة النظام <span class="method get">GET</span></div>
                    <p>/health — فحص الاتصال بقاعدة البيانات وحالة المجموعات الخمس.</p>
                </a>
                <a href="/queries" class="card" target="_blank">
                    <div class="card-title">الاستعلامات الـ 5 <span class="method get">GET</span></div>
                    <p>/queries — استعراض قائمة الاستعلامات العملية ومعاملاتها وفهارسها.</p>
                </a>
                <a href="/queries/orders_by_status_and_date?status=تم%20التسليم&limit=5" class="card" target="_blank">
                    <div class="card-title">استعلام الحالات والتاريخ <span class="method get">GET</span></div>
                    <p>/queries/orders_by_status_and_date — تجربة الاستعلام المفهرس المركب.</p>
                </a>
                <a href="/queries/city_high_value_orders?city=صنعاء&limit=5" class="card" target="_blank">
                    <div class="card-title">الطلبات الكبرى بالمدينة <span class="method get">GET</span></div>
                    <p>/queries/city_high_value_orders — استعلام طلبات صنعاء ذات القيمة العالية.</p>
                </a>
                <a href="/aggregations" class="card" target="_blank">
                    <div class="card-title">تقارير التجميع الـ 5 <span class="method get">GET</span></div>
                    <p>/aggregations — استعراض تقارير التجميع الـ 5 المعتمدة.</p>
                </a>
                <a href="/aggregations/sales_by_city?limit=5" class="card" target="_blank">
                    <div class="card-title">تقرير مبيعات المدن <span class="method get">GET</span></div>
                    <p>/aggregations/sales_by_city — إجمالي الإيرادات ومتوسط السلة للمدن.</p>
                </a>
                <a href="/aggregations/top_products?limit=5" class="card" target="_blank">
                    <div class="card-title">تقرير أفضل المنتجات <span class="method get">GET</span></div>
                    <p>/aggregations/top_products — المنتجات الأكثر مبيعاً وإيراداً في المتجر.</p>
                </a>
                <a href="/materialized-views/daily_sales_summary" class="card" target="_blank">
                    <div class="card-title">عرض مبيعات الأيام <span class="method get">GET</span></div>
                    <p>/materialized-views/daily_sales_summary — بيانات العرض المادي اليومي.</p>
                </a>
                <a href="/materialized-views/top_products_summary" class="card" target="_blank">
                    <div class="card-title">عرض أداء المنتجات <span class="method get">GET</span></div>
                    <p>/materialized-views/top_products_summary — بيانات العرض المادي للمنتجات.</p>
                </a>
                <a href="/jobs" class="card" target="_blank">
                    <div class="card-title">المهام المجدولة وسجل التدقيق <span class="method get">GET</span></div>
                    <p>/jobs — المهام المسجلة وجداولها وسجل التنفيذ السابق (Audit Log).</p>
                </a>
                <a href="/explain-report" class="card" target="_blank">
                    <div class="card-title">تقرير Explain والفهارس <span class="method get">GET</span></div>
                    <p>/explain-report — مقارنة الأداء (COLLSCAN vs IXSCAN) وتسريع 67,000x.</p>
                </a>
                <a href="/docs" class="card">
                    <div class="card-title">تشغيل وتحديث الفهارس والـ MV <span class="method post">POST</span></div>
                    <p>استخدم صفحة Swagger لتشغيل /indexes أو /refresh-mv أو /jobs/{name}/run بضغطة زر.</p>
                </a>
            </div>

            <footer>
                <p>مقرر البيانات الضخمة (العملي) — جامعة الرازي — 2026</p>
            </footer>
        </div>
    </body>
    </html>
    """


# نماذج البيانات (Pydantic Models)
class IngestRequest(BaseModel):
    file_path: Optional[str] = Field(
        default=None,
        description="مسار ملف CSV المراد معالجته (إذا ترك فارغاً يبحث عن الملف الافتراضي في مجلد data)"
    )


class RefreshMVRequest(BaseModel):
    force_full: bool = Field(
        default=False,
        description="إذا كانت True يتم فرض إعادة بناء شاملة، وإذا كانت False (الافتراضي) يتم التحديث التزايدي الذكي."
    )


# ─────────────────────────────────────────────────────────────
# 1. فحص صحة النظام (GET /health)
# ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["System Health"], summary="فحص جاهزية النظام والاتصال بقاعدة البيانات")
def health_check():
    """التحقق من حالة الاتصال بـ MongoDB وقراءة إحصائيات سريعة للبيانات."""
    try:
        client = get_client()
        server_info = client.server_info()
        db = client[DB_NAME]
        
        # فحص وجود المجموعات الأساسية
        existing_cols = db.list_collection_names()
        
        return {
            "status": "healthy",
            "mongodb_connected": True,
            "database_name": DB_NAME,
            "mongodb_version": server_info.get("version"),
            "collections_present": {
                RAW_COLLECTION: RAW_COLLECTION in existing_cols,
                VALIDATED_COLLECTION: VALIDATED_COLLECTION in existing_cols,
                QUARANTINE_COLLECTION: QUARANTINE_COLLECTION in existing_cols,
                DAILY_SALES_MV: DAILY_SALES_MV in existing_cols,
                TOP_PRODUCTS_MV: TOP_PRODUCTS_MV in existing_cols,
            },
            "phase": "Big Data - Phase 2 Final"
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"فشل الاتصال بقاعدة بيانات MongoDB: {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 2. تشغيل خط إدخال البيانات (POST /ingest)
# ─────────────────────────────────────────────────────────────
@app.post("/ingest", tags=["Data Pipeline"], summary="تشغيل خط إدخال البيانات (نفس Pipeline المشروع النصفي)")
def ingest_data(request: Optional[IngestRequest] = None):
    """استدعاء المنسق الرئيسي للمشروع النصفي (elt_pipeline.py) لمعالجة وتدقيق ملف CSV."""
    target_path = None
    if request and request.file_path:
        target_path = Path(request.file_path)
    else:
        # البحث عن ملف تجريبي افتراضي
        sample_path = PROJECT_ROOT / "data" / "sample_orders.csv"
        if sample_path.exists():
            target_path = sample_path
        else:
            # البحث عن أي ملف csv في مجلد data
            csv_files = list((PROJECT_ROOT / "data").glob("*.csv"))
            if csv_files:
                target_path = csv_files[0]
                
    if not target_path or not target_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"لم يتم العثور على ملف إدخال صالح. المسار المفحوص: {str(target_path)}"
        )
        
    try:
        result = elt_pipeline.run(file_path=str(target_path), reports_dir=str(PROJECT_ROOT / "reports"))
        return {
            "status": "success",
            "message": "تم تنفيذ خط الإدخال والتدقيق بنجاح.",
            "pipeline_summary": result
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"حدث خطأ أثناء تشغيل خط البيانات: {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 3. إدارة الفهارس (POST /indexes)
# ─────────────────────────────────────────────────────────────
@app.post("/indexes", tags=["Indexes & Optimization"], summary="إنشاء وتحديث فهارس المشروع النهائي")
def create_indexes():
    """إنشاء وضبط جميع الفهارس المخصصة للمشروع (Compound و Single و Multikey) لخدمة الاستعلامات."""
    try:
        res = create_phase2_indexes()
        return {
            "status": "success",
            "message": "تم التحقق من الفهارس وتطبيقها بنجاح.",
            "details": res
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل إنشاء الفهارس: {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 4. قائمة الاستعلامات (GET /queries)
# ─────────────────────────────────────────────────────────────
@app.get("/queries", tags=["Queries"], summary="استعراض قائمة الاستعلامات العملية الـ 5 المتاحة")
def list_available_queries():
    """عرض بيانات تعريف الاستعلامات الخمسة ومعاملاتها المقبولة والفهارس الداعمة لها."""
    queries_summary = []
    for q_id, q_info in AVAILABLE_QUERIES.items():
        queries_summary.append({
            "name": q_info["name"],
            "description": q_info["description"],
            "target_index": q_info["target_index"],
            "default_params": q_info["default_params"]
        })
    return {
        "status": "success",
        "total_queries": len(queries_summary),
        "queries": queries_summary
    }


# ─────────────────────────────────────────────────────────────
# 5. تنفيذ استعلام محدد (GET /queries/{name})
# ─────────────────────────────────────────────────────────────
@app.get("/queries/{name}", tags=["Queries"], summary="تنفيذ استعلام عملي محدد بالاسم")
def run_query(
    name: str,
    request: Request,
    limit: Optional[int] = Query(default=20, ge=1, le=1000, description="الحد الأقصى للنتائج"),
    skip: Optional[int] = Query(default=0, ge=0, description="عدد السجلات المتخطاة للترقيم الصفحي")
):
    """تنفيذ استعلام محدد بالاسم وتمرير أي معاملات إضافية كمعاملات استعلام (Query Parameters)."""
    if name not in AVAILABLE_QUERIES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"الاستعلام '{name}' غير موجود. المتاح: {list(AVAILABLE_QUERIES.keys())}"
        )
        
    try:
        # جمع كافة المعاملات الممررة في الرابط
        params = dict(request.query_params)
        params["limit"] = limit
        params["skip"] = skip
        
        result = execute_query_by_name(name=name, params=params)
        return {
            "status": "success",
            "query_result": result
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تنفيذ الاستعلام '{name}': {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 6. قائمة التجميعات (GET /aggregations)
# ─────────────────────────────────────────────────────────────
@app.get("/aggregations", tags=["Aggregations"], summary="استعراض قائمة تقارير التجميع الـ 5 المتاحة")
def list_available_aggregations():
    """عرض قائمة تقارير التجميع الخمسة ووصف كل منها ومعاملاتها."""
    aggregations_summary = []
    for a_id, a_info in AVAILABLE_AGGREGATIONS.items():
        aggregations_summary.append({
            "name": a_info["name"],
            "title": a_info["title"],
            "description": a_info["description"],
            "default_params": a_info["default_params"]
        })
    return {
        "status": "success",
        "total_aggregations": len(aggregations_summary),
        "aggregations": aggregations_summary
    }


# ─────────────────────────────────────────────────────────────
# 7. تشغيل تقرير تجميع محدد (GET /aggregations/{name})
# ─────────────────────────────────────────────────────────────
@app.get("/aggregations/{name}", tags=["Aggregations"], summary="تشغيل تقرير تجميع محدد بالاسم")
def run_aggregation(
    name: str,
    request: Request,
    limit: Optional[int] = Query(default=15, ge=1, le=100, description="الحد الأقصى للنتائج")
):
    """تنفيذ تقرير تجميعي محدد بالاسم واستخراج بياناته التحليلية."""
    if name not in AVAILABLE_AGGREGATIONS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"تقرير التجميع '{name}' غير موجود. المتاح: {list(AVAILABLE_AGGREGATIONS.keys())}"
        )
        
    try:
        params = dict(request.query_params)
        params["limit"] = limit
        result = execute_aggregation_by_name(name=name, params=params)
        return {
            "status": "success",
            "aggregation_result": result
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تنفيذ تقرير التجميع '{name}': {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 8. التحديث التزايدي للعروض المادية (POST /refresh-mv)
# ─────────────────────────────────────────────────────────────
@app.post("/refresh-mv", tags=["Materialized Views"], summary="تشغيل التحديث التزايدي للعروض المادية")
def refresh_views(body: Optional[RefreshMVRequest] = None):
    """تحديث العروض المادية (daily_sales_summary و top_products_summary) تزايدياً دون إعادة بناء كامل البيانات."""
    force_full = body.force_full if body else False
    try:
        result = refresh_all_materialized_views(force_full=force_full)
        return {
            "status": "success",
            "message": "تم تحديث العروض المادية بنجاح.",
            "details": result
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل تحديث العروض المادية: {str(exc)}"
        )


# مسار مساعد اختياري لعرض محتوى العروض المادية
@app.get("/materialized-views/{name}", tags=["Materialized Views"], summary="معاينة بيانات عرض مادي محدد")
def view_mv_content(name: str, limit: int = Query(default=20, ge=1, le=100)):
    """استرجاع بيانات ملخصة من أحد العروض المادية (daily_sales_summary أو top_products_summary)."""
    try:
        return get_materialized_view_data(view_name=name, limit=limit)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc)
        )


# ─────────────────────────────────────────────────────────────
# 9. استعراض المهام المجدولة (GET /jobs)
# ─────────────────────────────────────────────────────────────
@app.get("/jobs", tags=["Scheduled Jobs"], summary="استعراض المهام المجدولة وسجل تنفيذها السابق")
def list_jobs(history_limit: int = Query(default=10, ge=1, le=50, description="عدد سجلات التدقيق السابقة")):
    """عرض قائمة بجميع المهام المسجلة، جداولها الزمنية، وسجل تدقيق التنفيذ الأخير (Audit Log)."""
    try:
        jobs_metadata = get_all_jobs_metadata()
        recent_logs = get_job_history(limit=history_limit)
        return {
            "status": "success",
            "total_registered_jobs": len(jobs_metadata),
            "jobs": jobs_metadata,
            "recent_execution_logs": recent_logs
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"فشل قراءة بيانات المهام: {str(exc)}"
        )


# ─────────────────────────────────────────────────────────────
# 10. التشغيل اليدوي لمهمة مجدولة (POST /jobs/{name}/run)
# ─────────────────────────────────────────────────────────────
@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"], summary="تشغيل يدوي فوري لمهمة مجدولة محددة")
def run_job_manually(name: str):
    """إطلاق تنفيذ فوري لإحدى المهام المجدولة وتسجيل نتيجتها ووقتها في سجل التدقيق (Audit Log)."""
    if name not in REGISTERED_JOBS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"المهمة '{name}' غير مسجلة. المهام المتاحة: {list(REGISTERED_JOBS.keys())}"
        )
        
    try:
        result = trigger_job_manually(job_name=name)
        return {
            "status": "success",
            "message": f"تم تشغيل المهمة '{name}' يدوياً بنجاح.",
            "execution_details": result
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"حدث خطأ أثناء تشغيل المهمة '{name}': {str(exc)}"
        )


# نقطة نهاية مساعدة لعرض تقرير Explain
@app.get("/explain-report", tags=["Indexes & Optimization"], summary="عرض نتائج تقرير Explain قبل وبعد الفهارس")
def get_explain_report():
    """استرجاع بيانات مقارنة Explain المحفوظة وجداول المقارنة والأثر."""
    report_file = PROJECT_ROOT / "reports" / "explain_results.json"
    if report_file.exists():
        import json
        with open(report_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        # تشغيل المقارنة وتوليد التقرير
        return run_explain_comparison()


if __name__ == "__main__":
    import uvicorn
    print("[INFO] Starting FastAPI server on http://127.0.0.1:8000 ...")
    uvicorn.run("src.api:app", host="127.0.0.1", port=8000, reload=True)
