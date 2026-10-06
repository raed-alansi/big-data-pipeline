"""
اختبارات تكامل واجهة FastAPI ومكونات المرحلة النهائية (Phase 2 Test Suite)
مقرر البيانات الضخمة - العملي | جامعة الرازي

تختبر هذه الحزمة:
1. فحص صحة النظام والاتصال بقاعدة البيانات (GET /health)
2. إدارة الفهارس وضبطها (POST /indexes)
3. قائمة وتنفيذ الاستعلامات الـ 5 (GET /queries, GET /queries/{name})
4. قائمة وتشغيل تقارير التجميع الـ 5 (GET /aggregations, GET /aggregations/{name})
5. التحديث التزايدي للعروض المادية (POST /refresh-mv, GET /materialized-views/{name})
6. إدارة المهام المجدولة والتشغيل اليدوي وسجل التدقيق (GET /jobs, POST /jobs/{name}/run)
"""

import sys
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

# إعداد المسارات
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.api import app


@pytest.fixture(scope="module")
def client():
    """تجهيز عميل الاختبار لواجهة FastAPI."""
    with TestClient(app) as c:
        yield c


def test_health_endpoint(client):
    """اختبار نقطة فحص الصحة والاتصال بـ MongoDB."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["mongodb_connected"] is True
    assert "ecommerce_store" in data["database_name"]


def test_indexes_endpoint(client):
    """اختبار إنشاء الفهارس المطلوبة للمشروع النهائي."""
    response = client.post("/indexes")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "details" in data
    assert data["details"]["total_managed"] >= 3


def test_queries_list(client):
    """اختبار استعراض قائمة الاستعلامات والتأكد من وجود 5 استعلامات على الأقل."""
    response = client.get("/queries")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["total_queries"] >= 5
    
    query_names = [q["name"] for q in data["queries"]]
    assert "customer_orders" in query_names
    assert "orders_by_status_and_date" in query_names
    assert "city_high_value_orders" in query_names
    assert "orders_by_payment" in query_names
    assert "orders_by_sku" in query_names


def test_query_execution(client):
    """اختبار تنفيذ استعلامات محددة بالاسم."""
    # 1. اختبار استعلام حالة الطلب والتاريخ
    res = client.get("/queries/orders_by_status_and_date?status=تم التسليم&limit=5")
    assert res.status_code == 200
    res_json = res.json()
    assert res_json["status"] == "success"
    assert "data" in res_json["query_result"]

    # 2. اختبار استعلام الطلبات الكبرى بالمدينة
    res_city = client.get("/queries/city_high_value_orders?city=صنعاء&limit=5")
    assert res_city.status_code == 200
    assert res_city.json()["status"] == "success"

    # 3. اختبار استعلام غير موجود (يجب أن يعيد 404)
    res_unknown = client.get("/queries/unknown_query_name")
    assert res_unknown.status_code == 404


def test_aggregations_list(client):
    """اختبار قائمة التجميعات والتأكد من توفر 5 تقارير على الأقل."""
    response = client.get("/aggregations")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["total_aggregations"] >= 5
    
    rep_names = [a["name"] for a in data["aggregations"]]
    assert "sales_by_city" in rep_names
    assert "top_products" in rep_names
    assert "top_customers" in rep_names
    assert "sales_by_period" in rep_names
    assert "orders_by_status" in rep_names


def test_materialized_views_refresh(client):
    """اختبار التحديث التزايدي للعروض المادية (Materialized Views)."""
    response = client.post("/refresh-mv")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "daily_sales_summary" in data["details"]["views"]
    assert "top_products_summary" in data["details"]["views"]


def test_jobs_list_and_execution(client):
    """اختبار استعراض المهام وتشغيلها يدوياً وتسجيل التدقيق."""
    # 1. قائمة المهام
    response = client.get("/jobs")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["total_registered_jobs"] >= 2

    # 2. تشغيل يدوي للمهمة 1
    run_res1 = client.post("/jobs/refresh_views_job/run")
    assert run_res1.status_code == 200
    assert run_res1.json()["status"] == "success"

    # 3. تشغيل يدوي للمهمة 2
    run_res2 = client.post("/jobs/executive_report_job/run")
    assert run_res2.status_code == 200
    assert run_res2.json()["status"] == "success"

    # 4. التأكد من حفظ سجل التنفيذ (Audit Log)
    jobs_after = client.get("/jobs?history_limit=5").json()
    assert len(jobs_after["recent_execution_logs"]) > 0
    latest_log = jobs_after["recent_execution_logs"][0]
    assert latest_log["triggered_by"] == "manual"
    assert latest_log["status"] == "SUCCESS"
