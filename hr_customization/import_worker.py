import frappe
import pandas as pd
from frappe.utils.file_manager import get_file
from datetime import datetime
from hijridate import Hijri

@frappe.whitelist()
def import_worker_data(file_url):
    inserted = []
    skipped = []

    try:
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        file_path = get_file(file_url)[1]

        df = pd.read_excel(file_path)

        # Arabic to fieldname mapping
        header_map = {
            'رقم العامل': 'worker_id',
            'اسم العامل': 'worker_name',
            'الجنسية': 'nationality',
            'رقم المنشأة': 'company_id',
            'إسم المنشأة': 'company_name',
            'رقم الحدود': 'border_number',
            'الإقامة - البطاقة': 'iqama_number',
            'المهنة': 'occupation',
            'تاريخ انتهاء الاقامة': 'iqama_expiry',
            'تاريخ دخول المملكة': 'entry_date_ksa',
            'نوع العامل': 'worker_type'
        }

        df.rename(columns=header_map, inplace=True)

        for i, row in df.iterrows():
            try:
                worker_id = str(row.get("worker_id")).strip()
                if not worker_id or worker_id.lower() == 'nan':
                    skipped.append({"row": i + 2, "reason": "Missing worker_id"})
                    continue

                if frappe.db.exists("Worker Data", {"worker_id": worker_id}):
                    skipped.append({"row": i + 2, "reason": "Duplicate worker_id"})
                    continue

                doc = frappe.get_doc({
                    "doctype": "Worker Data",
                    "worker_id": worker_id,
                    "worker_name": safe_str(row.get("worker_name")),
                    "nationality": safe_str(row.get("nationality")),
                    "company_id": safe_str(row.get("company_id")),
                    "company_name": safe_str(row.get("company_name")),
                    "border_number": safe_str(row.get("border_number")),
                    "iqama_number": safe_str(row.get("iqama_number")),
                    "occupation": safe_str(row.get("occupation")),
                    "iqama_expiry": parse_date(row.get("iqama_expiry")),
                    "entry_date_ksa": parse_date(row.get("entry_date_ksa")),
                    "worker_type": safe_str(row.get("worker_type")),
                })

                doc.insert(ignore_permissions=True)
                inserted.append(doc.name)

            except Exception as e:
                frappe.log_error(f"Row {i+2}: {str(e)}", "Worker Import Error")
                skipped.append({"row": i + 2, "reason": str(e)})

        frappe.db.commit()

    except Exception as e:
        frappe.log_error(str(e), "Worker Import Critical Error")
        return {"inserted": [], "skipped": [{"row": "All", "reason": str(e)}]}

    return {
        "inserted": inserted,
        "skipped": skipped
    }

def safe_str(value):
    """Returns None if NaN or empty, otherwise string"""
    if pd.isna(value) or str(value).strip().lower() == 'nan':
        return None
    return str(value).strip()

def parse_date(value):
    """Handles Hijri (e.g., 1446/06/12هـ) or Gregorian and converts to datetime.date"""
    try:
        if pd.isna(value) or not value:
            return None

        if isinstance(value, datetime):
            return value.date()

        value = str(value).strip().replace("هـ", "").replace("-", "/")

        # Split and validate format
        parts = value.split("/")
        if len(parts) != 3:
            return pd.to_datetime(value, errors='coerce').date()

        parts = list(map(int, parts))

        # Determine Hijri format (usually starts with 14xx)
        if parts[0] >= 1400:
            y, m, d = parts
        else:
            d, m, y = parts

        g_date = Hijri(y, m, d).to_gregorian()
        return datetime(g_date.year, g_date.month, g_date.day).date()

    except Exception:
        return None



