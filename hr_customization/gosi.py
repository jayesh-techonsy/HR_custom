import frappe
import pandas as pd
from frappe.utils.file_manager import get_file
from datetime import datetime

@frappe.whitelist()
def import_gosi_worker_data(file_url):
    inserted = []
    skipped = []

    try:
        file_doc = frappe.get_doc("File", {"file_url": file_url})
        file_path = get_file(file_url)[1]

        # Read full sheet
        df_raw = pd.read_excel(file_path, header=None)

        # Detect header row dynamically
        header_row_idx = None
        for idx, row in df_raw.iterrows():
            if "اسم المشترك" in row.values and "رقم الهوية" in row.values:
                header_row_idx = idx
                break

        if header_row_idx is None:
            frappe.throw("Header row not found. Please upload a valid file.")

        # Load with correct header row
        df = pd.read_excel(file_path, header=header_row_idx)

        # Rename columns
        header_map = {
            "اسم المشترك": "subscriber_name",
            "رقم الهوية": "identity_number",
            "الجنسية": "nationality",
            "الجنس": "gender",
            "تاريخ الميلاد": "date_of_birth",
            "الأجر الأساسي": "basic_salary",
            "السكن": "housing",
            "العمولات": "commissions",
            "البدلات الأخرى": "other_allowances",
            "إجمالي الأجر": "total_salary",
            "الاجر الخاضع للاشتراك": "contributable_salary",
            "المهنة": "occupation",
            "تاريخ الإلتحاق": "joining_date"
        }

        df.rename(columns=header_map, inplace=True)
        df = df.applymap(lambda x: None if pd.isna(x) or str(x).strip().lower() == 'nan' else x)

        for i, row in df.iterrows():
            try:
                identity_number = str(row.get("identity_number")).strip()
                if not identity_number:
                    skipped.append({"row": i + 2, "reason": "Missing identity_number"})
                    continue

                if frappe.db.exists("GOSI Worker Data", {"identity_number": identity_number}):
                    skipped.append({"row": i + 2, "reason": "Duplicate"})
                    continue

                doc = frappe.get_doc({
                    "doctype": "GOSI Worker Data",
                    **row
                })

                doc.insert(ignore_permissions=True)
                inserted.append(doc.name)

            except Exception as e:
                frappe.log_error(f"Row {i+2}: {str(e)}", "GOSI Import Error")
                skipped.append({"row": i + 2, "reason": str(e)})

        frappe.db.commit()

    except Exception as e:
        frappe.log_error(str(e), "GOSI Import Critical Error")
        return {"inserted": [], "skipped": [{"row": "All", "reason": str(e)}]}

    return {
        "inserted": inserted,
        "skipped": skipped
    }

