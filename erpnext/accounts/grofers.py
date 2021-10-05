import json

import frappe
from frappe.utils.data import flt


def execute(job_name="1935090ed2"):
	api_jobs = frappe.db.get_all(
		'API Job',
		{'document_type': 'Sales Invoice', 'name': '16eddbee7c'},
		['name', 'docs', 'job_type', 'callback_url', 'callback_headers', 'submit_document'],
	)

	error_list = []
	for api_job in api_jobs:
		if api_job.docs:
			docs = json.loads(api_job.docs)
			for idx, d in enumerate(docs):
				try:
					if api_job.job_type == 'INSERT':
						if frappe.db.exists('POS Invoice', d.get('grofers_invoice_id')): continue

						d['doctype'] = 'POS Invoice'
						if not frappe.db.exists('Customer', d.get('customer')):
							frappe.get_doc({
								"name": d.get('customer'),
								"customer_name": d.get('customer'),
								"customer_type": "Individual",
								"gst_category": "Unregistered",
								"customer_group": "All Customer Groups",
								"doctype": "Customer",
							}).db_insert()

						doc = frappe.get_doc(d)
						doc.flags.ignore_links = True
						doc.flags.ignore_version = True
						doc.flags.ignore_validate = True
						doc.__islocal = True
						tax_template = doc.taxes_and_charges
						doc.set_missing_values()
						doc.pos_timestamp = frappe.utils.get_datetime(doc.pos_timestamp)
						company_details = frappe.db.get_value('Company', doc.company, ['abbr', 'pan_details', 'cin'], as_dict=1)
						doc.abbr = company_details.abbr
						doc.comapny_pan = company_details.pan_details
						doc.company_cin = company_details.cin

						warehouse_details = frappe.db.get_value('Warehouse', doc.set_warehouse, ['city', 'address_line_1', 'address_line_2', 'gst_tin'], as_dict=1)
						doc.supply_city = warehouse_details.city
						doc.address_line_1 = warehouse_details.address_line_1
						doc.address_line_2 = warehouse_details.address_line_2
						doc.warehouse = doc.set_warehouse
						doc.title = doc.customer_name
						doc.gst_tin = warehouse_details.gst_tin
						doc.taxes_and_charges = tax_template
						doc.gst_category = 'Unregistered'
						doc.set_taxes()
						mode_of_payment = doc.payments[0].mode_of_payment
						doc.payments[0].account = frappe.db.get_value("Mode of Payment Account",
							{"parent": mode_of_payment, "company": doc.company}, "default_account")
						doc.calculate_taxes_and_totals()
						doc.set_total_in_words()
						doc.set_new_name(set_name=doc.grofers_invoice_id, set_child_names=1)
						doc.posting_date = '2021-10-01'
						# doc.insert()
						doc.submit()
						print(f'{idx+1} out of {len(docs)} submitted', end='\r')
						# frappe.db.commit()
				except Exception as error:
					pass
					# error_list.append(f"{doc.grofers_invoice_id} ---- {str(error)}")
					# frappe.db.rollback()
					# frappe.log_error(f"{doc.grofers_invoice_id} ---- {str(error)}")
					# frappe.db.commit()

def copy_custom_field():
	fields = frappe.get_all('Custom Field', {'dt': 'Sales Invoice Item'}, ['name', 'fieldname'])
	for field in fields:
		if not frappe.get_all('Custom Field', {'dt': 'POS Invoice Item', 'fieldname': field.fieldname}):
			field = frappe.get_doc('Custom Field', field)
			pos_field = frappe.copy_doc(field)
			pos_field.dt = 'POS Invoice Item'
			pos_field.insert()

def create_pos_invoice_merge_logs():
	pos_invoices = frappe.get_all('POS Invoice', filters={'creation': ['>', '2021-10-04 19:40:00']},
		fields=["name", "company", "set_warehouse", "grand_total", "posting_date"])
	out = {}
	for inv in pos_invoices:
		if not out.get(inv.company):
			out[inv.company] = []

		out[inv.company].append(inv)

	for key, value in out.items():
		pos_inv = value
		set_warehouse = key[1]
		create_customer_if_not_exists(set_warehouse)
		merge_log = frappe.new_doc('POS Invoice Merge Log')
		merge_log.customer = set_warehouse
		merge_log.merge_invoices_based_on = 'Customer Group'
		merge_log.customer_group = 'Warehouses'
		for inv in pos_inv:
			merge_log.posting_date = inv.posting_date
			merge_log.append("pos_invoices", {
				'pos_invoice': inv.name
			})
		merge_log.ignore_validate = True
		merge_log.ignore_mandatory = True
		merge_log.insert()

def submit_merge_logs():
	logs = frappe.db.get_all('POS Invoice Merge Log', pluck='name')
	for log in logs:
		frappe.get_doc('POS Invoice Merge Log', log).submit()

def create_customer_if_not_exists(set_warehouse):
	if not frappe.db.exists('Customer', set_warehouse):
		frappe.get_doc({
			"name": set_warehouse,
			"customer_name": set_warehouse,
			"customer_type": "Individual",
			"gst_category": "Unregistered",
			"customer_group": "Warehouses",
			"territory": "All Territories",
			"doctype": "Customer"
		}).db_insert()


def run_api_job():
	docs = frappe.db.get_value('API Job', '16eddbee7c', 'docs')
	for doc in json.loads(docs):
		try:
			doc = frappe.get_doc(doc)
			ignore_validations(doc)
			doc.submit()
		except Exception as e:
			print(f'Failed - {str(e)}')

def show_diff():
	si = frappe.get_doc('Sales Invoice', 'SI-21-11-98220')
	si_copy = frappe.get_doc('Sales Invoice', 'SI-21-11-98220 - 5')

	frappe.log_error(message=json.dumps(si.as_dict(), indent=2, sort_keys=True, default=str))
	frappe.log_error(message=json.dumps(si_copy.as_dict(), indent=2, sort_keys=True, default=str))

def ignore_validations(doc):
	doc.flags.ignore_links = True
	doc.flags.ignore_version = True
	doc.flags.ignore_validate = True
	doc.__islocal = True
	tax_template = doc.taxes_and_charges
	# doc.set_missing_values()

	doc.pos_timestamp = frappe.utils.get_datetime(doc.pos_timestamp)
	company_details = frappe.db.get_value('Company', doc.company, ['abbr', 'pan_details', 'cin'], as_dict=1)
	doc.abbr = company_details.abbr
	doc.comapny_pan = company_details.pan_details
	doc.company_cin = company_details.cin
	warehouse_details = frappe.db.get_value('Warehouse', doc.set_warehouse, ['city', 'address_line_1', 'address_line_2', 'gst_tin'], as_dict=1)
	doc.supply_city = warehouse_details.city
	doc.address_line_1 = warehouse_details.address_line_1
	doc.address_line_2 = warehouse_details.address_line_2
	doc.warehouse = doc.set_warehouse
	doc.gst_tin = warehouse_details.gst_tin

	doc.customer_name = frappe.db.get_value('Customer', doc.customer, 'customer_name')
	doc.title = doc.customer_name
	doc.gst_category = 'Unregistered'

	doc.set_pos_fields()
	doc.set_price_list_currency('selling')
	doc.set_debit_to()

	for item in doc.items:
		item_details = frappe.db.get_value('Item', item.item_code, [
			'item_name', 'gst_hsn_code', 'description', 'stock_uom',
			'item_group'
		], as_dict=1)
		item.item_name = item_details.item_name
		item.gst_hsn_code = item_details.gst_hsn_code
		item.description = item_details.description
		item.item_group = item_details.item_group
		item.uom = item_details.stock_uom

		item.conversion_factor = 1
		item.stock_qty = flt(item.qty) * flt(item.conversion_factor)
		item.stock_uom_rate = flt(item.rate) / flt(item.conversion_factor or 1)

		# item.price_list_rate not set

	for data in doc.payments:
		if not data.account:
			data.account = frappe.db.get_value(
				"Mode of Payment Account",
				{"parent": data.mode_of_payment, "company": doc.company},
				"default_account"
			)

	doc.set_item_tax_template()
	doc.taxes_and_charges = tax_template
	doc.set_taxes()
	doc.calculate_taxes_and_totals()
	doc.set_total_in_words()
	# doc.grofers_invoice_id = 'CFN1956T21017523 - 5'
	# doc.set_new_name(set_name='SI-21-11-98220 - 5', set_child_names=1)
	doc.set_new_name(set_name=doc.grofers_invoice_id, set_child_names=1)