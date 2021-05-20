# -*- coding: utf-8 -*-
# Copyright (c) 2019, Frappe Technologies Pvt. Ltd. and Contributors
# See license.txt
from __future__ import unicode_literals

import frappe
import unittest

class TestTallyMigration(unittest.TestCase):
	def test_export_to_csv(self):
		from erpnext.erpnext_integrations.doctype.tally_migration.tally_migration import export_docs_to_csv

		docs = [{
			"doctype": "Sales Invoice",
			"customer": "CC Avenue",
			"tally_guid": "a6f8014d-f695-4baf-a1b1-1dc34bd27976-00002809",
			"tally_voucher_no": "9",
			"posting_date": "20200430",
			"due_date": "20200430",
			"items": [
				{
					"item_code": "Caramel Brown - M",
					"item_name": "Caramel Brown - M",
					"description": "Caramel Brown - M",
					"qty": "1.0000",
					"uom": "Nos",
					"conversion_factor": 1,
					"rate": "714.29",
					"price_list_rate": "714.29",
					"cost_center": "Main - MWPL",
					"warehouse": "Stores - MWPL",
					"income_account": "Maharashtra Sales - MWPL"
				},
				{
					"item_code": "Heather Grey Hammo - S",
					"item_name": "Heather Grey Hammo - S",
					"description": "Heather Grey Hammo - S",
					"qty": "1.0000",
					"uom": "Nos",
					"conversion_factor": 1,
					"rate": "1071.43",
					"price_list_rate": "1071.43",
					"cost_center": "Main - MWPL",
					"warehouse": "Stores - MWPL",
					"income_account": "Maharashtra Sales - MWPL"
				},
				{
					"item_code": "Matte Black Hammo - M",
					"item_name": "Matte Black Hammo - M",
					"description": "Matte Black Hammo - M",
					"qty": "1.0000",
					"uom": "Nos",
					"conversion_factor": 1,
					"rate": "1071.44",
					"price_list_rate": "1071.44",
					"cost_center": "Main - MWPL",
					"warehouse": "Stores - MWPL",
					"income_account": "Maharashtra Sales - MWPL"
				}
			],
			"taxes": [
				{
					"charge_type": "Actual",
					"category": "Total",
					"add_deduct_tax": "Add",
					"account_head": "Output CGST - MWPL",
					"description": "Output CGST - MWPL",
					"rate": 0,
					"tax_amount": "146.42",
					"cost_center": "Main - MWPL"
				},
				{
					"charge_type": "Actual",
					"category": "Total",
					"add_deduct_tax": "Add",
					"account_head": "Output SGST - MWPL",
					"description": "Output SGST - MWPL",
					"rate": 0,
					"tax_amount": "146.42",
					"cost_center": "Main - MWPL"
				}
			],
			"debit_to": "Sundry Debtors - MWPL",
			"selling_price_list": "Tally Price List",
			"set_posting_time": 1,
			"disable_rounded_total": 1,
			"company": "March Work Private Limited"
		}]

		export_docs_to_csv(docs)
