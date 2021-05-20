# -*- coding: utf-8 -*-
# Copyright (c) 2019, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from __future__ import unicode_literals

import json
import re
import sys
import traceback
import zipfile
from decimal import Decimal

from bs4 import BeautifulSoup as bs

import frappe
from erpnext import encode_company_abbr
from erpnext.accounts.doctype.account.chart_of_accounts.chart_of_accounts import create_charts
from erpnext.accounts.doctype.chart_of_accounts_importer.chart_of_accounts_importer import unset_existing_data

from frappe import _
from frappe.custom.doctype.custom_field.custom_field import create_custom_field
from frappe.model.document import Document
from frappe.model.naming import getseries, revert_series_if_last
from frappe.utils.data import format_datetime, cint, flt, cstr
from frappe.utils.csvutils import to_csv

from frappe.core.doctype.data_import.exporter import Exporter

PRIMARY_ACCOUNT = "Primary"
VOUCHER_CHUNK_SIZE = 500


@frappe.whitelist()
def new_doc(document):
	document = json.loads(document)
	doctype = document.pop("doctype")
	document.pop("name", None)
	doc = frappe.new_doc(doctype)
	doc.update(document)

	return doc

class TallyMigration(Document):
	def validate(self):
		failed_import_log = json.loads(self.failed_import_log)
		sorted_failed_import_log = sorted(failed_import_log, key=lambda row: row["doc"]["creation"])
		self.failed_import_log = json.dumps(sorted_failed_import_log)

	def autoname(self):
		if not self.name:
			self.name = "Tally Migration on " + format_datetime(self.creation)

	def get_collection(self, data_file):
		def sanitize(string):
			return re.sub("&#4;", "", string)

		def emptify(string):
			string = re.sub(r"<\w+/>", "", string)
			string = re.sub(r"<([\w.]+)>\s*<\/\1>", "", string)
			string = re.sub(r"\r\n", "", string)
			return string

		master_file = frappe.get_doc("File", {"file_url": data_file})
		master_file_path = master_file.get_full_path()

		if zipfile.is_zipfile(master_file_path):
			with zipfile.ZipFile(master_file_path) as zf:
				encoded_content = zf.read(zf.namelist()[0])
				try:
					content = encoded_content.decode("utf-8-sig")
				except UnicodeDecodeError:
					content = encoded_content.decode("utf-16")

		master = bs(sanitize(emptify(content)), "xml")
		collection = master.BODY.IMPORTDATA.REQUESTDATA
		return collection

	def dump_processed_data(self, data, filename):
		f = frappe.get_doc({
			"doctype": "File",
			"file_name": filename + ".json",
			"attached_to_doctype": self.doctype,
			"attached_to_name": self.name,
			"content": json.dumps(data),
			"is_private": True
		})
		try:
			f.insert()
			f.reload()
		except frappe.DuplicateEntryError:
			pass
		return f.name
	
	def fetch_processed_data(self, filename):
		_file = frappe.get_doc("File", filename)
		content = _file.get_content()
		return json.loads(content)

	def _process_master_data(self):
		def get_company_name(collection):
			return collection.find_all("REMOTECMPINFO.LIST")[0].REMOTECMPNAME.string.strip()

		def get_coa_customers_suppliers(collection):
			root_type_map = {
				"Application of Funds (Assets)": "Asset",
				"Expenses": "Expense",
				"Income": "Income",
				"Source of Funds (Liabilities)": "Liability"
			}
			roots = set(root_type_map.keys())
			accounts = list(get_groups(collection.find_all("GROUP"))) + list(get_ledgers(collection.find_all("LEDGER")))
			children, parents = get_children_and_parent_dict(accounts)
			group_set =  [acc[1] for acc in accounts if acc[2]]
			children, customers, suppliers = remove_parties(parents, children, group_set)

			try:
				coa = traverse({}, children, roots, roots, group_set)
			except RecursionError:
				self.log(_("Error occured while parsing Chart of Accounts: Please make sure that no two accounts have the same name"))

			for account in coa:
				coa[account]["root_type"] = root_type_map[account]

			return coa, customers, suppliers

		def get_groups(accounts):
			for account in accounts:
				if account["NAME"] in (self.tally_creditors_account, self.tally_debtors_account):
					yield get_parent(account), account["NAME"], 0
				else:
					yield get_parent(account), account["NAME"], 1

		def get_ledgers(accounts):
			for account in accounts:
				# If Ledger doesn't have PARENT field then don't create Account
				# For example "Profit & Loss A/c"
				if account.PARENT:
					yield account.PARENT.string.strip(), account["NAME"], 0

		def get_parent(account):
			if account.PARENT:
				return account.PARENT.string.strip()
			return {
				("Yes", "No"): "Application of Funds (Assets)",
				("Yes", "Yes"): "Expenses",
				("No", "Yes"): "Income",
				("No", "No"): "Source of Funds (Liabilities)",
			}[(account.ISDEEMEDPOSITIVE.string.strip(), account.ISREVENUE.string.strip())]

		def get_children_and_parent_dict(accounts):
			children, parents = {}, {}
			for parent, account, is_group in accounts:
				children.setdefault(parent, set()).add(account)
				parents.setdefault(account, set()).add(parent)
				parents[account].update(parents.get(parent, []))
			return children, parents

		def remove_parties(parents, children, group_set):
			customers, suppliers = set(), set()
			for account in parents:
				found = False
				if self.tally_creditors_account in parents[account]:
					found = True
					if account not in group_set:
						suppliers.add(account)
				if self.tally_debtors_account in parents[account]:
					found = True
					if account not in group_set:
						customers.add(account)
				if found:
					children.pop(account, None)

			return children, customers, suppliers

		def traverse(tree, children, accounts, roots, group_set):
			for account in accounts:
				if account in group_set or account in roots:
					if account in children:
						tree[account] = traverse({}, children, children[account], roots, group_set)
					else:
						tree[account] = {"is_group": 1}
				else:
					tree[account] = {}
			return tree

		def get_parties_addresses(collection, customers, suppliers):
			parties, addresses = [], []
			for account in collection.find_all("LEDGER"):
				party_type = None
				links = []
				if account.NAME.string.strip() in customers:
					party_type = "Customer"
					parties.append({
						"doctype": party_type,
						"customer_name": account.NAME.string.strip(),
						"tax_id": account.INCOMETAXNUMBER.string.strip() if account.INCOMETAXNUMBER else None,
						"customer_group": "All Customer Groups",
						"territory": "All Territories",
						"customer_type": "Individual",
					})
					links.append({"link_doctype": party_type, "link_name": account["NAME"]})

				if account.NAME.string.strip() in suppliers:
					party_type = "Supplier"
					parties.append({
						"doctype": party_type,
						"supplier_name": account.NAME.string.strip(),
						"pan": account.INCOMETAXNUMBER.string.strip() if account.INCOMETAXNUMBER else None,
						"supplier_group": "All Supplier Groups",
						"supplier_type": "Individual",
					})
					links.append({"link_doctype": party_type, "link_name": account["NAME"]})

				if party_type:
					address = "\n".join([a.string.strip() for a in account.find_all("ADDRESS")])
					addresses.append({
						"doctype": "Address",
						"address_line1": address[:140].strip(),
						"address_line2": address[140:].strip(),
						"country": account.COUNTRYNAME.string.strip() if account.COUNTRYNAME else None,
						"state": account.LEDSTATENAME.string.strip() if account.LEDSTATENAME else None,
						"gst_state": account.LEDSTATENAME.string.strip() if account.LEDSTATENAME else None,
						"pin_code": account.PINCODE.string.strip() if account.PINCODE else None,
						"mobile": account.LEDGERPHONE.string.strip() if account.LEDGERPHONE else None,
						"phone": account.LEDGERPHONE.string.strip() if account.LEDGERPHONE else None,
						"gstin": account.PARTYGSTIN.string.strip() if account.PARTYGSTIN else None,
						"links": links
					})
			return parties, addresses

		def get_stock_items_uoms(collection):
			uoms = []
			for uom in collection.find_all("UNIT"):
				uoms.append({"doctype": "UOM", "uom_name": uom.NAME.string.strip()})

			items = []
			for item in collection.find_all("STOCKITEM"):
				stock_uom = item.BASEUNITS.string.strip().title() if item.BASEUNITS else "Unit"
				items.append({
					"doctype": "Item",
					"item_code" : item.NAME.string.strip(),
					"stock_uom": stock_uom.strip(),
					"is_stock_item": 0,
					"item_group": "All Item Groups",
					"item_defaults": [{"company": self.erpnext_company}]
				})

			return items, uoms

		try:
			self.publish("Process Master Data", _("Reading Uploaded File"), 1, 5)
			collection = self.get_collection(self.master_data)
			company = get_company_name(collection)
			self.tally_company = company
			self.erpnext_company = company

			self.publish("Process Master Data", _("Processing Chart of Accounts and Parties"), 2, 5)
			chart_of_accounts, customers, suppliers = get_coa_customers_suppliers(collection)

			self.publish("Process Master Data", _("Processing Party Addresses"), 3, 5)
			parties, addresses = get_parties_addresses(collection, customers, suppliers)

			self.publish("Process Master Data", _("Processing Items and UOMs"), 4, 5)
			items, uoms = get_stock_items_uoms(collection)
			data = {"chart_of_accounts": chart_of_accounts, "parties": parties, "addresses": addresses, "items": items, "uoms": uoms}

			self.publish("Process Master Data", _("Done"), 5, 5)
			self.dump_processed_data(data)

			self.is_master_data_processed = 1

		except:
			self.publish("Process Master Data", _("Process Failed"), -1, 5)
			self.log()

		finally:
			self.set_status()

	def _import_master_data(self):
		def import_coa():
			self.publish(_("Importing Chart of Accounts"), 0, 100)

			coa = self.fetch_processed_data(self.chart_of_accounts)
			company = frappe.get_doc("Company", self.erpnext_company)

			frappe.local.flags.ignore_chart_of_accounts = True
			unset_existing_data(self.erpnext_company)
			create_charts(company.name, custom_chart=coa)
			company.on_update()
			company.validate()
			frappe.local.flags.ignore_chart_of_accounts = False

			self.update_field("is_chart_of_accounts_imported", 1)

		try:
			if not self.is_chart_of_accounts_imported:
				import_coa()

			self.publish("Import Master Data", _("Importing Items and UOMs"), 3, 4)
			create_items_uoms(self.items, self.uoms)

			self.publish("Import Master Data", _("Done"), 4, 4)

			self.set_account_defaults()
			self.is_master_data_imported = 1
			frappe.db.commit()

		except:
			self.publish("Import Master Data", _("Process Failed"), -1, 5)
			frappe.db.rollback()
			self.log()

		finally:
			self.set_status()
	
	def after_master_data_import(self):
		self.default_cost_center = frappe.db.get_value("Company", self.erpnext_company, ["cost_center"])
		self.default_warehouse = frappe.db.get_value("Stock Settings", "Stock Settings", "default_warehouse")
		self.update_field("is_master_data_imported", 1)

	def _process_day_book_data(self):
		def create_temporary_opening_account():
			temporary_opening_acc = encode_company_abbr("Temporary Opening", self.erpnext_company)
			if not frappe.db.exists("Account", temporary_opening_acc):
				frappe.get_doc({
					"doctype": "Account",
					"company": self.erpnext_company,
					"account_name": "Temporary Opening",
					"account_type": "Temporary",
					"is_group": 0,
					"report_type": "Balance Sheet",
					"root_type": "Asset",
					"parent_account": encode_company_abbr("Application of Funds (Assets)", self.erpnext_company)
				}).insert()

		def get_opening_entry(trial_balance_report):
			ledgers = trial_balance_report.find_all('DSPDISPNAME')
			credit_amounts = trial_balance_report.find_all('DSPCLCRAMT')
			debit_amounts = trial_balance_report.find_all('DSPCLDRAMT')

			total_debit = 0
			total_credit = 0

			jv_accounts = []
			for idx, ledger in enumerate(ledgers):
				if ledger.string.strip() == 'Opening Stock':
					#skip stock opening
					continue

				account_name = encode_company_abbr(ledger.string.strip(), self.erpnext_company)
				cr_amount = Decimal(credit_amounts[idx].get_text().strip() or 0)
				dr_amount = Decimal(debit_amounts[idx].get_text().strip() or 0)

				total_credit += cr_amount
				total_debit += dr_amount

				row = {
					"account": account_name,
					"cost_center": self.default_cost_center,
					"credit_in_account_currency": str(abs(cr_amount)),
					"debit_in_account_currency": str(abs(dr_amount))
				}
				party_details = get_party(ledger.string.strip())
				if party_details:
					party_type, party_account = party_details
					row["party_type"] = party_type
					row["account"] = party_account
					row["party"] = ledger.string.strip()

				jv_accounts.append(row)

			difference = flt(total_debit - total_credit, 2)
			if difference:
				create_temporary_opening_account()
				row = {
					"account": temporary_opening_acc,
					"cost_center": self.default_cost_center
				}
				amount = Decimal(difference)
				cr_or_dr = "debit_in_account_currency" if amount < 0 else "credit_in_account_currency"
				row[cr_or_dr] = str(abs(amount))
				jv_accounts.append(row)

			journal_entry = {
				"doctype": "Journal Entry",
				"title": "Tally Opening Balance",
				"voucher_type": "Opening Entry",
				"is_opening": "Yes",
				"posting_date": frappe.utils.now(), # TODO
				"company": self.erpnext_company,
				"accounts": jv_accounts,
			}
			return journal_entry
		
		def voucher_to_journal_entry(voucher):
			accounts = []
			ledger_entries = voucher.find_all("ALLLEDGERENTRIES.LIST") + voucher.find_all("LEDGERENTRIES.LIST")
			for entry in ledger_entries:
				account = {
					"account": encode_company_abbr(entry.LEDGERNAME.string.strip(), self.erpnext_company),
					"cost_center": self.default_cost_center
				}
				if entry.ISPARTYLEDGER.string.strip() == "Yes":
					party_details = get_party(entry.LEDGERNAME.string.strip())
					if party_details:
						party_type, party_account = party_details
						account["party_type"] = party_type
						account["account"] = party_account
						account["party"] = entry.LEDGERNAME.string.strip()
				
				amount = entry.AMOUNT.string.strip() if entry.AMOUNT else 0
				if '@' in amount:
					# eg. "-JPY363953.00 @ ₹ 0.6931/JPY = -₹ 252255.82"
					amount = amount.split("=")[-1].strip().replace("₹ ", "") # handle multicurrency

				amount = Decimal(amount)
				cr_or_dr = "debit_in_account_currency" if amount < 0 else "credit_in_account_currency"
				account[cr_or_dr] = str(abs(amount))
				accounts.append(account)
			
			if not accounts:
				return {}

			journal_entry = {
				"doctype": "Journal Entry",
				"tally_guid": voucher.GUID.string.strip(),
				"tally_voucher_no": voucher.VOUCHERNUMBER.string.strip() if voucher.VOUCHERNUMBER else "",
				"posting_date": voucher.DATE.string.strip(),
				"company": self.erpnext_company,
				"accounts": accounts,
			}
			return journal_entry

		def voucher_to_invoice(voucher):
			if voucher.VOUCHERTYPENAME.string.strip() in ["Sales", "Credit Note"]:
				doctype = "Sales Invoice"
				party_field = "customer"
				account_field = "debit_to"
				account_name = encode_company_abbr(self.tally_debtors_account, self.erpnext_company)
				price_list_field = "selling_price_list"
			elif voucher.VOUCHERTYPENAME.string.strip() in ["Purchase", "Debit Note"]:
				doctype = "Purchase Invoice"
				party_field = "supplier"
				account_field = "credit_to"
				account_name = encode_company_abbr(self.tally_creditors_account, self.erpnext_company)
				price_list_field = "buying_price_list"
			else:
				# Do not handle vouchers other than "Purchase", "Debit Note", "Sales" and "Credit Note"
				# Do not handle Custom Vouchers either
				return

			invoice = {
				"doctype": doctype,
				party_field: voucher.PARTYNAME.string.strip(),
				"tally_guid": voucher.GUID.string.strip(),
				"tally_voucher_no": voucher.VOUCHERNUMBER.string.strip() if voucher.VOUCHERNUMBER else "",
				"posting_date": voucher.DATE.string.strip(),
				"due_date": voucher.DATE.string.strip(),
				"items": get_voucher_items(voucher, doctype),
				"taxes": get_voucher_taxes(voucher),
				account_field: account_name,
				price_list_field: "Tally Price List",
				"set_posting_time": 1,
				"disable_rounded_total": 1,
				"company": self.erpnext_company,
			}
			return invoice

		def get_voucher_items(voucher, doctype):
			inventory_entries = voucher.find_all("INVENTORYENTRIES.LIST") + voucher.find_all("ALLINVENTORYENTRIES.LIST") + voucher.find_all("INVENTORYENTRIESIN.LIST") + voucher.find_all("INVENTORYENTRIESOUT.LIST")
			if doctype == "Sales Invoice":
				account_field = "income_account"
			elif doctype == "Purchase Invoice":
				account_field = "expense_account"
			items = []
			for entry in inventory_entries:
				item_code = entry.STOCKITEMNAME.string.strip()
				if entry.ACTUALQTY:
					qty, uom = entry.ACTUALQTY.string.strip().split()
				else:
					qty, uom = "1", frappe.db.get_value("Item", item_code, "stock_uom")
				rate = entry.RATE.string.strip().split("/")[0] if entry.RATE else 0
				items.append({
					"item_code": item_code,
					"item_name": item_code,
					"description": item_code,
					"qty": qty.strip(),
					"uom": uom.strip().title(),
					"conversion_factor": 1,
					"rate": rate,
					"price_list_rate": rate,
					"cost_center": self.default_cost_center,
					"warehouse": self.default_warehouse,
					account_field: encode_company_abbr(entry.find_all("ACCOUNTINGALLOCATIONS.LIST")[0].LEDGERNAME.string.strip(), self.erpnext_company),
				})
			return items

		def get_voucher_taxes(voucher):
			ledger_entries = voucher.find_all("ALLLEDGERENTRIES.LIST") + voucher.find_all("LEDGERENTRIES.LIST")
			taxes = []
			for entry in ledger_entries:
				if entry.ISPARTYLEDGER.string.strip() == "No":
					tax_account = encode_company_abbr(entry.LEDGERNAME.string.strip(), self.erpnext_company)
					tax_amount = Decimal(entry.AMOUNT.string.strip()) if entry.AMOUNT else 0
					taxes.append({
						"charge_type": "Actual",
						"category": "Total",
						"add_deduct_tax": "Add",
						"account_head": tax_account,
						"description": tax_account,
						"rate": 0,
						"tax_amount": str(abs(tax_amount)),
						"cost_center": self.default_cost_center,
					})
			return taxes

		def get_party(party):
			if frappe.db.exists({"doctype": "Supplier", "supplier_name": party}):
				return "Supplier", encode_company_abbr(self.tally_creditors_account, self.erpnext_company)
			elif frappe.db.exists({"doctype": "Customer", "customer_name": party}):
				return "Customer", encode_company_abbr(self.tally_debtors_account, self.erpnext_company)

		def get_vouchers(day_book_data):
			vouchers = []
			invalid_vouchers = []
			for voucher in day_book_data.find_all("VOUCHER"):
				if voucher.ISCANCELLED.string.strip() == "Yes":
					continue
				inventory_entries = voucher.find_all("INVENTORYENTRIES.LIST") + voucher.find_all("ALLINVENTORYENTRIES.LIST") + voucher.find_all("INVENTORYENTRIESIN.LIST") + voucher.find_all("INVENTORYENTRIESOUT.LIST")
				if voucher.VOUCHERTYPENAME.string.strip() not in ["Journal", "Receipt", "Payment", "Contra"] and inventory_entries:
					function = voucher_to_invoice
				else:
					function = voucher_to_journal_entry
				try:
					processed_voucher = function(voucher)
					if processed_voucher:
						vouchers.append(processed_voucher)
					else:
						invalid_vouchers.append(voucher)
				except:
					invalid_vouchers.append(voucher)

			return vouchers, invalid_vouchers

		def log_invalid_vouchers(vouchers):
			for d in vouchers:
				self.log(d)

		try:
			self.publish(_("Reading Trial Balance Report"), 1, 5)
			trial_balance_report = self.fetch_xml(self.trial_balance_report)

			self.publish(_("Processing Trial Balance Report"), 2, 5)
			opening_entry = get_opening_entry(trial_balance_report)

			self.publish(_("Reading Day Book Data"), 2, 5)
			day_book_data = self.get_collection(self.day_book_data)

			self.publish(_("Processing Vouchers"), 4, 5)
			vouchers, invalid_vouchers = get_vouchers(day_book_data)

			log_invalid_vouchers(invalid_vouchers)

			self.publish("Process Day Book Data", _("Done"), 3, 3)
			self.dump_processed_data({"vouchers": vouchers})

			self.is_day_book_data_processed = 1

		except:
			self.publish("Process Day Book Data", _("Process Failed"), -1, 5)
			self.log()

		finally:
			self.set_status()

	def _import_day_book_data(self):
		def create_custom_fields(doctypes):
			tally_guid_df = {
				"fieldtype": "Data",
				"fieldname": "tally_guid",
				"read_only": 1,
				"label": "Tally GUID"
			}
			tally_voucher_no_df = {
				"fieldtype": "Data",
				"fieldname": "tally_voucher_no",
				"read_only": 1,
				"label": "Tally Voucher Number"
			}
			for df in [tally_guid_df, tally_voucher_no_df]:
				for doctype in doctypes:
					create_custom_field(doctype, df)

		def create_price_list():
			frappe.get_doc({
				"doctype": "Price List",
				"price_list_name": "Tally Price List",
				"selling": 1,
				"buying": 1,
				"enabled": 1,
				"currency": "INR"
			}).insert()

		def create_expense_valuation_account():
			exp_included_in_val = encode_company_abbr("Expenses Included In Valuation", self.erpnext_company)
			if not frappe.db.exists("Account", exp_included_in_val):
				accounts = [
					["Direct Expenses", "Expenses", 1],
					["Stock Expenses", "Direct Expenses", 1],
					["Expenses Included In Valuation", "Stock Expenses", 0]
				]
				for acc in accounts:
					frappe.get_doc({
						"doctype": "Account",
						"company": self.erpnext_company,
						"account_name": acc[0],
						"is_group": acc[2],
						"report_type": "Profit and Loss",
						"root_type": "Expense",
						"parent_account": encode_company_abbr(acc[1], self.erpnext_company)
					}).insert(ignore_if_duplicate=True)

			return exp_included_in_val

		def create_round_off_account():
			round_off_acc = encode_company_abbr("Rounded Off", self.erpnext_company)
			if not frappe.db.exists("Account", round_off_acc):
				accounts = [
					["Indirect Expenses", "Expenses", 1],
					["Rounded Off", "Indirect Expenses", 0]
				]
				for acc in accounts:
					frappe.get_doc({
						"doctype": "Account",
						"company": self.erpnext_company,
						"account_name": acc[0],
						"is_group": acc[2],
						"report_type": "Profit and Loss",
						"root_type": "Expense",
						"parent_account": encode_company_abbr(acc[1], self.erpnext_company)
					}).insert(ignore_if_duplicate=True)

			return round_off_acc

		def before_day_book_data_import():
			self.update_field("error_log", "[]")
			creditors = encode_company_abbr(self.tally_creditors_account, self.erpnext_company)
			debtors = encode_company_abbr(self.tally_debtors_account, self.erpnext_company)

			frappe.db.set_value("Account", creditors, "account_type", "Payable")
			frappe.db.set_value("Account", debtors, "account_type", "Receivable")
			company = frappe.get_doc("Company", self.erpnext_company)
			exp_included_in_val = create_expense_valuation_account()
			round_off_account = create_round_off_account()

			company.round_off_account = self.default_round_off_account
			company.expenses_included_in_valuation = exp_included_in_val
			company.enable_perpetual_inventory = 0
			company.save()

			create_custom_fields(["Journal Entry", "Purchase Invoice", "Sales Invoice"])
			create_price_list()

		def pre_requisite_satisfied():
			return frappe.db.exists("Price List", {"price_list_name": "Tally Price List"})

		def import_opening_balances(entry):
			self.publish(_("Importing Opening Balances"), 0, 100)
			try:
				jv = frappe.get_doc(entry)
				jv.insert()
				jv.submit()
				self.update_field("is_opening_balances_imported", 1)
			except:
				frappe.db.rollback()
				self.log(entry)
				raise

		try:
			if not pre_requisite_satisfied(): # check if `before_day_book_data_import` has already executed
				before_day_book_data_import()

			vouchers = self.fetch_processed_data(self.vouchers)
			if not self.is_opening_balances_imported:
				import_opening_balances(vouchers[0])

			for index in range(0, total, VOUCHER_CHUNK_SIZE):
				if index + VOUCHER_CHUNK_SIZE >= total:
					is_last = True
				frappe.enqueue_doc(self.doctype, self.name, "_import_vouchers", queue="long", timeout=3600, start=index+1, total=total, is_last=is_last)

		except:
			self.log()

		finally:
			self.set_status()
	
	def enqueue_import(self, payload):
		total = len(payload)
		is_last = False

		for index in range(0, total, CHUNK_SIZE):
			if index + CHUNK_SIZE >= total:
				is_last = True

			frappe.enqueue_doc(
				self.doctype, self.name, "start_import",
				queue="long", timeout=3600, data=payload,
				start=index+1, is_last=is_last
			)

	def start_import(self, data, start, is_last):
		frappe.flags.in_migrate = True
		chunk = data[start: start + CHUNK_SIZE]

		import_type = "day_book" if self.is_master_data_imported else "masters"
		error_log = json.loads(self.error_log)
		progress_total = len(data)

		for index, voucher in enumerate(chunk, start=start):
			try:
				doctype = doc['doctype']

				self.publish(_("Importing {}").format(doctype), i + 1, progress_total)
				flags = doc.pop("flags") if doc.get("flags") else {}
				d = frappe.get_doc(doc)
				d.flags.update(flags)
				d.insert()
				if d.meta.is_submittable:
					d.submit()
				frappe.db.commit()
			except:
				frappe.db.rollback()
				error = str(e)

				if len(e.args) == 3 and frappe.db.is_unique_key_violation(e.args[2]):
					# if duplicate then ignore
					continue

				error_log.append({ "type": import_type, "doc": doc, "error": error })

		self.update_field("error_log", json.dumps(error_log))
		if is_last:
			self.finish_import(error_log)

		frappe.flags.in_migrate = False

	def finish_import(self, error_log):
		if not self.is_master_data_imported:
			errored_docs = [d.get("doc") for d in error_log if d.get("type") == "masters"]
			remaining_masters = self.dump_processed_data(errored_docs, "masters")
			self.update_field("masters", remaining_masters)

			if not errored_docs:
				self.publish(_("Master Data Import Complete"), 1, 1)
				self.after_master_data_import()
			else:
				self.publish(_("Resolve Errors and Try Again"), -1, 1)

		elif not self.is_day_book_data_imported:
			errored_docs = [d.get("doc") for d in error_log if d.get("type") == "day_book"]
			remaining_vouchers = self.dump_processed_data(errored_docs, "vouchers")
			self.update_field("vouchers", remaining_vouchers)

			if not errored_docs:
				self.publish(_("Day Book Data Import Complete"), 1, 1)
				self.after_day_book_data_import()
			else:
				self.publish(_("Resolve Errors and Try Again"), -1, 1)
	
	def publish(self, message, progress, total):
		frappe.publish_realtime("tally_migration_progress_update", {
			"title": "Tally Migration",
			"progress": progress,
			"total": total,
			"user": frappe.session.user,
			"message": message
		})

	def set_status(self, status=""):
		self.update_field("status", status)

	def update_field(self, field, value):
		self.db_set(field, value, update_modified=False, commit=True)
	
	def after_day_book_data_import(self):
		self.status = ""
		self.is_day_book_data_imported = 1
		self.save()
		frappe.db.set_value("Price List", "Tally Price List", "enabled", 0)

	def process_master_data(self):
		self.set_status("Processing Master Data")
		frappe.enqueue_doc(self.doctype, self.name, "_process_master_data", queue="long", timeout=3600)

	@frappe.whitelist()
	def import_master_data(self):
		self.set_status("Importing Master Data")
		frappe.enqueue_doc(self.doctype, self.name, "_import_master_data", queue="long", timeout=3600)

	@frappe.whitelist()
	def process_day_book_data(self):
		self.set_status("Processing Day Book Data")
		frappe.enqueue_doc(self.doctype, self.name, "_process_day_book_data", queue="long", timeout=3600)

	@frappe.whitelist()
	def import_day_book_data(self):
		self.set_status("Importing Day Book Data")
		frappe.enqueue_doc(self.doctype, self.name, "_import_day_book_data", queue="long", timeout=3600)

	def export_to_csv(self, to_export):
		if to_export in ["UOM", "Item Group", "Item", "Customer", "Supplier", "Address"]:
			docs = self.fetch_processed_data(self.masters)
		else:
			docs = self.fetch_processed_data(self.vouchers)

		doctype = to_export

		if to_export == 'Opening Entry':
			docs = docs[0]
			doctype = 'Journal Entry'
		else:
			docs = [d for d in docs if d.get('doctype') == to_export]

		export_fields = get_export_fields(docs, doctype)

		e = TallyExporter(
			doctype,
			docs=docs,
			export_fields=export_fields,
			export_data=True
		)
		e.build_response()

	def log(self, data=None):
		data = data or self.status
		tb = traceback.format_exc()
		error_msg = frappe.bold(str(sys.exc_info()[1]))

		message = "\n".join([
			f"Error: {error_msg}",
			"--" * 50,
			"Data:", json.dumps(data, default=str, indent=4),
			"--" * 50,
			"\nException:", tb
		])
		frappe.log_error(title="Tally Migration Error", message=message)
		frappe.db.commit()

class TallyExporter(Exporter):
	def __init__(self, *args, **kwargs):
		docs = kwargs.pop('docs')
		self.docs = docs

		super(TallyExporter, self).__init__(*args, **kwargs)

	def get_data_as_docs(self):
		return self.docs

def get_export_fields(docs, doctype):
	export_fields = {}
	for doc in docs:
		for key, value in doc.items():
			if key == 'doctype':
				export_fields[doctype] = ["name"]

			elif isinstance(value, list):
				export_fields[key] = ["name"]
				for child_doc in value:
					child_doc["parentfield"] = key
					for child_key in child_doc:
						if child_key not in export_fields[key]:
							export_fields[key].append(child_key)

			else:
				if key not in export_fields[doctype]:
					export_fields[doctype].append(key)

	return export_fields

@frappe.whitelist()
def export_to_csv(docname, to_export):
	tm = frappe.get_doc("Tally Migration", docname)
	tm.export_to_csv(to_export)

