<template>
	<div class="flex gap-2">
		<FormControl
			class="w-44"
			v-for="filter in quickFilters"
			:key="filter.fieldname"
			:type="filter.type"
			:placeholder="filter.label"
			:options="filter.options"
			:debounce="300"
		/>
	</div>
</template>

<script setup lang="ts">
import { FormControl, useCall } from 'frappe-ui'
import { computed } from 'vue'

const props = defineProps<{ doctype: string }>()

type Meta = {
	title_field: string
	fields: {
		options: any
		label: string
		fieldname: string
		fieldtype: string
		in_standard_filter: boolean
		permlevel: number
	}[]
}

const meta = useCall<Meta>({
	url: `/api/v2/doctype/${props.doctype}/meta`,
	method: 'GET',
})

const quickFilters = computed(() => {
	if (!meta.data) return []

	const _meta = meta.data as Meta
	const fields = _meta.fields

	const quickFilterFields = fields.filter((df) => {
		const isTitleField = df.fieldname === _meta.title_field
		const isValidStandardFilter = df.in_standard_filter && isValueType(df.fieldtype)
		const hasPermLevelAccess = hasPerm(df.permlevel)

		return (isTitleField || isValidStandardFilter) && hasPermLevelAccess
	})

	const quickFilters = []
	for (const df of quickFilterFields) {
		let fieldtype = df.fieldtype
		let operator = '='
		let options = []

		if (dataFields.includes(df.fieldtype)) {
			fieldtype = 'Data'
			operator = 'like'
		}
		if (df.fieldtype === 'Select') {
			options = df.options.split('\n')
		}

		quickFilters.push({
			label: df.label,
			fieldname: df.fieldname,
			fieldtype: fieldtype,
			type: fieldtype === 'Select' ? 'select' : 'text',
			operator: operator,
			options: options,
		})
	}

	return quickFilters
})

const dataFields = [
	'Text',
	'Small Text',
	'Text Editor',
	'HTML Editor',
	'Data',
	'Code',
	'Phone',
	'JSON',
	'Read Only',
]

function isValueType(fieldtype: string) {
	return (
		['Data', 'Text', 'Small Text', 'Long Text', 'Code', 'Password'].includes(fieldtype) ||
		fieldtype.startsWith('Select') ||
		fieldtype === 'Link' ||
		fieldtype === 'Dynamic Link' ||
		fieldtype === 'Read Only'
	)
}

function hasPerm(permlevel: number) {
	return true
}
</script>
