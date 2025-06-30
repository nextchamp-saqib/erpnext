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
import { FormControl } from 'frappe-ui'
import { computed } from 'vue'
import { Meta } from './types'
import { dataFields, hasPerm, isValueType } from './utils'

const props = defineProps<{ meta: Meta }>()

const quickFilters = computed(() => {
	const fields = props.meta.fields

	const quickFilterFields = fields.filter((df) => {
		const isTitleField = df.fieldname === props.meta.title_field
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
</script>
