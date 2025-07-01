<template>
	<div class="flex gap-2">
		<DocField
			class="w-44"
			v-for="filter in quickFilters"
			:key="filter.fieldname"
			:fieldname="filter.fieldname"
			:fieldtype="filter.fieldtype"
			:label="filter.label"
			:options="filter.options"
		/>
	</div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import DocField from './DocField.vue'
import { Meta } from './types'
import { hasPerm, isValueType } from './utils'

const props = defineProps<{ meta: Meta }>()

const quickFilters = computed(() => {
	const fields = props.meta.fields

	return fields.filter((df) => {
		const isTitleField = df.fieldname === props.meta.title_field
		const isValidStandardFilter = df.in_standard_filter && isValueType(df.fieldtype)
		const hasPermLevelAccess = hasPerm(df.permlevel)

		return (isTitleField || isValidStandardFilter) && hasPermLevelAccess
	})
})
</script>
