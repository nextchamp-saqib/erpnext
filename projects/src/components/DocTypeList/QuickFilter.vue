<template>
	<DocField
		class="w-44"
		:key="props.field.fieldname"
		:fieldname="props.field.fieldname"
		:fieldtype="props.field.fieldtype"
		:label="props.field.label"
		:options="props.field.options"
		v-model="value"
	/>
</template>

<script setup lang="ts">
import { FilterValue } from 'frappe-ui/src/data-fetching/useList/types'
import { computed, ref, watchEffect } from 'vue'
import { DEFAULT_OPERATOR } from './constants'
import DocField from './DocField.vue'
import { DocField as TDocField } from './types'

const props = defineProps<{ field: TDocField }>()
const filterValue = defineModel<FilterValue>()

const value = ref('')
const operator = computed(() => {
	return props.field.fieldtype in DEFAULT_OPERATOR ? DEFAULT_OPERATOR[props.field.fieldtype] : '='
})

watchEffect(() => {
	if (value.value) {
		filterValue.value = [operator.value, value.value]
	} else {
		filterValue.value = undefined
	}
})
</script>
