<template>
	<FormControl
		v-if="props.fieldtype == 'Check'"
		type="checkbox"
		v-model="modelValue"
		:label="props.label"
	/>
	<FormControl
		v-else-if="props.fieldtype === 'Select'"
		class="form-control cursor-pointer [&_select]:cursor-pointer"
		type="select"
		v-model="modelValue"
		:options="parseSelectOptions(props.options!)"
		:placeholder="props.label"
	/>
	<Link
		v-else-if="props.fieldtype === 'Link'"
		v-model="modelValue"
		:doctype="props.options"
		:placeholder="props.label"
	/>
	<component
		v-else-if="['Date', 'Datetime'].includes(props.fieldtype)"
		:is="props.fieldtype === 'Date' ? DatePicker : DateTimePicker"
		class="border-none"
		v-model="modelValue"
		:placeholder="props.label"
	/>
	<FormControl v-else type="text" v-model="modelValue" :placeholder="props.label" />
</template>

<script setup lang="ts">
import { DatePicker, DateTimePicker, FormControl } from 'frappe-ui'

const modelValue = defineModel()
const props = defineProps<{
	fieldname: string
	fieldtype: string
	label: string
	options?: string | string[]
}>()

const parseSelectOptions = (options: string | string[]) => {
	if (Array.isArray(options) && options[0] && typeof options[0] === 'string') {
		return options.map((option) => ({ label: option, value: option }))
	}
	if (typeof options === 'string') {
		return options.split('\n').map((option) => ({ label: option, value: option }))
	}
	console.warn(`Invalid options: '${options}' for select field '${props.fieldname}'`)
	return []
}
</script>
