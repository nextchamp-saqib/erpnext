<template>
	<div class="flex items-center text-base">
		<!-- Status Badge -->
		<Badge v-if="isStatusField" :theme="'gray'" :size="'lg'" :variant="'subtle'">
			{{ props.value }}
		</Badge>

		<!-- Tags Display -->
		<div v-else-if="isTagsField" class="flex flex-wrap gap-1">
			<span
				v-for="tag in parsedTags"
				:key="tag"
				class="inline-flex items-center px-2 py-0.5 rounded-md font-medium bg-blue-100 text-blue-800"
			>
				{{ tag }}
			</span>
		</div>

		<!-- Assignment Display -->
		<div v-else-if="isAssignField" class="flex -space-x-2">
			<Avatar
				v-for="email in parsedAssignments"
				:key="email"
				:label="email"
				size="lg"
				class="border-2 border-[var(--surface-white)]"
			/>
		</div>

		<!-- Link Field -->
		<div v-else-if="isLinkField" class="truncate">
			{{ props.value }}
		</div>

		<!-- Date/Datetime Fields -->
		<span v-else-if="isDateField">
			{{ formatDate(props.value) }}
		</span>

		<!-- Numeric Fields -->
		<span v-else-if="isNumericField" class="text-right">
			{{ formatNumeric(props.value) }}
		</span>

		<!-- Currency Field -->
		<span v-else-if="isCurrencyField" class="text-right">
			{{ formatCurrency(props.value) }}
		</span>

		<!-- Percent Field -->
		<div v-else-if="isPercentField" class="w-full bg-surface-gray-2 rounded-full h-2.5 mr-4">
			<div
				class="bg-surface-green-3 h-2.5 rounded-full"
				:style="{ width: `${Number(props.value)}%` }"
			></div>
		</div>

		<!-- Check Field -->
		<Checkbox
			v-else-if="isCheckField"
			disabled
			:model-value="Number(props.value) ? true : false"
		/>

		<!-- Select Field -->
		<span v-else-if="isSelectField" class="truncate">
			{{ props.value }}
		</span>

		<!-- Text Fields -->
		<span v-else-if="isTextField" class="truncate" :title="props.value">
			{{ truncateText(props.value) }}
		</span>

		<!-- Image Field -->
		<div v-else-if="isImageField" class="flex items-center">
			<img
				v-if="props.value"
				:src="props.value"
				alt="Image"
				class="h-8 w-8 rounded object-cover"
			/>
			<span v-else class="italic">No Image</span>
		</div>

		<!-- Rating Field -->
		<Rating v-else-if="isRatingField" :model-value="Number(props.value) * 5" />

		<!-- Default Display -->
		<span v-else class="truncate"> {{ props.value }} </span>
	</div>
</template>

<script setup lang="ts">
import { useTimeAgo } from '@vueuse/core'
import { Avatar, Badge, Checkbox, Rating } from 'frappe-ui'
import { computed } from 'vue'

const props = defineProps<{
	field: {
		fieldname: string
		fieldtype: string
		label: string
		options?: string
	}
	value: any
}>()

const isStatusField = computed(() => {
	return (
		props.field.fieldname === 'status' ||
		props.field.fieldname === 'workflow_state' ||
		props.field.fieldname === 'docstatus'
	)
})

const isTagsField = computed(() => {
	return props.field.fieldname === '_user_tags'
})

const isAssignField = computed(() => {
	return props.field.fieldname === '_assign'
})

const isLinkField = computed(() => {
	return props.field.fieldtype === 'Link'
})

const isDateField = computed(() => {
	return props.field.fieldtype === 'Date' || props.field.fieldtype === 'Datetime'
})

const isNumericField = computed(() => {
	return (
		metadata.numeric_fieldtypes.includes(props.field.fieldtype) &&
		props.field.fieldtype !== 'Currency' &&
		props.field.fieldtype !== 'Percent'
	)
})

const isCurrencyField = computed(() => {
	return props.field.fieldtype === 'Currency'
})

const isPercentField = computed(() => {
	return props.field.fieldtype === 'Percent'
})

const isCheckField = computed(() => {
	return props.field.fieldtype === 'Check'
})

const isSelectField = computed(() => {
	return props.field.fieldtype === 'Select'
})

const isTextField = computed(() => {
	return metadata.html_fieldtypes.includes(props.field.fieldtype)
})

const isImageField = computed(() => {
	return props.field.fieldtype === 'Attach Image' || props.field.fieldtype === 'Image'
})

const isRatingField = computed(() => {
	return props.field.fieldtype === 'Rating'
})

const parsedTags = computed(() => {
	try {
		return props.value ? JSON.parse(props.value) : []
	} catch {
		return []
	}
})

const parsedAssignments = computed(() => {
	try {
		return props.value ? JSON.parse(props.value) : []
	} catch {
		return []
	}
})

const formatDate = (value: any) => {
	if (!value) return ''
	if (props.field.fieldname === 'creation' || props.field.fieldname === 'modified') {
		return useTimeAgo(value).value
	}

	const date = new Date(value)
	return date.toLocaleDateString()
}

const formatNumeric = (value: any) => {
	if (value === null || value === undefined) return ''
	return Number(value).toLocaleString()
}

const formatCurrency = (value: any) => {
	if (value === null || value === undefined) return ''
	return new Intl.NumberFormat('en-US', {
		style: 'currency',
		currency: 'USD',
	}).format(Number(value))
}

const truncateText = (text: any) => {
	const str = String(text || '')
	return str.length > 50 ? str.substring(0, 50) + '...' : str
}

const metadata = {
	all_fieldtypes: [
		'Autocomplete',
		'Attach',
		'Attach Image',
		'Barcode',
		'Button',
		'Check',
		'Code',
		'Color',
		'Currency',
		'Data',
		'Date',
		'Datetime',
		'Duration',
		'Dynamic Link',
		'Float',
		'Geolocation',
		'Heading',
		'HTML',
		'HTML Editor',
		'Icon',
		'Image',
		'Int',
		'JSON',
		'Link',
		'Long Text',
		'Markdown Editor',
		'Password',
		'Percent',
		'Phone',
		'Read Only',
		'Rating',
		'Select',
		'Signature',
		'Small Text',
		'Table',
		'Table MultiSelect',
		'Text',
		'Text Editor',
		'Time',
	],

	no_value_type: [
		'Section Break',
		'Column Break',
		'Tab Break',
		'HTML',
		'Table',
		'Table MultiSelect',
		'Button',
		'Image',
		'Fold',
		'Heading',
	],

	layout_fields: ['Section Break', 'Column Break', 'Tab Break', 'Fold'],

	std_fields_list: [
		'name',
		'owner',
		'creation',
		'modified',
		'modified_by',
		'_user_tags',
		'_comments',
		'_assign',
		'_liked_by',
		'docstatus',
		'idx',
	],

	child_table_field_list: ['parent', 'parenttype', 'parentfield'],

	restricted_fields: [
		'name',
		'parent',
		'creation',
		'modified',
		'modified_by',
		'parentfield',
		'parenttype',
		'file_list',
		'flags',
		'docstatus',
	],

	html_fieldtypes: [
		'Text Editor',
		'Text',
		'Small Text',
		'Long Text',
		'HTML Editor',
		'Markdown Editor',
		'Code',
	],

	std_fields: [
		{ fieldname: 'name', fieldtype: 'Link', label: 'ID' },
		{ fieldname: 'owner', fieldtype: 'Link', label: 'Created By', options: 'User' },
		{ fieldname: 'idx', fieldtype: 'Int', label: 'Index' },
		{ fieldname: 'creation', fieldtype: 'Datetime', label: 'Created On' },
		{ fieldname: 'modified', fieldtype: 'Datetime', label: 'Last Updated On' },
		{ fieldname: 'modified_by', fieldtype: 'Link', label: 'Last Updated By', options: 'User' },
		{ fieldname: '_user_tags', fieldtype: 'Data', label: 'Tags' },
		{ fieldname: '_liked_by', fieldtype: 'Data', label: 'Liked By' },
		{ fieldname: '_comments', fieldtype: 'Text', label: 'Comments' },
		{ fieldname: '_assign', fieldtype: 'Text', label: 'Assigned To' },
		{ fieldname: 'docstatus', fieldtype: 'Int', label: 'Document Status' },
	],

	numeric_fieldtypes: ['Int', 'Float', 'Currency', 'Percent', 'Duration'],

	std_fields_table: [{ fieldname: 'parent', fieldtype: 'Data', label: 'Parent' }],

	table_fields: ['Table', 'Table MultiSelect'],
}
</script>
