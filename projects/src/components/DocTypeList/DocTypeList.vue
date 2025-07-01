<template>
	<div v-if="!list.loading" class="flex flex-col h-full w-full overflow-hidden p-4">
		<!-- List Header -->
		<div class="flex justify-between gap-4">
			<QuickFilters v-if="list.meta" :meta="list.meta" />

			<!-- Filter -->
			<div class="flex gap-2">
				<Button label="Filter">
					<template #prefix>
						<LucideListFilter class="size-4 text-ink-gray-6" />
					</template>
				</Button>
				<Button label="Sort">
					<template #prefix>
						<LucideArrowUpDown class="size-4 text-ink-gray-6" />
					</template>
				</Button>
				<Button>
					<template #icon>
						<LucideMoreVertical class="size-4 text-ink-gray-6" />
					</template>
				</Button>
			</div>
		</div>

		<ListView
			class="mt-4"
			:id="`${list.doctype}-list`"
			:columns="list.columns"
			:rows="list.data"
			rowKey="name"
			:options="list.options"
		>
			<template #cell="{ column, row, item: value }">
				<DocTypeListRowCell :field="column" :value="value" />
			</template>
		</ListView>
	</div>
</template>

<script setup lang="ts">
import { useTimeAgo } from '@vueuse/core'
import { ListView, useCall, useList } from 'frappe-ui'
import { reactive } from 'vue'
import DocTypeListRowCell from './DocTypeListRowCell.vue'
import QuickFilters from './QuickFilters.vue'
import { Meta } from './types'
import { hasPerm, isValueType } from './utils'

const props = defineProps<{ doctype: string }>()

const list = reactive({
	loading: true,
	doctype: props.doctype,
	meta: null as Meta | null,
	permittedFields: [] as Meta['fields'],
	listFields: [] as Meta['fields'],
	columns: [] as (Meta['fields'][number] & { key: string })[],
	data: [] as any[],
	loadData() {
		useList({
			doctype: props.doctype,
			fields: list.listFields.map((df) => df.fieldname as any),
			onSuccess: (data) => {
				list.data = data
				list.loading = false
			},
		})
	},
	options: {
		showTooltip: false,
		resizeColumn: true,
		emptyState: {
			title: 'You have no projects',
			description: 'Create a new project to get started.',
			button: {
				label: 'Create',
				variant: 'solid',
				onClick: () => {},
			},
		},
	},
})

useCall<Meta>({
	method: 'GET',
	url: `/api/v2/doctype/${props.doctype}/meta`,
	onSuccess: (data) => {
		list.meta = data
		list.permittedFields = data.fields.filter((df) => {
			return isValueType(df.fieldtype) && hasPerm(df.permlevel)
		})
		list.listFields = filterListFields(list.permittedFields)
		list.columns = list.listFields.map((df) => {
			return {
				...df,
				key: df.fieldname,
			}
		})
		list.loadData()
	},
})

function filterListFields(fields: Meta['fields']) {
	if (!list.meta) {
		return fields
	}

	const titleFieldName = list.meta.title_field
	const displayFields = fields.filter((df) => {
		if (df.fieldname === titleFieldName) {
			return false
		}
		return (
			df.in_list_view ||
			(df.fieldtype === 'Currency' && df.options && !df.options.includes(':')) ||
			df.fieldname === 'status'
		)
	})

	if (titleFieldName) {
		const titleField = fields.find((df) => df.fieldname === titleFieldName)
		if (titleField) {
			displayFields.unshift(titleField)
		}
	}

	const hideNameColumn = false
	if (!hideNameColumn && list.meta.title_field !== 'name') {
		displayFields.push({
			fieldname: 'name',
			label: 'ID',
			fieldtype: 'Data',
		})
	}

	displayFields.push({
		fieldname: 'modified',
		label: 'Modified',
		fieldtype: 'Datetime',
	})

	displayFields.push({
		fieldname: '_assign',
		label: 'Assigned To',
		fieldtype: 'Data',
	})

	return displayFields
}

function getTimeAgo(date: string) {
	return useTimeAgo(date).value
}
</script>
