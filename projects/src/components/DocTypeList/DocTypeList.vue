<template>
	<div class="flex flex-col h-full w-full overflow-hidden">
		<div class="flex h-12 items-center flex-shrink-0 justify-between border-b px-4">
			<Breadcrumbs :items="breadcrumbs" />
		</div>
		<div
			v-if="!list.meta.loading"
			class="flex flex-col flex-1 h-full w-full overflow-hidden p-4"
		>
			<!-- List Header -->
			<div class="flex justify-between gap-4">
				<div class="flex gap-2">
					<QuickFilter
						v-for="field in list.quickFilterFields"
						:key="field.fieldname"
						:field="field"
						:modelValue="list.filters[field.fieldname]"
						@update:modelValue="
							$event
								? (list.filters[field.fieldname] = $event)
								: delete list.filters[field.fieldname]
						"
					/>
				</div>

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
				:columns="list.props.columns"
				:rows="list.props.rows"
				:rowKey="list.props.rowKey"
				:options="list.props.options"
			>
				<template #cell="{ column, row, item: value }">
					<DocTypeListCell :field="column" :value="value" />
				</template>
			</ListView>
		</div>
	</div>
</template>

<script setup lang="ts">
import { Breadcrumbs, ListView } from 'frappe-ui'
import { useRoute } from 'vue-router'
import DocTypeListCell from './DocTypeListCell.vue'
import QuickFilter from './QuickFilter.vue'
import { useDocTypeList } from './useDocTypeList'
import { pluralize } from './utils'

const props = defineProps<{ doctype: string }>()

const route = useRoute()
const breadcrumbs = [
	{
		label: pluralize(props.doctype),
		to: route.path,
	},
]

const list = useDocTypeList(props.doctype)
</script>
