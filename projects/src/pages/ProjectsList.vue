<template>
	<div class="flex flex-col h-full w-full overflow-hidden p-4">
		<!-- List Header -->
		<div class="flex justify-between gap-4">
			<QuickFilters doctype="Project" />

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
			class="h-full mt-4"
			:columns="[
				{ label: 'Name', key: 'name', width: 1 },
				{ label: 'Status', key: 'status', width: 1 },
				{ label: 'Priority', key: 'priority', width: 1 },
				{ label: 'Start Date', key: 'start_date', width: 1 },
				{ label: 'End Date', key: 'end_date', width: 1 },
				{ label: 'Assigned To', key: 'assigned_to', width: 1 },
				{ label: 'Created By', key: 'created_by', width: 1 },
			]"
			:rows="[]"
			rowKey="name"
			:options="{
				showTooltip: false,
				emptyState: {
					title: 'You have no projects',
					description: 'Create a new project to get started.',
					button: {
						label: 'Create',
						variant: 'solid',
						onClick: () => {},
					},
				},
			}"
		/>
	</div>
</template>

<script setup lang="ts">
import { setBreadcrumbs } from '@/breadcrumbs'
import QuickFilters from '@/components/QuickFilters.vue'
import { ListView } from 'frappe-ui'

setBreadcrumbs([{ label: 'Projects', route: '/' }])
</script>
