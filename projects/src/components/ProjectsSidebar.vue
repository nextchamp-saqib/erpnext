<template>
	<Sidebar v-bind="sidebarConfig" />
</template>

<script setup lang="ts">
import { Sidebar } from 'frappe-ui'
import session from '../session'
import { reactive } from 'vue'

import LucideLogout from '~icons/lucide/log-out'
import LucideHelpCircle from '~icons/lucide/help-circle'
import LucideBell from '~icons/lucide/bell'
import LucideFolder from '~icons/lucide/folder-open-dot'
import LucideSearch from '~icons/lucide/search'
import LucideHourglass from '~icons/lucide/hourglass'
import LucideListTodo from '~icons/lucide/list-todo'

const sidebarConfig = reactive({
	header: {
		title: 'Projects',
		subtitle: session.user.full_name,
		menuItems: [
			{
				label: 'Help',
				icon: LucideHelpCircle,
				onClick: () => {},
			},
			{
				label: 'Logout',
				icon: LucideLogout,
				onClick: () => session.logout(),
			},
		],
	},
	sections: [
		{
			label: '',
			items: [
				{
					label: 'Search',
					icon: LucideSearch,
					onClick: () => {},
				},
				{
					label: 'Notifications',
					icon: LucideBell,
					onClick: () => {},
				},
			],
		},
		{
			label: 'Core',
			items: [
				{
					label: 'Projects',
					icon: LucideFolder,
					to: '/projects',
				},
				{
					label: 'Tasks',
					icon: LucideListTodo,
					to: '/tasks',
				},
				{
					label: 'Timesheets',
					icon: LucideHourglass,
					to: '/timesheets',
				},
			],
		},
	],
})
</script>
