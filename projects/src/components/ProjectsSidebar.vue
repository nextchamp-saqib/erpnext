<template>
	<Sidebar v-bind="sidebarConfig" />
</template>

<script setup lang="ts">
import { Sidebar } from 'frappe-ui'
import { computed, reactive } from 'vue'
import { useRoute } from 'vue-router'
import { session } from 'frappe-ui/frappe'

import LucideBell from '~icons/lucide/bell'
import LucideFolder from '~icons/lucide/folder-open-dot'
import LucideHelpCircle from '~icons/lucide/help-circle'
import LucideHourglass from '~icons/lucide/hourglass'
import LucideListTodo from '~icons/lucide/list-todo'
import LucideLogout from '~icons/lucide/log-out'
import LucideMoon from '~icons/lucide/moon'
import LucideSearch from '~icons/lucide/search'

const route = useRoute()
const appName = 'Plainbase'

const sidebarConfig = reactive({
	header: {
		title: appName,
		subtitle: session.user.full_name,
		menuItems: [
			{
				label: 'Toggle Theme',
				icon: LucideMoon,
				onClick: () => {
					const currentTheme = document.documentElement.getAttribute('data-theme')
					let theme = currentTheme === 'dark' ? 'light' : 'dark'
					document.documentElement.setAttribute('data-theme', theme)
					localStorage.setItem('theme', theme)
				},
			},
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
					to: '/',
					isActive: computed(() => route.path === '/'),
				},
				{
					label: 'Tasks',
					icon: LucideListTodo,
					to: '/task',
					isActive: computed(() => route.path.startsWith('/task')),
				},
				{
					label: 'Timesheets',
					icon: LucideHourglass,
					to: '/timesheet',
					isActive: computed(() => route.path.startsWith('/timesheet')),
				},
			],
		},
	],
})
</script>
