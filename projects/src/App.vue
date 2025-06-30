<template>
	<div class="flex h-screen w-screen overflow-hidden bg-surface-white text-base antialiased">
		<ProjectsSidebar />
		<div class="flex flex-col flex-1 overflow-hidden">
			<div class="flex h-12 items-center justify-between border-b px-4">
				<Breadcrumbs :items="breadcrumbs" />
			</div>
			<div class="flex-1 overflow-hidden">
				<router-view />
			</div>
		</div>
	</div>
</template>

<script setup lang="ts">
import { Breadcrumbs } from 'frappe-ui'
import ProjectsSidebar from './components/ProjectsSidebar.vue'
import { breadcrumbs } from './breadcrumbs'
</script>
