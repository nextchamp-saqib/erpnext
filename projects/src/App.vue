<template>
	<div class="flex h-screen w-screen overflow-hidden bg-white text-base antialiased">
		<ProjectsSidebar />
		<div class="flex flex-col flex-1">
			<div class="flex h-12 items-center justify-between border-b py-2.5 px-5">
				<Breadcrumbs :items="breadcrumbs" />
			</div>
			<div class="flex-1 overflow-y-auto">
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
