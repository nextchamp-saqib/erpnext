<template>
	<div class="flex h-screen w-screen overflow-hidden bg-surface-white text-base antialiased">
		<ProjectsSidebar />
		<div class="flex flex-col flex-1 overflow-hidden">
			<router-view />
		</div>
	</div>
</template>

<script setup lang="ts">
import ProjectsSidebar from './components/ProjectsSidebar.vue'
</script>
