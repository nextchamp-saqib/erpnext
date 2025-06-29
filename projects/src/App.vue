<template>
	<div class="flex h-screen w-screen overflow-hidden bg-white text-base antialiased">
		<ProjectsSidebar />
		<div class="flex flex-1">
			<router-view />
		</div>
	</div>
</template>

<script setup lang="ts">
import ProjectsSidebar from './components/ProjectsSidebar.vue'
</script>
