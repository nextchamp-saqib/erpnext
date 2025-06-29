<template>
	<div></div>
</template>

<script setup lang="ts">
import { setBreadcrumbs } from '@/breadcrumbs'

setBreadcrumbs([
	{ label: 'Projects', route: '/' },
	{ label: 'List', route: '/' },
])
</script>
