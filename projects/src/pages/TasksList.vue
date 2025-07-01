<template>
	<DocTypeList doctype="Task" />
</template>

<script setup lang="ts">
import { setBreadcrumbs } from '@/breadcrumbs'
import DocTypeList from '@/components/DocTypeList/DocTypeList.vue'

setBreadcrumbs([{ label: 'Tasks', route: '/tasks' }])
</script>
