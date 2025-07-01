<template>
	<DocTypeList doctype="Timesheet" />
</template>

<script setup lang="ts">
import { setBreadcrumbs } from '@/breadcrumbs'
import DocTypeList from '@/components/DocTypeList/DocTypeList.vue'

setBreadcrumbs([{ label: 'Timesheets', route: '/timesheets' }])
</script>
