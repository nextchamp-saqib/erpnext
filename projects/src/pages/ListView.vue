<template>
	<ListView :doctype="doctype || 'Project'" />
</template>

<script setup lang="ts">
import { ListView } from 'frappe-ui/frappe'
import { unscrub } from 'frappe-ui/frappe/utils'
import { computed } from 'vue'

const props = defineProps<{ doctype?: string }>()

const doctype = computed(() => unscrub(props.doctype || 'Project'))
</script>
