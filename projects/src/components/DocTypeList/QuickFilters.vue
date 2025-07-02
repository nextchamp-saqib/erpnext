<template>
	<div class="flex gap-2">
		<DocField
			class="w-44"
			v-for="field in props.fields"
			:key="field.fieldname"
			:fieldname="field.fieldname"
			:fieldtype="field.fieldtype"
			:label="field.label"
			:options="field.options"
		/>
	</div>
</template>

<script setup lang="ts">
import DocField from './DocField.vue'
import { DocField as TDocField } from './types'

const props = defineProps<{ fields: TDocField[] }>()
</script>
